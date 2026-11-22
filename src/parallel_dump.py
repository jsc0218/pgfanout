#!/usr/bin/env python3
"""Parallel COPY export using a shared PostgreSQL snapshot.

Opens a REPEATABLE READ transaction via ``psycopg`` to export a snapshot,
then launches parallel workers that each COPY a partition of the table
under the same snapshot — guaranteeing a consistent point-in-time view
of the data regardless of how long the dump takes.

Splits work by physical page location using TID range scans
(PostgreSQL 14+).  Each worker reads only its assigned pages — no
column knowledge, no index, and no full-table scan per worker.  No
superuser or ``pg_write_server_files`` privileges needed, just SELECT.

Data plane — stays out of Python:
    Each worker shells out to ``psql`` running a single transaction
    that imports the shared snapshot and emits ``COPY (SELECT ...)
    TO STDOUT WITH (FORMAT binary)``.  Its stdout is piped straight
    into a :class:`~storage.Blob` sink via
    :func:`~common.stream_subprocess_to_sink`, so the data bytes
    travel ``libpq → psql → pipe → sink`` inside a single native
    subprocess.  Python only reads the pipe in a tight ``read()/
    write()`` loop that releases the GIL on every syscall, which is
    what lets N parallel workers scale linearly and matches the
    throughput of hand-rolled ``psql \\copy TO file`` invocations
    while still targeting ``s3://...`` transparently.

Control plane — runs in Python via psycopg:
    Snapshot export and the hold-open transaction, catalog queries
    (schemas, tables, matviews, ``pg_relation_size`` for page-range
    planning), and SIGINT-driven cancellation (``conn.cancel_safe()``)
    all live here.  None of these carry bulk data.

Schema discovery and snapshot timing (known limitation):
    ``discover_schemas`` / ``discover_tables`` / ``discover_matviews``
    run *before* :func:`export_snapshot`.  Each runs in its own
    short-lived auto-commit transaction — i.e. each sees a different
    point in time, and none of them coincides with the snapshot the
    workers later import.

    Practically: a table CREATEd between discovery and snapshot is
    silently missed (we never enumerate it, so we never dump it),
    and a table DROPped in that window surfaces as a loud
    ``pg_dump --snapshot`` failure that aborts the run with a clear
    error — silent miss vs. loud abort, never silent corruption.

    Coordinating the two (running discovery inside the snapshot
    transaction itself) would require importing the snapshot from
    each discovery connection — a measurable refactor for a window
    of milliseconds against schemas that almost never change during
    a backup.  Operators running concurrent DDL while a database-level
    backup is in flight should expect this race; everyone else is
    unaffected in practice.

DDL for each table is produced by a short-lived ``pg_dump`` subprocess
whose stdout is piped directly into a ``Blob`` via
:func:`~common.stream_subprocess_to_sink`, so gigabyte-sized dumps
don't materialize on local disk when targeting S3.

Works with any heap table regardless of schema or column types.

Storage targets:
    ``--output-dir`` accepts either a local filesystem path or an
    ``s3://bucket/prefix`` URI.  When an S3 URI is used, configure the
    endpoint and credentials via the standard AWS environment
    variables (``AWS_ENDPOINT_URL_S3`` for Ceph/MinIO,
    ``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY``, and
    ``AWS_S3_VERIFY_SSL=false`` for self-signed certificates).

Parameters:
    Required:  --host, --user, --password, --database
    Optional:  --port          (default: 5432)
               --table         (single-table mode; omit for database-level)
               --schema        (single-table mode only; default: 'public')
               --num-parts     (default: 4)
               --output-dir    (local path or s3://... URI; default: /tmp)
               --retries       (max retries for failed parts; default: 0)

Single-table mode (explicit --table):
    python3 src/parallel_dump.py \\
        --host HOST --user USER --password PASS \\
        --database mydb --table mytable --num-parts 4 \\
        --output-dir s3://my-bucket/dumps/mydb/mytable

    Part files land directly under --output-dir.

Database-level mode (omit --table):
    python3 src/parallel_dump.py \\
        --host HOST --user USER --password PASS \\
        --database mydb --num-parts 4 \\
        --output-dir s3://my-bucket/dumps/mydb

    Discovers all user schemas and tables.  Creates nested
    "directories" (real on local disk, key prefixes in S3):
    <output-dir>/<schema>/<table>/
"""

import argparse
import math
import re
import shutil
import subprocess
import sys
import threading
import time

import psycopg

from common import (
    GLOBAL_POST_DATA_FILENAME,
    GLOBAL_PRE_DATA_FILENAME,
    LOG,
    MATVIEW_MARKER_FILENAME,
    MIN_PG_VERSION_TID_SCAN,
    POST_DATA_FILENAME,
    PRE_DATA_FILENAME,
    PROGRESS_INTERVAL,
    SEQUENCES_FILENAME,
    ConnInfo,
    add_connection_args,
    apply_config_defaults,
    apply_logging_config,
    cancel_workers,
    conn_from_args,
    discover_matviews,
    discover_schemas,
    discover_tables,
    install_signal_handlers,
    is_cancelled,
    load_config,
    psql_query,
    qualified_name,
    quote_literal,
    regclass_literal,
    register_conn,
    require_pg_version,
    stream_subprocess_to_sink,
    unregister_conn,
    validate_connection_args,
)
from storage import Blob, is_s3_uri, make_blob
from storage import join as storage_join
from storage import makedirs as storage_makedirs

# pg_export_snapshot() returns IDs like "0000025E-00000002-1" (hex-hex-decimal).
SNAPSHOT_ID_RE = re.compile(r"^[0-9A-Fa-f]+-[0-9A-Fa-f]+-\d+$")

PAGE_SIZE = 8192  # PostgreSQL page size in bytes


# ---------------------------------------------------------------------------
# Page-range detection
# ---------------------------------------------------------------------------


def detect_page_ranges(
    conn: ConnInfo,
    schema: str,
    table: str,
    num_parts: int,
) -> list[tuple[int, int]]:
    """Query table size and split into *num_parts* page ranges for TID range scans.

    Returns contiguous, non-overlapping (lo_page, hi_page) tuples where each
    worker should scan ``ctid >= '(lo_page,0)' AND ctid < '(hi_page,0)'``.
    The last range uses a very large hi_page to capture any pages added
    between the size check and the snapshot export.

    Returns an empty list for empty tables (0 pages).
    """
    rc = regclass_literal(schema, table)
    total_pages = int(
        psql_query(conn, f"SELECT pg_relation_size({rc}::regclass)::bigint / {PAGE_SIZE};")
    )

    size_bytes = total_pages * PAGE_SIZE
    LOG.info("  Table has %s pages (%.2f GB)", f"{total_pages:,}", size_bytes / 1e9)

    if total_pages == 0:
        return []

    step = math.ceil(total_pages / num_parts)
    ranges: list[tuple[int, int]] = []
    lo = 0
    for _ in range(num_parts):
        hi = min(lo + step, total_pages)
        ranges.append((lo, hi))
        lo = hi
        if lo >= total_pages:
            break

    # Extend last range so rows on pages added after the size check
    # (but before the snapshot) are still captured.
    last_lo, _ = ranges[-1]
    ranges[-1] = (last_lo, 2**32 - 1)
    return ranges


# ---------------------------------------------------------------------------
# Disk space check
# ---------------------------------------------------------------------------

DISK_SPACE_MARGIN = 1.2  # 20 % headroom for binary COPY overhead and DDL files


def check_disk_space(
    conn: ConnInfo,
    work: list[tuple[str, str]],
    output_dir: str,
) -> None:
    """Abort if the output filesystem lacks space for the estimated dump.

    Sums ``pg_table_size`` for every table in *work* and compares against
    free space on *output_dir* (with a safety margin for binary COPY
    overhead, DDL files, and filesystem bookkeeping).

    Uses ``pg_table_size`` (main fork + TOAST + FSM + VM) rather than
    ``pg_relation_size`` (main fork only) so the estimate includes
    out-of-line TOAST storage — without it a 1 GB main fork holding
    100 GB of TOASTed JSONB/text would look like 1 GB on the free-space
    gate and then fill the destination mid-stream once ``COPY … BINARY``
    expanded the TOAST pointers.
    """
    _BATCH = 500
    estimated_bytes = 0
    for i in range(0, len(work), _BATCH):
        batch = work[i : i + _BATCH]
        rc_list = " UNION ALL ".join(
            f"SELECT {regclass_literal(s, t)}::regclass AS c" for s, t in batch
        )
        sql = f"SELECT COALESCE(SUM(pg_table_size(q.c)), 0) FROM ({rc_list}) q;"
        estimated_bytes += int(psql_query(conn, sql))
    needed = int(estimated_bytes * DISK_SPACE_MARGIN)

    # S3 bucket capacity isn't something we can probe cheaply (no
    # disk_usage equivalent), and in practice bucket quotas are
    # provisioned to greatly exceed per-backup footprints.  Log the
    # estimate and skip the free-space gate.
    if is_s3_uri(output_dir):
        LOG.info("\nDisk space check:")
        LOG.info("  Estimated dump size : %.2f GB", estimated_bytes / 1e9)
        LOG.info("  Output is S3 — skipping free-space check.")
        return

    storage_makedirs(output_dir)
    free = shutil.disk_usage(output_dir).free

    margin_pct = (DISK_SPACE_MARGIN - 1) * 100
    LOG.info("\nDisk space check:")
    LOG.info("  Estimated dump size : %.2f GB", estimated_bytes / 1e9)
    LOG.info("  With %.0f%% margin     : %.2f GB", margin_pct, needed / 1e9)
    LOG.info("  Free on output dir  : %.2f GB", free / 1e9)

    if free < needed:
        LOG.error(
            "\nERROR: Not enough disk space.  Need %.2f GB but only %.2f GB free on %s.",
            needed / 1e9,
            free / 1e9,
            output_dir,
        )
        sys.exit(1)
    LOG.info("  OK — sufficient space.")


# ---------------------------------------------------------------------------
# DDL export
# ---------------------------------------------------------------------------


def export_ddl(
    conn: ConnInfo,
    schema: str,
    table: str,
    pre_data: Blob,
    post_data: Blob,
    snapshot_id: str | None = None,
) -> None:
    """Export table DDL split into pre-data and post-data sections.

    pre-data  — plain SQL: CREATE TABLE (columns, types, defaults, NOT NULL,
                sequences).  Applied via ``psql -f``.
    post-data — pg_dump custom format: CREATE INDEX, PRIMARY KEY, UNIQUE,
                FOREIGN KEY, CHECK, triggers, rules.  Restored via
                ``pg_restore -j N`` for parallel index builds.

    Splitting lets ``parallel_restore.py`` create a bare table, bulk-load
    data without index overhead, then build indexes and add constraints
    once on the full dataset — much faster than incremental maintenance.

    When *snapshot_id* is provided, ``pg_dump --snapshot`` is used so that
    catalog queries see the same point-in-time as the COPY workers.  This
    prevents DDL/data mismatches if the table is altered between the
    snapshot capture and the DDL export.

    ``--no-owner --no-privileges`` avoids SET OWNER / GRANT statements
    that would fail if the restoring user differs from the original owner.

    Both sections stream straight into the *pre_data*/*post_data*
    :class:`Blob` sinks — for S3 that means the pg_dump bytes flow through
    multipart-upload parts without ever being buffered on local disk.  We
    drop pg_dump's ``-f`` argument and instead capture stdout (so the
    binary custom-format payload and the plain-SQL pre-data are treated
    uniformly), piped chunk-by-chunk into the blob.
    """
    qtable = qualified_name(schema, table)
    base_args = [
        *conn.pg_dump_args(),
        "--schema-only",
        "-t",
        qtable,
        "--no-owner",
        "--no-privileges",
    ]
    if snapshot_id:
        base_args.extend(["--snapshot", snapshot_id])

    # pre-data: plain SQL on stdout, streamed straight into the blob.
    with pre_data.open_write() as sink:
        stream_subprocess_to_sink(
            [*base_args, "--section", "pre-data"],
            sink,
            env=conn.env(),
            label=f"pg_dump --section=pre-data for {qtable}",
        )

    # post-data: custom format (binary) on stdout -> blob.  Previously
    # pg_dump wrote directly to a local path via -f; now we stream so
    # the same code path works for ``s3://`` destinations.
    with post_data.open_write() as sink:
        stream_subprocess_to_sink(
            [*base_args, "--section", "post-data", "-Fc"],
            sink,
            env=conn.env(),
            label=f"pg_dump --section=post-data for {qtable}",
        )


# ---------------------------------------------------------------------------
# Global pre/post-data export
# ---------------------------------------------------------------------------


def _export_global_section(
    conn: ConnInfo,
    output_dir: str,
    snapshot_id: str,
    section: str,
    filename: str,
) -> None:
    """Export database-wide objects not tied to any single table.

    Runs ``pg_dump --section=<section> -T '*.*'`` which excludes all
    per-table objects (``-T '*.*'``) while keeping standalone ones.
    Output is pg_dump custom format (``-Fc``) for dependency ordering.

    Used for both global pre-data (extensions, types, enums, domains,
    standalone functions) and global post-data (event triggers, rules).
    Only used in database-level mode.
    """
    blob = make_blob(storage_join(output_dir, filename))
    cmd = [
        *conn.pg_dump_args(),
        f"--section={section}",
        "-T",
        "*.*",
        "-Fc",
        "--no-owner",
        "--no-privileges",
        "--snapshot",
        snapshot_id,
    ]
    with blob.open_write() as sink:
        stream_subprocess_to_sink(cmd, sink, env=conn.env(), label=f"pg_dump global {section}")
    LOG.info("Global %s exported to %s (%d bytes)", section, blob.uri, blob.size())


def export_global_pre_data(
    conn: ConnInfo,
    output_dir: str,
    snapshot_id: str,
) -> None:
    """Export database-wide pre-data (extensions, types, enums, domains).

    Per-table ``pg_dump -t`` does not capture these objects.  If a
    table column uses ``citext`` or a custom enum, the per-table
    ``CREATE TABLE`` will fail on restore unless the type exists first.

    Only used in database-level mode.
    """
    _export_global_section(conn, output_dir, snapshot_id, "pre-data", GLOBAL_PRE_DATA_FILENAME)


def export_global_post_data(
    conn: ConnInfo,
    output_dir: str,
    snapshot_id: str,
) -> None:
    """Export database-wide post-data (event triggers, standalone rules).

    Restored after all per-table post-data so that event triggers
    don't fire during the restore's own DDL operations.

    Only used in database-level mode.
    """
    _export_global_section(conn, output_dir, snapshot_id, "post-data", GLOBAL_POST_DATA_FILENAME)


# ---------------------------------------------------------------------------
# Sequence value export
# ---------------------------------------------------------------------------


def _parse_seq_rows(raw: str) -> list[tuple[str, str]]:
    """Parse ``schema|name`` rows from psql output into (schema, name) tuples."""
    if not raw:
        return []
    out: list[tuple[str, str]] = []
    for line in raw.splitlines():
        line = line.strip()
        if line:
            s, n = line.split("|", maxsplit=1)
            out.append((s, n))
    return out


def dump_sequences(
    conn: ConnInfo,
    schema: str,
    table: str,
    output_dir: str,
) -> None:
    """Save current values of sequences used by the table's columns.

    Binary COPY (used by both dump and restore) bypasses ``nextval()``,
    so after a restore the sequence position must be explicitly reset
    to avoid duplicate-key errors on subsequent INSERTs.

    Discovers sequences via ``pg_depend`` (covers SERIAL, IDENTITY, and
    standalone sequences referenced by DEFAULT expressions), reads
    ``last_value`` and ``is_called`` from each sequence relation, and
    writes ``SELECT pg_catalog.setval(...)`` statements to
    ``sequences.sql``.  The file is applied by ``parallel_restore.py``
    after data loading.

    For standalone sequences (not owned by any column), ``pg_dump -t``
    does not include a CREATE SEQUENCE statement.  This function detects
    them and prepends their DDL to ``pre_data.sql`` so that the table's
    DEFAULT expression can reference the sequence at restore time.

    Sequences are non-transactional: their values are globally visible
    regardless of snapshot isolation, so this function does not need
    the shared snapshot.
    """
    rc = regclass_literal(schema, table)

    # --- Owned sequences (SERIAL / IDENTITY — deptype 'a' or 'i') ---
    owned_sql = (
        "SELECT n.nspname, s.relname "
        "FROM pg_class s "
        "JOIN pg_namespace n ON n.oid = s.relnamespace "
        "JOIN pg_depend d ON d.objid = s.oid "
        "AND d.classid = 'pg_class'::regclass "
        "AND d.refclassid = 'pg_class'::regclass "
        "AND d.deptype IN ('a', 'i') "
        f"AND d.refobjid = {rc}::regclass "
        "WHERE s.relkind = 'S' "
        "ORDER BY n.nspname, s.relname;"
    )

    # --- Standalone sequences referenced by column DEFAULT expressions ---
    standalone_sql = (
        "SELECT n.nspname, s.relname "
        "FROM pg_class s "
        "JOIN pg_namespace n ON n.oid = s.relnamespace "
        "JOIN pg_depend d ON d.refobjid = s.oid "
        "AND d.refclassid = 'pg_class'::regclass "
        "AND d.classid = 'pg_attrdef'::regclass "
        "AND d.deptype = 'n' "
        "JOIN pg_attrdef ad ON ad.oid = d.objid "
        f"AND ad.adrelid = {rc}::regclass "
        "WHERE s.relkind = 'S' "
        "AND NOT EXISTS ("
        "  SELECT 1 FROM pg_depend d2 "
        "  WHERE d2.objid = s.oid "
        "  AND d2.classid = 'pg_class'::regclass "
        "  AND d2.deptype IN ('a', 'i')"
        ") "
        "ORDER BY n.nspname, s.relname;"
    )

    owned = _parse_seq_rows(psql_query(conn, owned_sql))
    standalone = _parse_seq_rows(psql_query(conn, standalone_sql))
    all_seqs = sorted(set(owned + standalone))

    if not all_seqs:
        return

    # --- Prepend standalone-sequence DDL to pre_data.sql ---
    if standalone:
        _prepend_standalone_seq_ddl(conn, standalone, output_dir)

    # --- Export setval() for all sequences ---
    stmts: list[str] = []
    for seq_schema, seq_name in all_seqs:
        qseq = qualified_name(seq_schema, seq_name)
        state = psql_query(conn, f"SELECT last_value, is_called FROM {qseq};")
        last_value, is_called_pg = state.split("|", maxsplit=1)
        is_called = "true" if is_called_pg == "t" else "false"
        seq_ref = regclass_literal(seq_schema, seq_name)
        stmts.append(f"SELECT pg_catalog.setval({seq_ref}, {last_value}, {is_called});")

    blob = make_blob(storage_join(output_dir, SEQUENCES_FILENAME))
    with blob.open_write() as f:
        for stmt in stmts:
            f.write((stmt + "\n").encode("utf-8"))
    LOG.info("  Saved %d sequence value(s) to %s", len(stmts), SEQUENCES_FILENAME)


def _prepend_standalone_seq_ddl(
    conn: ConnInfo,
    sequences: list[tuple[str, str]],
    output_dir: str,
) -> None:
    """Prepend CREATE SEQUENCE DDL for standalone sequences to pre_data.sql.

    ``pg_dump -t table`` does not include CREATE SEQUENCE for sequences
    that are not owned by a column (i.e. standalone sequences used via
    DEFAULT nextval()).  This function queries ``pg_sequences`` and
    generates the DDL so the restore can create the sequence before the
    table.
    """
    ddl_sql = (
        "SELECT schemaname, sequencename, data_type, "
        "increment_by, min_value, max_value, start_value, "
        "cache_size, cycle "
        "FROM pg_sequences "
        "WHERE (schemaname, sequencename) IN ("
        + ",".join(f"({quote_literal(s)},{quote_literal(n)})" for s, n in sequences)
        + ") ORDER BY schemaname, sequencename;"
    )
    raw = psql_query(conn, ddl_sql)
    if not raw:
        return

    ddl_lines: list[str] = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split("|")
        s_schema, s_name = parts[0], parts[1]
        data_type = parts[2]
        inc, minv, maxv, start, cache = parts[3], parts[4], parts[5], parts[6], parts[7]
        cycle = "CYCLE" if parts[8] == "t" else "NO CYCLE"
        qseq = qualified_name(s_schema, s_name)
        # Use CREATE IF NOT EXISTS (no DROP) so a sequence shared by
        # multiple tables — each of whose pre_data.sql prepends the
        # same DDL — is only created once.  A DROP SEQUENCE on the
        # second table would fail with "cannot drop because other
        # objects depend on it" once the first table's DEFAULT
        # expression has been created.  The trailing setval() in
        # sequences.sql still resets the value, so skipping DDL for
        # an already-existing sequence is safe.
        ddl_lines.append(
            f"CREATE SEQUENCE IF NOT EXISTS {qseq}\n"
            f"    AS {data_type}\n"
            f"    INCREMENT BY {inc}\n"
            f"    MINVALUE {minv}\n"
            f"    MAXVALUE {maxv}\n"
            f"    START WITH {start}\n"
            f"    CACHE {cache}\n"
            f"    {cycle};\n"
        )

    if not ddl_lines:
        return

    # Read-modify-write of pre_data.sql via the blob abstraction.
    # pre_data.sql is small (a few KB at most — CREATE TABLE + defaults),
    # so pulling it fully into memory before rewriting is fine even for
    # S3-backed blobs.
    pre_data_blob = make_blob(storage_join(output_dir, PRE_DATA_FILENAME))
    existing = b""
    if pre_data_blob.exists():
        with pre_data_blob.open_read() as f:
            existing = f.read()
    with pre_data_blob.open_write() as f:
        f.write(b"-- Standalone sequences (not owned by any table column)\n")
        for ddl in ddl_lines:
            f.write((ddl + "\n").encode("utf-8"))
        f.write(existing)


# ---------------------------------------------------------------------------
# Snapshot management
# ---------------------------------------------------------------------------


SNAPSHOT_TIMEOUT = 30  # seconds for connect + pg_export_snapshot() response


def export_snapshot(conn: ConnInfo) -> tuple[psycopg.Connection, str]:
    """Open a long-lived psycopg connection holding a REPEATABLE READ snapshot.

    pg_export_snapshot() returns a snapshot ID that other transactions can
    import via SET TRANSACTION SNAPSHOT, allowing them to see the exact
    same data — even across different connections.

    The snapshot remains valid only as long as the holder transaction is
    open, so callers MUST eventually call close_snapshot_holder().

    Returns (holder_connection, snapshot_id).
    """
    try:
        # autocommit=True lets us emit an explicit BEGIN for the snapshot
        # transaction; psycopg honours the BEGIN and will not auto-commit
        # subsequent statements until our COMMIT / ROLLBACK.
        pg = psycopg.connect(
            conn.conninfo(),
            connect_timeout=SNAPSHOT_TIMEOUT,
            autocommit=True,
        )
    except psycopg.OperationalError as exc:
        LOG.error("ERROR: Failed to open snapshot holder connection: %s", exc)
        sys.exit(1)

    try:
        # Disable timeouts: the holder sits idle for the entire multi-table
        # loop, and server defaults for statement_timeout or
        # idle_in_transaction_session_timeout could kill it mid-dump.
        pg.execute("SET statement_timeout = 0")
        pg.execute("SET idle_in_transaction_session_timeout = 0")

        # REPEATABLE READ, not SERIALIZABLE.  For read-only transactions
        # both give the same snapshot semantics, but SERIALIZABLE adds
        # predicate-lock tracking and can abort on serialization conflicts
        # with concurrent writers — pure risk with zero benefit here.
        pg.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        row = pg.execute("SELECT pg_export_snapshot()").fetchone()
    except psycopg.Error as exc:
        LOG.error("ERROR: pg_export_snapshot() failed: %s", exc)
        pg.close()
        sys.exit(1)

    if not row or not row[0]:
        LOG.error("ERROR: pg_export_snapshot() returned no value.")
        pg.close()
        sys.exit(1)

    snapshot_id = row[0]
    if not SNAPSHOT_ID_RE.match(snapshot_id):
        LOG.error("ERROR: Unexpected snapshot ID format: %r", snapshot_id)
        pg.close()
        sys.exit(1)

    # Register only on the happy path so a botched setup can't leave a
    # dead reference in the cancel registry.  The caller is responsible
    # for calling close_snapshot_holder(), which unregisters.
    register_conn(pg)
    return pg, snapshot_id


def close_snapshot_holder(holder: psycopg.Connection) -> None:
    """Commit the holder transaction and close the connection gracefully.

    Always unregisters the connection from the cancellation registry,
    even if the server-side COMMIT fails (which is expected after
    SIGINT, since ``cancel_workers()`` has already fired a cancel at
    the holder).
    """
    try:
        try:
            holder.execute("COMMIT")
        except psycopg.Error as exc:
            # The connection may already be broken (server restart,
            # SIGINT-cancelled query, etc.).  Swallow — we're closing
            # anyway.
            LOG.debug("close_snapshot_holder: COMMIT failed (ignored): %s", exc)
        try:
            holder.close()
        except psycopg.Error:
            pass
    finally:
        unregister_conn(holder)


# ---------------------------------------------------------------------------
# Snapshot holder watchdog
# ---------------------------------------------------------------------------

WATCHDOG_INTERVAL = 2  # seconds between holder health checks


def snapshot_watchdog(
    holder: psycopg.Connection,
    stop_event: threading.Event,
) -> None:
    """Probe the holder connection and cancel workers if it dies.

    If the holder session dies unexpectedly (server restart, OOM kill,
    network drop), the exported snapshot becomes invalid and every COPY
    worker will eventually fail with ``snapshot ... does not exist``.
    Detecting the death early and cancelling all workers avoids waiting
    for each worker to time out individually.

    Runs as a daemon thread; exits when *stop_event* is set or the
    probe raises.  The probe is a trivial ``SELECT 1`` round-trip — cheap
    and safe to run inside the holder's open REPEATABLE READ transaction.
    """
    while not stop_event.is_set():
        stop_event.wait(WATCHDOG_INTERVAL)
        if stop_event.is_set():
            break
        try:
            with holder.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        except psycopg.Error as exc:
            LOG.error(
                "\nERROR: Snapshot holder died: %s. Cancelling all workers...",
                exc,
            )
            cancel_workers()
            break


# ---------------------------------------------------------------------------
# Parallel COPY worker
# ---------------------------------------------------------------------------


def copy_range(
    conn: ConnInfo,
    qtable: str,
    lo_page: int,
    hi_page: int,
    snapshot_id: str,
    output: Blob,
    part_index: int,
    results: dict[int, dict],
    bytes_written: dict[int, int],
) -> None:
    """Export one page range of the table using the shared snapshot.

    *qtable* is a schema-qualified, double-quoted identifier
    (e.g. ``"public"."mytable"``).

    Shells out to ``psql`` and pipes its ``COPY (…) TO STDOUT`` binary
    output straight into *output* (a :class:`storage.Blob`) via
    :func:`~common.stream_subprocess_to_sink`.  Keeping the data bytes
    inside a single native subprocess — libpq buffer → psql → pipe →
    sink — avoids the per-chunk Python/GIL overhead a psycopg
    ``Copy.read()`` loop would incur on this hot path, which measurably
    throttled parallel dumps (GIL contention dropped throughput well
    below single-thread `psql` speed).  Using a TID range scan
    (PostgreSQL 14+) on ``ctid`` so each worker touches only its
    assigned heap pages — no coordination, no full table scans, no
    column knowledge.

    The snapshot-importing ``BEGIN``, ``SET TRANSACTION SNAPSHOT`` and
    terminating ``COMMIT`` live inside the same psql session as the
    ``COPY`` so every worker reads from the shared snapshot that the
    dump's holder connection keeps open.  ``-q -v ON_ERROR_STOP=1``
    suppresses stray command tags (so only binary COPY bytes reach the
    sink) and turns any server-side error into a non-zero exit (raised
    as :class:`subprocess.CalledProcessError` by
    ``stream_subprocess_to_sink``).  ``conn.psql_args()`` already
    includes ``-X`` so no ``~/.psqlrc`` side effects can alter output.

    Cancellation: ``stream_subprocess_to_sink`` registers the psql
    subprocess in the global cancel registry; :func:`~common.cancel_workers`
    SIGTERMs it on SIGINT.  A terminated subprocess surfaces as
    ``CalledProcessError`` which we classify as ``cancelled`` when
    :func:`is_cancelled` is set.

    *bytes_written* is a shared counter (``{part_index: int}``) that
    :func:`progress_monitor` reads to report throughput.  We publish
    live updates into it via the ``bytes_callback`` hook that
    ``stream_subprocess_to_sink`` fires after each sink-write — the
    payload bytes themselves never enter Python, but the cumulative
    *count* is cheap enough (one dict assignment per chunk) to keep
    the progress display alive.  Reset to 0 on entry so retries don't
    double-count a previous attempt.
    """
    bytes_written[part_index] = 0
    LOG.info(
        "[part %d] pages %d..%d -> %s",
        part_index,
        lo_page,
        hi_page,
        output.uri,
    )

    # snapshot_id is validated against SNAPSHOT_ID_RE by
    # export_snapshot() before we ever reach this worker, and qtable
    # comes from qualified_name() which double-quotes both schema and
    # table.  lo_page / hi_page are Python ints.  Embedding these in
    # the SQL literal carries no injection risk.
    sql_script = (
        "SET statement_timeout = 0;\n"
        "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;\n"
        f"SET TRANSACTION SNAPSHOT '{snapshot_id}';\n"
        f"COPY (SELECT * FROM {qtable} "
        f"WHERE ctid >= '({lo_page},0)' AND ctid < '({hi_page},0)') "
        "TO STDOUT WITH (FORMAT binary);\n"
        "COMMIT;\n"
    )
    cmd = [
        *conn.psql_args(),
        "-q",
        "-v",
        "ON_ERROR_STOP=1",
        "-c",
        sql_script,
    ]

    start = time.time()
    try:
        with output.open_write() as sink:
            stream_subprocess_to_sink(
                cmd,
                sink,
                env=conn.env(),
                label=f"psql COPY part {part_index} of {qtable}",
                bytes_callback=lambda n, i=part_index: bytes_written.__setitem__(i, n),
            )
        elapsed = time.time() - start
        # Prefer the last callback value (already set in bytes_written)
        # so restarts / S3 eventual-consistency quirks can't make the
        # reported size disagree with what we actually streamed.
        total = bytes_written[part_index]
        LOG.info(
            "[part %d] Done in %.1fs (%.2f GB)",
            part_index,
            elapsed,
            total / 1e9,
            extra={
                "part": part_index,
                "elapsed_s": elapsed,
                "status": "ok",
                "bytes": total,
            },
        )
        results[part_index] = {"status": "ok", "elapsed": elapsed, "bytes": total}
    except subprocess.CalledProcessError as exc:
        elapsed = time.time() - start
        if is_cancelled():
            LOG.info(
                "[part %d] Cancelled after %.1fs",
                part_index,
                elapsed,
                extra={"part": part_index, "elapsed_s": elapsed, "status": "cancelled"},
            )
            results[part_index] = {"status": "cancelled", "elapsed": elapsed, "bytes": 0}
        else:
            LOG.error(
                "[part %d] FAILED after %.1fs: %s",
                part_index,
                elapsed,
                exc,
                extra={
                    "part": part_index,
                    "elapsed_s": elapsed,
                    "status": "error",
                },
            )
            results[part_index] = {
                "status": "error",
                "elapsed": elapsed,
                "bytes": 0,
                "error": str(exc),
            }
    except OSError as exc:
        # E.g. local disk full while streaming to a LocalBlob sink,
        # or a network error while uploading to an S3Blob.
        elapsed = time.time() - start
        LOG.error(
            "[part %d] FAILED after %.1fs (I/O): %s",
            part_index,
            elapsed,
            exc,
            extra={"part": part_index, "elapsed_s": elapsed, "status": "error"},
        )
        results[part_index] = {
            "status": "error",
            "elapsed": elapsed,
            "bytes": 0,
            "error": str(exc),
        }


# ---------------------------------------------------------------------------
# Progress monitor
# ---------------------------------------------------------------------------


def progress_monitor(
    bytes_written: dict[int, int],
    num_parts: int,
    stop_event: threading.Event,
) -> None:
    """Report live throughput by polling the workers' shared byte counter.

    Runs in a daemon thread alongside the COPY workers.  Each worker
    updates ``bytes_written[part_index]`` after every chunk; this thread
    sums the snapshot every :data:`PROGRESS_INTERVAL` seconds and logs
    the delta-based throughput.

    Works for any storage backend — local files, S3, etc. — because the
    source of truth is the in-process counter, not the filesystem.
    """
    prev_total = 0
    prev_snapshot: dict[int, int] = {}
    prev_time = time.time()
    while not stop_event.is_set():
        stop_event.wait(PROGRESS_INTERVAL)
        if stop_event.is_set():
            break
        # dict.items() is atomic under the GIL; a concurrent update
        # is harmless because we only need an approximate snapshot.
        snapshot = dict(bytes_written.items())
        now = time.time()
        total = sum(snapshot.values())
        dt = now - prev_time
        throughput = (total - prev_total) / dt if dt > 0 else 0
        # "active" = parts whose counter *grew* since the last sample.
        # A simple `s > 0` test would count parts that already finished
        # (workers never reset their per-part counter on completion,
        # because total must stay monotonic for throughput math), making
        # the display stick at e.g. "8/8 parts active" long after seven
        # of them had exited.
        active = sum(1 for pi, s in snapshot.items() if s > prev_snapshot.get(pi, 0))
        prev_total = total
        prev_snapshot = snapshot
        prev_time = now
        LOG.info(
            "  Progress: %.2f GB written, %.1f MB/s, %d/%d parts active",
            total / 1e9,
            throughput / 1e6,
            active,
            num_parts,
            extra={"bytes": total, "throughput_mbps": round(throughput / 1e6, 1)},
        )


# ---------------------------------------------------------------------------
# Per-table dump
# ---------------------------------------------------------------------------


def _log_dump_summary(
    results: dict[int, dict],
    num_parts: int,
    overall_elapsed: float,
) -> None:
    """Log part-file sizes and aggregate throughput after a dump run.

    Uses the byte counts already recorded in *results* by each worker
    rather than re-statting the blob sink.  For S3-backed dumps this
    avoids one ``HeadObject`` network round-trip per part (the sink
    itself knows what it streamed — there's no reason to ask the
    server).
    """
    total_bytes = sum(r.get("bytes", 0) for r in results.values() if r.get("status") == "ok")

    LOG.info("\n%s", "=" * 60)
    LOG.info(
        "Completed %d parts in %.1fs total",
        num_parts,
        overall_elapsed,
        extra={"num_parts": num_parts, "elapsed_s": overall_elapsed, "bytes": total_bytes},
    )
    if overall_elapsed > 0:
        LOG.info(
            "Total size: %.2f GB, avg throughput: %.1f MB/s",
            total_bytes / 1e9,
            total_bytes / overall_elapsed / 1e6,
            extra={
                "bytes": total_bytes,
                "throughput_mbps": round(total_bytes / overall_elapsed / 1e6, 1),
            },
        )
    LOG.info("")
    for i in sorted(results):
        r = results[i]
        LOG.info("  part %d: %9s  (%.1fs)", i, r["status"].upper(), r["elapsed"])


def _retry_failed_parts(
    conn: ConnInfo,
    qtable: str,
    ranges: list[tuple[int, int]],
    snapshot_id: str,
    blobs: dict[int, Blob],
    results: dict[int, dict],
    bytes_written: dict[int, int],
    max_retries: int,
) -> None:
    """Re-run failed COPY workers up to *max_retries* times.

    Each retry deletes the partial blob before re-launching the worker
    (so a corrupt write doesn't get overlaid by a shorter retry output).
    Modifies *results* and *bytes_written* in place.
    """
    for attempt in range(1, max_retries + 1):
        retry_indices = [
            i for i in range(1, len(ranges) + 1) if i not in results or results[i]["status"] != "ok"
        ]
        if not retry_indices or is_cancelled():
            break

        LOG.info(
            "\n--- Retry %d/%d: re-running %d failed parts ---",
            attempt,
            max_retries,
            len(retry_indices),
        )
        for i in retry_indices:
            blobs[i].delete()

        retry_threads: list[threading.Thread] = []
        for i in retry_indices:
            lo, hi = ranges[i - 1]
            t = threading.Thread(
                target=copy_range,
                args=(
                    conn,
                    qtable,
                    lo,
                    hi,
                    snapshot_id,
                    blobs[i],
                    i,
                    results,
                    bytes_written,
                ),
            )
            retry_threads.append(t)
        for t in retry_threads:
            t.start()
        for t in retry_threads:
            t.join()

        for i in sorted(retry_indices):
            r = results.get(i, {"status": "missing", "elapsed": 0})
            LOG.info("  part %d: %9s  (%.1fs)", i, r["status"].upper(), r["elapsed"])


def _check_dump_results(
    results: dict[int, dict],
    num_parts: int,
) -> bool:
    """Log final status and return True if all parts succeeded."""
    missing = [i for i in range(1, num_parts + 1) if i not in results]
    if missing:
        LOG.error("\nMISSING parts (worker crashed): %s", missing)
    failed = [k for k, v in sorted(results.items()) if v["status"] != "ok"]
    if failed:
        LOG.error("\nFAILED parts: %s", failed)

    ok = not missing and not failed
    if ok:
        LOG.info("\nAll parts succeeded.")
    return ok


def dump_table(
    conn: ConnInfo,
    schema: str,
    table: str,
    num_parts: int,
    output_dir: str,
    snapshot_id: str,
    max_retries: int = 0,
) -> bool:
    """Dump one table using a shared snapshot.

    Exports DDL (pre-data + post-data), detects page ranges, and launches
    parallel COPY workers to write part files into *output_dir*.

    *snapshot_id* must come from a still-open ``export_snapshot()`` holder
    so all workers see the same point-in-time view.

    Returns True on success, False if any part failed.
    """
    qtable = qualified_name(schema, table)
    display_table = f"{schema}.{table}"

    # --- Export table DDL ---------------------------------------------------
    pre_data_blob = make_blob(storage_join(output_dir, PRE_DATA_FILENAME))
    post_data_blob = make_blob(storage_join(output_dir, POST_DATA_FILENAME))
    LOG.info("Exporting DDL for %s (pre-data + post-data)...", display_table)
    export_ddl(conn, schema, table, pre_data_blob, post_data_blob, snapshot_id)

    # --- Export sequence values --------------------------------------------
    dump_sequences(conn, schema, table, output_dir)

    # --- Auto-detect page ranges from table size ---------------------------
    LOG.info("Auto-detecting page ranges for %s (%d parts)...", display_table, num_parts)
    ranges = detect_page_ranges(conn, schema, table, num_parts)

    if not ranges:
        LOG.info("\n%s is empty — DDL exported, no data to dump.", display_table)
        return True

    LOG.info(
        "\nExporting %d partitions from %s (binary format)",
        len(ranges),
        display_table,
        extra={"num_parts": len(ranges), "table": display_table},
    )
    for i, (lo, hi) in enumerate(ranges, 1):
        LOG.info("  part %d: pages %d..%d", i, lo, hi)

    # --- Launch parallel COPY workers --------------------------------------
    results: dict[int, dict] = {}
    bytes_written: dict[int, int] = {i: 0 for i in range(1, len(ranges) + 1)}
    # make_blob dispatches on scheme: local paths produce LocalBlob,
    # ``s3://…`` prefixes produce S3Blob — no code change needed here
    # to flip a single table's parts onto object storage.  storage_join
    # is URI-aware, so it picks ``/`` for S3 and os.path.join otherwise.
    blobs: dict[int, Blob] = {
        i: make_blob(storage_join(output_dir, f"part{i}.bin")) for i in range(1, len(ranges) + 1)
    }
    threads: list[threading.Thread] = []

    for i, (lo, hi) in enumerate(ranges, start=1):
        t = threading.Thread(
            target=copy_range,
            args=(
                conn,
                qtable,
                lo,
                hi,
                snapshot_id,
                blobs[i],
                i,
                results,
                bytes_written,
            ),
        )
        threads.append(t)

    stop_progress = threading.Event()
    monitor = threading.Thread(
        target=progress_monitor,
        args=(bytes_written, len(ranges), stop_progress),
        daemon=True,
    )

    LOG.info("\nStarting %d parallel workers...\n", len(threads))
    overall_start = time.time()
    monitor.start()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    stop_progress.set()
    monitor.join(timeout=2)
    overall_elapsed = time.time() - overall_start

    _log_dump_summary(results, len(ranges), overall_elapsed)
    _retry_failed_parts(
        conn,
        qtable,
        ranges,
        snapshot_id,
        blobs,
        results,
        bytes_written,
        max_retries,
    )
    return _check_dump_results(results, len(ranges))


# ---------------------------------------------------------------------------
# Materialized view dump (DDL only)
# ---------------------------------------------------------------------------


def dump_matview(
    conn: ConnInfo,
    schema: str,
    matview: str,
    output_dir: str,
    snapshot_id: str,
) -> None:
    """Dump materialized view DDL only (no data).

    Materialized views cannot be loaded via ``COPY FROM`` — they must
    be populated via ``REFRESH MATERIALIZED VIEW``.  So we only export:

    * pre-data DDL (``CREATE MATERIALIZED VIEW ... WITH NO DATA``)
    * post-data DDL (indexes on the mat-view, in custom format)

    An empty marker file (``matview``) is written to the output directory
    so ``parallel_restore.py`` can distinguish mat-view directories from
    regular table directories and handle them in a separate phase.
    """
    storage_makedirs(output_dir)
    display = f"{schema}.{matview}"

    pre_data_blob = make_blob(storage_join(output_dir, PRE_DATA_FILENAME))
    post_data_blob = make_blob(storage_join(output_dir, POST_DATA_FILENAME))

    LOG.info("\nExporting DDL for materialized view %s...", display)
    export_ddl(conn, schema, matview, pre_data_blob, post_data_blob, snapshot_id)

    # The marker's only job is to exist, so an empty blob is enough.
    # `discover_parts` on the restore side will notice the marker and
    # route the directory through the matview-refresh code path.
    marker = make_blob(storage_join(output_dir, MATVIEW_MARKER_FILENAME))
    with marker.open_write():
        pass

    LOG.info("  %s: DDL exported (data will be refreshed on restore)", display)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Parallel COPY export via shared PostgreSQL snapshot",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    add_connection_args(parser)
    parser.add_argument(
        "--schema",
        default=None,
        help="Optional. Schema containing the table (single-table mode only, "
        "defaults to 'public').  Ignored in database-level mode.",
    )
    parser.add_argument(
        "--table",
        default=None,
        help="Optional. Table to export (single-table mode).  "
        "Omit to dump all tables (database-level mode).",
    )
    parser.add_argument(
        "--num-parts",
        type=int,
        default=None,
        help="Optional. Number of parallel parts (default: 4)",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Optional. Directory for output part files (default: /tmp)",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=None,
        help="Optional. Max retries for failed COPY workers (default: 0, no retry)",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    apply_config_defaults(args, config, "dump")
    apply_logging_config(args, config)
    validate_connection_args(args)

    if args.num_parts is None:
        args.num_parts = 4
    if args.output_dir is None:
        args.output_dir = "/tmp"
    if args.retries is None:
        args.retries = 0

    if args.num_parts < 1:
        parser.error("--num-parts must be at least 1")
    if args.retries < 0:
        parser.error("--retries must be >= 0")

    conn = conn_from_args(args)
    install_signal_handlers()
    require_pg_version(conn, MIN_PG_VERSION_TID_SCAN, "TID range scan parallel dump")

    try:
        _run_dump(conn, args)
    except subprocess.CalledProcessError:
        # pg_dump / psql wrapper already logged stderr at ERROR level.
        # Suppress the Python traceback and exit with a clean code.
        sys.exit(1)
    except psycopg.Error as exc:
        # Symmetric treatment for control-plane failures: a dropped
        # snapshot connection, ``pg_export_snapshot()`` returning an
        # error, or a catalog query racing with concurrent DDL all
        # raise ``psycopg.Error``.  ``str(exc)`` carries the SQLSTATE
        # and diagnostic detail; a single ERROR line is cleaner than
        # the default Python traceback.
        LOG.error("ERROR: %s", exc)
        sys.exit(1)


def _run_dump(conn: ConnInfo, args: argparse.Namespace) -> None:
    # --- Determine (schema, table) work list -------------------------------
    # Returns list of (schema, table) pairs and whether this is a
    # multi-table dump (which uses <output-dir>/<schema>/<table>/ layout).
    matview_work: list[tuple[str, str]] = []
    if args.table:
        schema = args.schema or "public"
        work: list[tuple[str, str]] = [(schema, args.table)]
        multi = False
    else:
        LOG.info("\nDiscovering schemas...")
        schemas = discover_schemas(conn)
        if not schemas:
            LOG.info("No user schemas found in the database.")
            return
        LOG.info("Found %d schemas: %s", len(schemas), ", ".join(schemas))

        work = []
        for s in schemas:
            tables = discover_tables(conn, s)
            for t in tables:
                work.append((s, t))

        if not work:
            LOG.info("No tables found in database.")
            return

        LOG.info(
            "\n%d tables to dump (largest first within each schema):",
            len(work),
            extra={"num_tables": len(work)},
        )
        for s, t in work:
            LOG.info("  %s.%s", s, t)

        for s in schemas:
            for mv in discover_matviews(conn, s):
                matview_work.append((s, mv))

        if matview_work:
            LOG.info("\n%d materialized views to dump (DDL only):", len(matview_work))
            for s, mv in matview_work:
                LOG.info("  %s.%s", s, mv)

        multi = True

    check_disk_space(conn, work, args.output_dir)

    # --- Export a shared snapshot ------------------------------------------
    LOG.info("\nExporting snapshot...")
    holder, snapshot_id = export_snapshot(conn)
    LOG.info(
        "Snapshot ID: %s\n",
        snapshot_id,
        extra={"snapshot_id": snapshot_id},
    )

    stop_watchdog = threading.Event()
    watchdog = threading.Thread(
        target=snapshot_watchdog,
        args=(holder, stop_watchdog),
        daemon=True,
    )
    watchdog.start()

    try:
        if multi:
            LOG.info("Exporting global pre-data (extensions, types, enums, domains)...")
            export_global_pre_data(conn, args.output_dir, snapshot_id)

        all_ok = True
        for schema, table in work:
            if is_cancelled():
                all_ok = False
                break
            if multi:
                table_dir = storage_join(args.output_dir, schema, table)
            else:
                table_dir = args.output_dir
            storage_makedirs(table_dir)

            ok = dump_table(
                conn,
                schema,
                table,
                args.num_parts,
                table_dir,
                snapshot_id,
                max_retries=args.retries,
            )
            if not ok:
                all_ok = False

        if multi and all_ok and not is_cancelled() and matview_work:
            for schema, matview in matview_work:
                if is_cancelled():
                    break
                mv_dir = storage_join(args.output_dir, schema, matview)
                dump_matview(conn, schema, matview, mv_dir, snapshot_id)

        if multi and all_ok and not is_cancelled():
            LOG.info("\nExporting global post-data (standalone functions, event triggers)...")
            export_global_post_data(conn, args.output_dir, snapshot_id)

    finally:
        stop_watchdog.set()
        watchdog.join(timeout=WATCHDOG_INTERVAL + 1)
        LOG.info("\nClosing snapshot holder...")
        close_snapshot_holder(holder)

    if multi:
        LOG.info("\n%s", "=" * 60)
        mv_msg = f", {len(matview_work)} materialized views" if matview_work else ""
        LOG.info("Dump complete: %d tables%s", len(work), mv_msg)
        for schema, table in work:
            LOG.info("  %s.%s -> %s/", schema, table, storage_join(args.output_dir, schema, table))
        for schema, matview in matview_work:
            LOG.info(
                "  %s.%s -> %s/ (matview)",
                schema,
                matview,
                storage_join(args.output_dir, schema, matview),
            )

    if not all_ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
