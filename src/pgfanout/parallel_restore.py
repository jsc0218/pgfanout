#!/usr/bin/env python3
"""Parallel COPY-FROM restore of part files produced by parallel_dump.py.

Each part file (``part*.bin``, PostgreSQL binary COPY format) is
streamed straight from storage into the target table via a ``psql``
subprocess running ``COPY ... FROM STDIN WITH (FORMAT binary)``.  Parts
load concurrently; each worker runs an independent implicit
transaction that commits on success and rolls back on failure, so a
retry of a failed part is always safe.

Data plane — stays out of Python:
    Each worker opens the part :class:`~storage.Blob` and pumps its
    bytes into ``psql``'s stdin via
    :func:`~common.stream_source_to_subprocess`.  The payload travels
    ``Blob → pipe → psql → libpq → Postgres`` inside a single native
    subprocess; Python only runs the ``read()/write()`` loop that
    drives the pipe, releasing the GIL on every syscall.  This lets
    parallel workers scale linearly and matches the throughput of a
    hand-rolled ``psql \\copy FROM file`` while accepting S3 sources
    transparently.

Control plane — runs in Python via psycopg:
    Layout discovery (``list_dir`` / ``is_dir`` / ``is_file`` against
    a local path or an S3 prefix), the ``pg_stat_progress_copy`` poll
    driving the progress monitor, retry bookkeeping, and SIGINT-driven
    cancellation live here.  None of these carry bulk data.

Table preparation: the target table is always dropped (DROP TABLE IF
EXISTS) and recreated from the ``pre_data.sql`` / ``post_data.dump``
blobs produced by ``parallel_dump.py``.  ``pre_data.sql`` (bare CREATE
TABLE) is applied via ``psql -f`` before loading; ``post_data.dump``
(indexes, constraints, triggers in ``pg_dump`` custom format) is
restored via ``pg_restore`` after all data is loaded — bulk-building
indexes on the full dataset is much faster than incremental maintenance
per row.

Because ``psql -f`` and ``pg_restore`` need a real filesystem path, DDL
blobs are transparently materialized to a temp file via
:func:`~storage.localize` when the source is S3; local-filesystem
blobs are passed through zero-copy.

No snapshot coordination is needed on the restore side: each worker's
``COPY FROM`` is independent.

Storage targets:
    ``--input-dir`` accepts either a local filesystem path or an
    ``s3://bucket/prefix`` URI.  When an S3 URI is used, configure the
    endpoint and credentials via the standard AWS environment
    variables (``AWS_ENDPOINT_URL_S3`` for Ceph/MinIO,
    ``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY``, and
    ``AWS_S3_VERIFY_SSL=false`` for self-signed certificates).

Parameters:
    Required:  --host, --user, --password, --database, --input-dir
    Optional:  --port          (default: 5432)
               --table         (single-table mode; omit for database-level)
               --schema        (single-table mode only; default: 'public')
               --index-jobs    (parallel pg_restore jobs for index builds; default: 1)
               --unlogged      (SET UNLOGGED during load, SET LOGGED after)
               --retries       (max retries for failed parts; default: 0)

Single-table mode (explicit --table):
    pgfanout-restore \\
        --host HOST --user USER --password PASS \\
        --database mydb --table mytable \\
        --input-dir s3://my-bucket/dumps/mydb/mytable

    Files are read directly from --input-dir.

Database-level mode (omit --table):
    pgfanout-restore \\
        --host HOST --user USER --password PASS \\
        --database mydb --input-dir s3://my-bucket/dumps/mydb

    Auto-detects the <schema>/<table>/ layout produced by a
    database-level dump (discovery uses ``list_objects_v2`` with
    ``Delimiter='/'`` for S3 URIs).  Restores in phases:
      Phase 0  — global pre-data (extensions, types, enums, domains)
      Phase 1  — load all tables (data only, no indexes)
      Phase 2a — PKs, indexes, unique constraints (no FKs)
      Phase 2b — FK constraints (after all PKs exist)
      Phase 2c — sequence values (setval to dump-time positions)
      Phase 2d — global post-data (event triggers, standalone rules)
      Phase 3  — ANALYZE all tables
      Phase 4  — materialized views (create, refresh, index, analyze)
"""

import argparse
import os
import re
import subprocess
import sys
import tempfile
import threading
import time

import psycopg

from .common import (
    GLOBAL_POST_DATA_FILENAME,
    GLOBAL_PRE_DATA_FILENAME,
    LOG,
    MATVIEW_MARKER_FILENAME,
    POST_DATA_FILENAME,
    PRE_DATA_FILENAME,
    PROGRESS_INTERVAL,
    SEQUENCES_FILENAME,
    ConnInfo,
    add_connection_args,
    apply_config_defaults,
    apply_logging_config,
    check_subprocess_result,
    conn_from_args,
    install_signal_handlers,
    is_cancelled,
    load_config,
    psql_exec,
    qualified_name,
    quote_ident,
    regclass_literal,
    run_cancellable,
    stream_source_to_subprocess,
    tracked_connection,
    validate_connection_args,
)
from .storage import Blob, is_dir, is_file, list_dir, localize, make_blob
from .storage import join as storage_join

# Chunk size used when streaming a part blob into psql's stdin for
# COPY FROM STDIN.  Large enough to amortise per-write pipe-syscall
# overhead; small enough that :func:`is_cancelled`-aware helpers in
# ``stream_source_to_subprocess`` still react to SIGINT within a few
# milliseconds (the subprocess is terminated directly by the signal
# handler — chunk size only bounds the "how much extra gets written
# before EOF propagates" window).
RESTORE_CHUNK_SIZE = 1 << 20  # 1 MiB

# ---------------------------------------------------------------------------
# Part file discovery
# ---------------------------------------------------------------------------


_PART_RE = re.compile(r"^part(\d+)\.bin$")


def discover_parts(input_dir: str) -> list[Blob]:
    """Find ``part<N>.bin`` blobs under *input_dir*.

    Returns a list of :class:`Blob` sorted numerically by part number,
    or an empty list when no part files exist (empty-table backup).
    Files that don't match the strict ``part<digits>.bin`` pattern are
    ignored.

    *input_dir* may be a local filesystem directory or an ``s3://``
    prefix — :func:`storage.list_dir` abstracts the difference.  A
    missing local directory propagates :class:`FileNotFoundError` (same
    contract as ``os.listdir``); a missing / empty S3 prefix returns an
    empty list without error.
    """
    found: list[tuple[int, Blob]] = []
    for name in list_dir(input_dir):
        m = _PART_RE.match(name)
        if m:
            found.append((int(m.group(1)), make_blob(storage_join(input_dir, name))))
    found.sort(key=lambda pair: pair[0])
    return [blob for _, blob in found]


def discover_dump_dirs(input_dir: str) -> list[tuple[str, str, str]]:
    """Detect the <schema>/<table>/ layout produced by database-level dumps.

    Scans *input_dir* for two-level subdirectory nesting whose leaf
    contains ``pre_data.sql``.  Works transparently for local paths and
    ``s3://`` prefixes — each returned ``dir_path`` carries the original
    scheme, so downstream callers can keep passing it through the same
    :func:`storage.*` helpers.

    Returns ``(schema, table, dir_path)`` triples sorted by
    ``(schema, table)``, or an empty list if no nested layout is found.
    """
    result: list[tuple[str, str, str]] = []
    try:
        entries = list_dir(input_dir)
    except FileNotFoundError:
        return []
    for schema_name in entries:
        schema_path = storage_join(input_dir, schema_name)
        if not is_dir(schema_path):
            continue
        for table_name in list_dir(schema_path):
            table_path = storage_join(schema_path, table_name)
            if not is_dir(table_path):
                continue
            if is_file(storage_join(table_path, PRE_DATA_FILENAME)):
                result.append((schema_name, table_name, table_path))
    return result


# ---------------------------------------------------------------------------
# DDL application
# ---------------------------------------------------------------------------


def apply_ddl(conn: ConnInfo, ddl: Blob) -> None:
    """Execute a pg_dump-produced plain-SQL DDL script via ``psql -f``.

    Used for the pre-data section (CREATE TABLE).  Post-data is in
    pg_dump custom format and restored via ``restore_post_data()``.

    The DDL may live in S3 or on local disk — :func:`storage.localize`
    hides the difference.  For local blobs the script is streamed
    directly by psql (zero-copy); for S3 blobs it is downloaded to a
    temp file first, then the temp file's path is passed to ``psql -f``.
    """
    with localize(ddl) as path:
        result = run_cancellable(
            [*conn.psql_args(), "-v", "ON_ERROR_STOP=1", "-f", path],
            env=conn.env(),
        )
        check_subprocess_result(result, f"apply_ddl for {ddl.uri}")


def _run_pg_restore(
    conn: ConnInfo,
    local_dump_path: str,
    jobs: int = 1,
    toc_file: str | None = None,
    label: str | None = None,
) -> None:
    """Low-level ``pg_restore`` invocation on an already-local dump file.

    Kept separate from :func:`restore_post_data` so callers that already
    hold a localized path (e.g. after running ``pg_restore -l`` for TOC
    filtering) don't download the blob twice.  When *jobs* > 1,
    ``pg_restore -j N`` builds independent indexes / constraints in
    parallel — significantly faster for tables with many indexes.

    *toc_file*, when set, is passed as ``pg_restore -L <toc_file>`` to
    restore only the items listed in the file.  *label* is used in the
    error message (defaults to the dump path) for callers passing a
    temp-file path that's meaningless to a user.
    """
    cmd = [
        *conn.pg_restore_args(),
        "--no-owner",
        "--no-privileges",
    ]
    if jobs > 1:
        cmd.extend(["-j", str(jobs)])
    if toc_file:
        cmd.extend(["-L", toc_file])
    cmd.append(local_dump_path)

    result = run_cancellable(cmd, env=conn.env())
    check_subprocess_result(result, f"pg_restore for {label or local_dump_path}")


def _filter_post_data_toc(dump_path: str, only_fk: bool) -> str | None:
    """Write a filtered pg_restore TOC list to a temp file.

    ``pg_restore -l`` lists all objects in the archive.  We filter to
    either keep only FK CONSTRAINT entries (*only_fk=True*) or keep
    everything except FK CONSTRAINTs (*only_fk=False*).

    Returns the path to the temp file, or None if the filtered list
    contains no actionable entries.  Caller is responsible for cleanup.
    """
    result = run_cancellable(["pg_restore", "-l", dump_path])
    check_subprocess_result(result, f"pg_restore -l for {dump_path}")

    filtered_lines: list[str] = []
    has_items = False
    for line in result.stdout.splitlines():
        if line.startswith(";") or not line.strip():
            filtered_lines.append(line)
            continue
        is_fk = " FK CONSTRAINT " in line
        if (only_fk and is_fk) or (not only_fk and not is_fk):
            filtered_lines.append(line)
            has_items = True

    if not has_items:
        return None

    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".list", prefix="pg_restore_toc_", delete=False
    )
    try:
        tmp.write("\n".join(filtered_lines) + "\n")
    except BaseException:
        # Never hand the caller a path it doesn't know to clean up.  If
        # the write fails (e.g. disk full mid-write) the temp file on
        # disk is orphaned because the caller sees no return value and
        # therefore cannot run ``os.unlink`` in its ``finally``.  Close
        # the handle and best-effort unlink before re-raising.
        tmp.close()
        try:
            os.unlink(tmp.name)
        except OSError:
            pass
        raise
    tmp.close()
    return tmp.name


# ---------------------------------------------------------------------------
# Parallel COPY FROM worker
# ---------------------------------------------------------------------------


def copy_part(
    conn: ConnInfo,
    qtable: str,
    source: Blob,
    part_index: int,
    results: dict[int, dict],
    size_hint: int = 0,
) -> None:
    """Load one part blob into the table via ``COPY ... FROM STDIN``.

    *qtable* is a schema-qualified, double-quoted identifier
    (e.g. ``"public"."mytable"``).

    Shells out to ``psql`` running a single ``COPY FROM STDIN WITH
    (FORMAT binary)`` and pumps *source* directly into its stdin via
    :func:`~common.stream_source_to_subprocess`.  The payload bytes
    therefore travel ``source → pipe → psql → libpq → Postgres`` inside
    a single native subprocess — no Python on the hot path — which is
    what avoids the GIL-serialised throughput collapse we observed with
    psycopg's ``cursor.copy().write()`` loop in multi-threaded restores.

    ``-q -v ON_ERROR_STOP=1`` suppresses stray command tags (psql's
    stdout is redirected to ``DEVNULL`` anyway) and turns any
    server-side error into a non-zero exit.  ``conn.psql_args()``
    already supplies ``-X`` so no ``~/.psqlrc`` can interfere.

    Atomicity: ``psql`` runs the COPY in its own implicit transaction
    which commits on clean exit and rolls back on any error or on
    SIGTERM.  No partial rows are ever visible, so the caller's retry
    loop can safely re-run a failed part.

    Cancellation: ``stream_source_to_subprocess`` registers the psql
    subprocess in the global cancel registry;
    :func:`~common.cancel_workers` SIGTERMs it on SIGINT.  A terminated
    subprocess surfaces as ``CalledProcessError``, which we classify as
    ``cancelled`` when :func:`is_cancelled` is set.

    *size_hint* is the blob's byte size if the caller has already
    stat'd it (``restore_table_data`` does so when sizing each part
    for the "Found N part files" log).  Passing it in avoids a
    per-worker ``HeadObject`` round-trip on S3-backed restores —
    without the hint we'd issue N HEADs in parallel every time a set
    of parts is launched, just to print each part's size in the
    "Starting" log line.  Zero means "unknown, stat it here".
    """
    size = size_hint if size_hint else source.size()
    LOG.info("[part %d] %s (%.1f MB)", part_index, source.uri, size / 1e6)

    # qtable comes from qualified_name() which double-quotes both
    # schema and table; no injection risk from string-embedding it.
    sql_script = f"SET statement_timeout = 0;\nCOPY {qtable} FROM STDIN WITH (FORMAT binary);\n"
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
        with source.open_read() as src:
            stream_source_to_subprocess(
                cmd,
                src,
                env=conn.env(),
                label=f"psql COPY FROM part {part_index} of {qtable}",
                chunk_size=RESTORE_CHUNK_SIZE,
            )
        elapsed = time.time() - start
        LOG.info(
            "[part %d] Done in %.1fs",
            part_index,
            elapsed,
            extra={"part": part_index, "elapsed_s": elapsed, "status": "ok", "bytes": size},
        )
        results[part_index] = {"status": "ok", "elapsed": elapsed, "bytes": size}
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
                extra={"part": part_index, "elapsed_s": elapsed, "status": "error"},
            )
            results[part_index] = {
                "status": "error",
                "elapsed": elapsed,
                "bytes": 0,
                "error": str(exc),
            }
    except OSError as exc:
        # Local read failure (missing file, permission denied, ...) or
        # an S3 network error while streaming the part into the pipe.
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
    conn: ConnInfo,
    rc: str,
    total_bytes: int,
    stop_event: threading.Event,
) -> None:
    """Poll pg_stat_progress_copy and print aggregate throughput.

    *rc* is a regclass literal (e.g. ``'"public"."mytable"'``) used to
    filter by OID so concurrent COPY operations on other tables —
    even identically named ones in different schemas — don't pollute
    the numbers.  Computes delta-based throughput between intervals
    (not cumulative average).

    Holds **one** psycopg connection for the entire monitor lifetime
    and reuses it across every poll, instead of going through
    :func:`psql_query` (which opens, authenticates, and closes a fresh
    connection per call).  At ``PROGRESS_INTERVAL = 3s`` over a
    multi-hour restore that's ~1200 unnecessary connect/auth round-
    trips on a remote PG — invisible on a local socket but measurable
    against managed services (RDS, Aurora, Cloud SQL) and brief but real
    contention against ``max_connections`` on a 128-way restore.

    The connection is tracked via :func:`tracked_connection` so a
    SIGINT-driven ``cancel_workers()`` can abort whatever poll is in
    flight.  Per-poll errors (broken connection, server side issue)
    fall back to a zero reading and try again on the next tick — the
    monitor is observability, not control flow, so it must never
    abort the data plane.
    """
    prev_total = 0
    prev_time = time.time()

    query = (
        "SELECT COALESCE(SUM(p.bytes_processed), 0) "
        "FROM pg_stat_progress_copy p "
        f"WHERE p.relid = {rc}::regclass;"
    )

    # autocommit=True so each poll is its own micro-transaction — no
    # idle-in-transaction state to worry about over a multi-hour run.
    #
    # The ``try`` wraps the whole ``with`` statement, not the
    # ``tracked_connection`` call itself: tracked_connection is a
    # ``@contextmanager`` whose body (the actual ``psycopg.connect``)
    # doesn't run until ``__enter__``, so a connect failure surfaces
    # from the ``with``, never from the bare call.  Catching it here
    # keeps a connect error from killing the monitor thread with a
    # traceback while leaving the data plane to finish.
    try:
        with tracked_connection(conn.conninfo(), autocommit=True) as pg:
            _poll_progress(pg, query, total_bytes, stop_event, prev_total, prev_time)
    except psycopg.Error as exc:
        LOG.warning("progress_monitor: polling connection failed: %s", exc)


def _poll_progress(
    pg: psycopg.Connection,
    query: str,
    total_bytes: int,
    stop_event: threading.Event,
    prev_total: int,
    prev_time: float,
) -> None:
    """Inner poll loop for :func:`progress_monitor`.

    Split out so the outer ``try/except psycopg.Error`` covers
    connect failures only — query-level failures inside the loop are
    handled here and never reach the outer guard.
    """
    while not stop_event.is_set():
        stop_event.wait(PROGRESS_INTERVAL)
        if stop_event.is_set():
            break
        now = time.time()
        dt = now - prev_time
        try:
            with pg.cursor() as cur:
                cur.execute(query)
                row = cur.fetchone()
            bytes_loaded = int(row[0]) if row and row[0] is not None else 0
        except (psycopg.Error, ValueError):
            # Connection or query glitch — skip this tick and try
            # again next interval.  The persistent connection is
            # left in place so a transient blip doesn't cost us
            # the rest of the restore's progress reporting.  If
            # the connection is permanently broken pg's next
            # cursor() will raise too, and we'll keep skipping.
            bytes_loaded = 0

        # pg_stat_progress_copy only reports IN-PROGRESS COPYs, so
        # the aggregated sum *drops* every time a worker finishes
        # and its row disappears from the view.  Clamp negative
        # throughput to 0 so the user doesn't see nonsense lines
        # like "-1834.5 MB/s" on the cycle where a bunch of parts
        # complete simultaneously.
        raw_throughput = (bytes_loaded - prev_total) / dt if dt > 0 else 0
        throughput = max(raw_throughput, 0.0)
        prev_total = bytes_loaded
        prev_time = now
        pct = (bytes_loaded / total_bytes * 100) if total_bytes > 0 else 0
        LOG.info(
            "  Progress: %.2f / %.2f GB (%.1f%%), %.1f MB/s",
            bytes_loaded / 1e9,
            total_bytes / 1e9,
            pct,
            throughput / 1e6,
            extra={"bytes": bytes_loaded, "throughput_mbps": round(throughput / 1e6, 1)},
        )


# ---------------------------------------------------------------------------
# Per-table restore phases
# ---------------------------------------------------------------------------


def restore_table_data(
    conn: ConnInfo,
    schema: str,
    table: str,
    input_dir: str,
    unlogged: bool = False,
    cascade: bool = False,
    max_retries: int = 0,
) -> bool:
    """Phase 1: DROP + CREATE table and parallel COPY FROM data.

    Validates DDL files, drops the old table, creates a bare table from
    pre-data DDL, then loads all part files via parallel workers.

    No indexes or constraints are applied here — that happens in
    ``restore_table_post_data()`` (Phase 2) so that in multi-table
    restores all data is present before FK constraints are validated.

    *cascade* controls whether DROP TABLE uses CASCADE.  In database-level
    mode this is True because FK constraints from other tables may
    reference this one.  In single-table mode this is False so the DROP
    fails loudly if dependents exist, avoiding silent data loss.

    If *max_retries* > 0, failed parts are re-attempted up to that many
    times.  Each worker's ``COPY FROM STDIN`` runs inside the ``psql``
    subprocess's own implicit transaction that commits only on success;
    a failed worker rolls back and inserts zero rows, so retrying is
    safe without any deduplication or upsert logic.

    Returns True on success, False if any part failed.
    """
    qtable = qualified_name(schema, table)
    rc = regclass_literal(schema, table)
    display_table = f"{schema}.{table}"

    parts = discover_parts(input_dir)

    # --- Validate DDL files ------------------------------------------------
    pre_data_blob = make_blob(storage_join(input_dir, PRE_DATA_FILENAME))
    post_data_blob = make_blob(storage_join(input_dir, POST_DATA_FILENAME))
    if not pre_data_blob.exists():
        LOG.error(
            "ERROR: %s not found in %s (produced by parallel_dump.py)",
            PRE_DATA_FILENAME,
            input_dir,
        )
        return False
    if not post_data_blob.exists():
        LOG.error(
            "ERROR: %s not found in %s (produced by parallel_dump.py)",
            POST_DATA_FILENAME,
            input_dir,
        )
        return False

    # --- Prepare table for bulk load ----------------------------------------
    LOG.info("\nDropping %s if it exists...", display_table)
    drop_sql = f"DROP TABLE IF EXISTS {qtable}"
    if cascade:
        drop_sql += " CASCADE"
    psql_exec(conn, f"{drop_sql};")

    LOG.info("Creating bare table %s from %s...", display_table, PRE_DATA_FILENAME)
    apply_ddl(conn, pre_data_blob)

    # --- Empty table (0 part files) ----------------------------------------
    if not parts:
        LOG.info("\nNo part files found — %s is empty.", display_table)
        return True

    # Stat each blob exactly once — for S3Blob, .size() is a HeadObject
    # round-trip, so calling it both here and inside the per-part log
    # loop below would double the HEAD calls per table (N+1 → 2N) for
    # purely cosmetic logging.
    part_sizes = [blob.size() for blob in parts]
    total_bytes = sum(part_sizes)
    LOG.info(
        "\nFound %d part files in %s (%.2f GB total)",
        len(parts),
        input_dir,
        total_bytes / 1e9,
        extra={"num_parts": len(parts), "bytes": total_bytes},
    )
    for i, (blob, size_bytes) in enumerate(zip(parts, part_sizes, strict=False), 1):
        # rsplit("/", 1) works for both local paths and s3:// URIs;
        # os.path.basename would silently misbehave on s3:// under Windows.
        LOG.info(
            "  part %d: %s (%.1f MB)",
            i,
            blob.uri.rsplit("/", 1)[-1],
            size_bytes / 1e6,
        )

    if unlogged:
        LOG.info("Setting %s to UNLOGGED (skipping WAL for bulk load)...", display_table)
        psql_exec(conn, f"ALTER TABLE {qtable} SET UNLOGGED;")

    # --- Launch parallel COPY FROM workers ---------------------------------
    results: dict[int, dict] = {}
    threads: list[threading.Thread] = []

    for i, (part_blob, part_size) in enumerate(zip(parts, part_sizes, strict=False), start=1):
        t = threading.Thread(
            target=copy_part,
            args=(conn, qtable, part_blob, i, results, part_size),
        )
        threads.append(t)

    stop_progress = threading.Event()
    monitor = threading.Thread(
        target=progress_monitor,
        args=(conn, rc, total_bytes, stop_progress),
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

    # --- Summary -----------------------------------------------------------
    loaded_bytes = sum(r.get("bytes", 0) for r in results.values())

    LOG.info("\n%s", "=" * 60)
    LOG.info(
        "Completed %d parts in %.1fs total",
        len(parts),
        overall_elapsed,
        extra={"num_parts": len(parts), "elapsed_s": overall_elapsed, "bytes": loaded_bytes},
    )
    if overall_elapsed > 0:
        LOG.info(
            "Total loaded: %.2f GB, avg throughput: %.1f MB/s",
            loaded_bytes / 1e9,
            loaded_bytes / overall_elapsed / 1e6,
            extra={
                "bytes": loaded_bytes,
                "throughput_mbps": round(loaded_bytes / overall_elapsed / 1e6, 1),
            },
        )
    LOG.info("")
    for i in sorted(results):
        r = results[i]
        LOG.info("  part %d: %9s  (%.1fs)", i, r["status"].upper(), r["elapsed"])

    # --- Retry failed parts --------------------------------------------------
    for attempt in range(1, max_retries + 1):
        retry_indices = [
            i for i in range(1, len(parts) + 1) if i not in results or results[i]["status"] != "ok"
        ]
        if not retry_indices or is_cancelled():
            break

        LOG.info(
            "\n--- Retry %d/%d: re-running %d failed parts ---",
            attempt,
            max_retries,
            len(retry_indices),
        )
        retry_threads: list[threading.Thread] = []
        for i in retry_indices:
            t = threading.Thread(
                target=copy_part,
                args=(conn, qtable, parts[i - 1], i, results, part_sizes[i - 1]),
            )
            retry_threads.append(t)
        for t in retry_threads:
            t.start()
        for t in retry_threads:
            t.join()

        for i in sorted(retry_indices):
            r = results.get(i, {"status": "missing", "elapsed": 0, "bytes": 0})
            LOG.info("  part %d: %9s  (%.1fs)", i, r["status"].upper(), r["elapsed"])

    # --- Final status ---------------------------------------------------------
    missing = [i for i in range(1, len(parts) + 1) if i not in results]
    failed = [k for k, v in sorted(results.items()) if v["status"] != "ok"]
    load_ok = not missing and not failed

    if missing:
        LOG.error("\nMISSING parts (worker crashed): %s", missing)
    if failed:
        LOG.error("\nFAILED parts: %s", failed)
    if load_ok:
        LOG.info("\nAll parts loaded successfully.")

    # --- Restore WAL logging -----------------------------------------------
    if unlogged:
        # SET LOGGED must run even if workers failed, otherwise the table
        # stays unlogged and data would be lost on crash.
        LOG.info("\nSetting %s back to LOGGED...", display_table)
        logged_start = time.time()
        psql_exec(conn, f"ALTER TABLE {qtable} SET LOGGED;")
        LOG.info("SET LOGGED completed in %.1fs", time.time() - logged_start)

    return load_ok


def restore_table_post_data(
    conn: ConnInfo,
    input_dir: str,
    index_jobs: int = 1,
    skip_fk: bool = False,
    only_fk: bool = False,
) -> None:
    """Phase 2: Apply post-data DDL (indexes, constraints) via pg_restore.

    Called after *all* tables have their data loaded (Phase 1) so that
    foreign-key constraints referencing other tables can be validated.

    In database-level mode, this is called in two sub-phases:
      *skip_fk=True*  — PKs, indexes, unique constraints (Phase 2a)
      *only_fk=True*  — FK constraints only (Phase 2b, after all PKs exist)

    In single-table mode, both are False (apply everything at once).

    The post-data blob is localized once (downloaded if it lives in S3)
    and the same temp path is reused for both ``pg_restore -l`` (TOC
    listing for FK filtering) and the final ``pg_restore`` — otherwise
    we'd pay the S3 download cost twice.
    """
    post_data_blob = make_blob(storage_join(input_dir, POST_DATA_FILENAME))

    with localize(post_data_blob) as local_post_data_path:
        toc_file: str | None = None
        try:
            if skip_fk or only_fk:
                toc_file = _filter_post_data_toc(local_post_data_path, only_fk=only_fk)
                if toc_file is None:
                    return

            label = "FK constraints" if only_fk else "indexes and constraints"
            jobs_msg = f" ({index_jobs} parallel jobs)" if index_jobs > 1 else ""
            LOG.info("\nApplying %s from %s%s...", label, POST_DATA_FILENAME, jobs_msg)
            post_start = time.time()
            _run_pg_restore(
                conn,
                local_post_data_path,
                jobs=index_jobs,
                toc_file=toc_file,
                label=post_data_blob.uri,
            )
            LOG.info("Post-data DDL completed in %.1fs", time.time() - post_start)
        finally:
            if toc_file:
                os.unlink(toc_file)


def restore_sequences(conn: ConnInfo, input_dir: str) -> None:
    """Apply saved sequence values from sequences.sql if present.

    Binary COPY doesn't call ``nextval()``, so after restoring table
    data the sequences must be explicitly reset to their dump-time
    positions to avoid duplicate-key errors on subsequent INSERTs.

    No-op when the dump contains no sequences (e.g. tables without
    SERIAL or IDENTITY columns).
    """
    seq_blob = make_blob(storage_join(input_dir, SEQUENCES_FILENAME))
    if not seq_blob.exists():
        return
    LOG.info("Restoring sequence values from %s...", SEQUENCES_FILENAME)
    apply_ddl(conn, seq_blob)


def _restore_global_dump(conn: ConnInfo, dump: Blob, label: str) -> None:
    """Restore a global pg_dump custom-format blob, tolerating "already exists".

    Used for both global pre-data (extensions, types) and global post-data
    (event triggers, standalone rules).  ``pg_restore`` handles dependency
    ordering; "already exists" warnings are expected and suppressed.

    Localizes the blob for the duration of the call so the same code
    path works for S3 and local inputs.
    """
    # blob.uri may be a filesystem path or ``s3://...``; either way the
    # last slash-delimited segment is the file-ish name the user sees.
    display_name = dump.uri.rsplit("/", 1)[-1]
    LOG.info("\nApplying global %s from %s...", label, display_name)
    with localize(dump) as local_path:
        cmd = [
            *conn.pg_restore_args(),
            "--no-owner",
            "--no-privileges",
            local_path,
        ]
        result = run_cancellable(cmd, env=conn.env())
    if result.returncode != 0 and result.stderr:
        unexpected_groups = _classify_pg_restore_stderr(result.stderr)
        if unexpected_groups:
            LOG.error("ERROR: pg_restore global %s failed with unexpected errors:", label)
            for group in unexpected_groups:
                for line in group:
                    LOG.error("    %s", line)
            sys.exit(1)
    LOG.info("Global %s applied.", label)


def _classify_pg_restore_stderr(stderr: str) -> list[list[str]]:
    """Return only the *genuinely-unexpected* error groups from pg_restore stderr.

    pg_restore emits three lines for each ignored error::

        pg_restore: from TOC entry 4; 3079 16580 EXTENSION citext
        pg_restore: error: could not execute query: ERROR:  extension "citext" already exists
        Command was: CREATE EXTENSION IF NOT EXISTS citext WITH SCHEMA public;

    and finishes with a standalone summary::

        pg_restore: warning: errors ignored on restore: 1

    Re-applying globals against a database that already has them —
    shared extensions, event triggers, composite types — is the whole
    point of this helper, so "already exists" triplets must be
    suppressed completely (all three lines, plus the summary).  A
    per-line filter catches only the middle line and surfaces the
    *from TOC entry* and *Command was:* lines as "unexpected",
    triggering a spurious sys.exit(1) and breaking the idempotent-
    restore contract.

    Group the output by the ``from TOC entry`` marker — every genuine
    error starts with one — and classify each group as a whole: if
    any line in the group contains ``already exists`` the entire
    triplet is dropped.  Anything else (permission denied, syntax
    error, missing role, ...) returns as one list of lines per
    genuine failure, preserving the full TOC context for the
    operator to diagnose.

    Lines that appear before the first ``from TOC entry`` marker are
    also grouped together (covers pre-TOC errors like missing input
    files) and classified the same way.
    """
    groups: list[list[str]] = []
    current: list[str] = []
    for line in stderr.splitlines():
        if not line.strip():
            continue
        # The summary "errors ignored on restore: N" is always paired
        # with the individual error lines and carries no extra signal.
        # Treat it as its own group boundary + noise.
        if line.startswith("pg_restore: warning: errors ignored on restore:"):
            if current:
                groups.append(current)
                current = []
            continue
        if line.startswith("pg_restore: from TOC entry"):
            if current:
                groups.append(current)
            current = [line]
        else:
            current.append(line)
    if current:
        groups.append(current)

    return [g for g in groups if not any("already exists" in line for line in g)]


def restore_global_pre_data(conn: ConnInfo, input_dir: str) -> None:
    """Apply database-wide pre-data objects (extensions, types, enums, domains).

    No-op when the dump was created without global pre-data (e.g. a
    single-table dump or a dump produced before this feature existed).
    """
    dump_blob = make_blob(storage_join(input_dir, GLOBAL_PRE_DATA_FILENAME))
    if dump_blob.exists():
        _restore_global_dump(conn, dump_blob, "pre-data")


def restore_global_post_data(conn: ConnInfo, input_dir: str) -> None:
    """Apply database-wide post-data objects (event triggers, etc.).

    No-op when the dump contains no global post-data file.
    """
    dump_blob = make_blob(storage_join(input_dir, GLOBAL_POST_DATA_FILENAME))
    if dump_blob.exists():
        _restore_global_dump(conn, dump_blob, "post-data")


def analyze_table(conn: ConnInfo, schema: str, table: str) -> None:
    """Phase 3: Run ANALYZE to update table statistics."""
    qtable = qualified_name(schema, table)
    display_table = f"{schema}.{table}"
    LOG.info("\nRunning ANALYZE %s...", display_table)
    analyze_start = time.time()
    psql_exec(conn, f"ANALYZE {qtable};")
    LOG.info("ANALYZE completed in %.1fs", time.time() - analyze_start)


# ---------------------------------------------------------------------------
# Database-level restore phases
# ---------------------------------------------------------------------------

# Type alias: (schema_name, object_name, directory_path)
_WorkItem = tuple[str, str, str]


def _log_phase(phase_id: str, message: str, *args: object) -> None:
    """Print a phase header banner with structured phase ID."""
    LOG.info("\n%s", "=" * 60)
    LOG.info(message, *args, extra={"phase": phase_id})
    LOG.info("%s", "=" * 60)


def _phase_0_pre_data(
    conn: ConnInfo,
    input_dir: str,
    work: list[_WorkItem],
    matview_work: list[_WorkItem],
) -> None:
    """Global pre-data (extensions, types, enums, domains) + schema creation.

    Must run before any per-table DDL because CREATE TABLE may reference
    extension types (e.g. citext, uuid) or custom enums/composites.
    """
    _log_phase("0", "Phase 0: Applying global pre-data and creating schemas...")
    restore_global_pre_data(conn, input_dir)

    schemas = sorted({schema for schema, _, _ in work} | {schema for schema, _, _ in matview_work})
    for schema in schemas:
        psql_exec(conn, f"CREATE SCHEMA IF NOT EXISTS {quote_ident(schema)};")


def _phase_1_load_data(
    conn: ConnInfo,
    work: list[_WorkItem],
    unlogged: bool,
    retries: int,
) -> None:
    """DROP + CREATE + parallel COPY FROM for all tables.

    Exits the process if any table fails or a signal is received.
    """
    _log_phase("1", "Phase 1: Loading data for all tables...")
    failed_tables: list[str] = []
    for schema, table, table_dir in work:
        if is_cancelled():
            break
        ok = restore_table_data(
            conn,
            schema,
            table,
            table_dir,
            unlogged,
            cascade=True,
            max_retries=retries,
        )
        if not ok:
            failed_tables.append(f"{schema}.{table}")

    if is_cancelled() or failed_tables:
        if is_cancelled():
            LOG.error("\nRestore cancelled by signal.")
        if failed_tables:
            LOG.error(
                "\nERROR: Phase 1 failed for %d tables: %s",
                len(failed_tables),
                ", ".join(failed_tables),
            )
        sys.exit(1)


def _phase_2a_indexes(
    conn: ConnInfo,
    work: list[_WorkItem],
    index_jobs: int,
) -> None:
    """PKs, indexes, unique constraints (no FKs).

    FKs are deferred to Phase 2b because they reference PKs on other
    tables that may not exist yet at this point.
    """
    _log_phase("2a", "Phase 2a: Applying indexes and constraints (no FKs)...")
    for schema, table, table_dir in work:
        if is_cancelled():
            sys.exit(1)
        LOG.info("\n--- %s.%s ---", schema, table)
        restore_table_post_data(conn, table_dir, index_jobs, skip_fk=True)


def _phase_2b_foreign_keys(
    conn: ConnInfo,
    work: list[_WorkItem],
    index_jobs: int,
) -> None:
    """FK constraints (all referenced PKs now exist).

    Forwards *index_jobs* to ``pg_restore -j`` so a table with many FK
    constraints validates them in parallel — symmetric with Phase 2a's
    parallel index builds.  Without this, ``--index-jobs 8`` would
    parallelise indexes but still validate FKs serially, leaving an
    O(num_fks * validate_time) tail on every multi-FK table.
    """
    _log_phase("2b", "Phase 2b: Applying FK constraints...")
    for schema, table, table_dir in work:
        if is_cancelled():
            sys.exit(1)
        LOG.info("\n--- %s.%s ---", schema, table)
        restore_table_post_data(conn, table_dir, index_jobs, only_fk=True)


def _phase_2c_sequences(conn: ConnInfo, work: list[_WorkItem]) -> None:
    """Restore sequence values to dump-time positions."""
    _log_phase("2c", "Phase 2c: Restoring sequence values...")
    for _schema, _table, table_dir in work:
        if is_cancelled():
            sys.exit(1)
        restore_sequences(conn, table_dir)


def _phase_2d_global_post_data(conn: ConnInfo, input_dir: str) -> None:
    """Global post-data (event triggers, standalone rules).

    Applied after all per-table DDL so that event triggers don't fire
    during the restore's own CREATE TABLE / CREATE INDEX operations.
    """
    if is_cancelled():
        sys.exit(1)
    _log_phase("2d", "Phase 2d: Applying global post-data (event triggers, etc.)...")
    restore_global_post_data(conn, input_dir)


def _phase_3_analyze(conn: ConnInfo, work: list[_WorkItem]) -> None:
    """ANALYZE all tables to update planner statistics."""
    _log_phase("3", "Phase 3: Updating statistics for all tables...")
    for schema, table, _table_dir in work:
        if is_cancelled():
            sys.exit(1)
        analyze_table(conn, schema, table)


def _phase_4_matviews(
    conn: ConnInfo,
    matview_work: list[_WorkItem],
    index_jobs: int,
) -> None:
    """Create, refresh, index, and analyze materialized views.

    Mat-views are restored after all base tables have data and
    constraints, because the mat-view query references those tables.
    Can't COPY FROM into a mat-view — must use REFRESH to populate.

    Known limitation — matview-to-matview dependencies:
        ``matview_work`` is iterated in the order produced by
        ``discover_matviews``, which is alphabetical per schema.  When
        a matview's defining ``SELECT`` references *another* matview
        (chained matviews) and the dependent name sorts before the
        parent name, the CREATE pass below tries to build the dependent
        before its parent exists and the run aborts loudly with a
        ``relation does not exist`` error from psql/ON_ERROR_STOP.

        Resolving this would require capturing the matview dependency
        graph from ``pg_depend`` at dump time and topologically sorting
        at restore time — a measurable refactor for a pattern that is
        rare in practice and that fails loudly rather than silently.
        Operators relying on chained matviews should restore them by
        hand (or via ``pg_restore`` of a separate ``pg_dump`` archive)
        until this is wired up; everyone else is unaffected.
    """
    if not matview_work:
        return
    _log_phase("4", "Phase 4: Restoring %d materialized views...", len(matview_work))

    for schema, matview, mv_dir in matview_work:
        if is_cancelled():
            sys.exit(1)
        qname = qualified_name(schema, matview)
        display = f"{schema}.{matview}"
        LOG.info("\n--- Creating %s ---", display)
        psql_exec(conn, f"DROP MATERIALIZED VIEW IF EXISTS {qname} CASCADE;")
        apply_ddl(conn, make_blob(storage_join(mv_dir, PRE_DATA_FILENAME)))

    for schema, matview, _mv_dir in matview_work:
        if is_cancelled():
            sys.exit(1)
        qname = qualified_name(schema, matview)
        display = f"{schema}.{matview}"
        LOG.info("\nRefreshing %s...", display)
        refresh_start = time.time()
        psql_exec(conn, f"REFRESH MATERIALIZED VIEW {qname};")
        LOG.info("  %s refreshed in %.1fs", display, time.time() - refresh_start)

    for schema, matview, mv_dir in matview_work:
        if is_cancelled():
            sys.exit(1)
        LOG.info("\n--- %s.%s indexes ---", schema, matview)
        restore_table_post_data(conn, mv_dir, index_jobs)

    for schema, matview, _mv_dir in matview_work:
        if is_cancelled():
            sys.exit(1)
        analyze_table(conn, schema, matview)


def _restore_database(conn: ConnInfo, args: argparse.Namespace) -> None:
    """Orchestrate a full database-level restore through all phases."""
    all_dirs = discover_dump_dirs(args.input_dir)
    if not all_dirs:
        if is_file(storage_join(args.input_dir, PRE_DATA_FILENAME)):
            sys.exit("ERROR: --input-dir contains a single-table dump.  Please specify --table.")
        sys.exit(
            f"ERROR: No <schema>/<table>/ layout found in {args.input_dir}.  "
            "Is this a database-level dump produced by parallel_dump.py?"
        )

    work: list[_WorkItem] = []
    matview_work: list[_WorkItem] = []
    for schema, name, dir_path in all_dirs:
        if is_file(storage_join(dir_path, MATVIEW_MARKER_FILENAME)):
            matview_work.append((schema, name, dir_path))
        else:
            work.append((schema, name, dir_path))

    mv_msg = f", {len(matview_work)} materialized views" if matview_work else ""
    LOG.info(
        "\nDatabase-level restore: %d tables%s",
        len(work),
        mv_msg,
        extra={"num_tables": len(work)},
    )
    for schema, table, table_dir in work:
        LOG.info("  %s.%s <- %s", schema, table, table_dir)
    for schema, matview, mv_dir in matview_work:
        LOG.info("  %s.%s <- %s (matview)", schema, matview, mv_dir)

    _phase_0_pre_data(conn, args.input_dir, work, matview_work)
    _phase_1_load_data(conn, work, args.unlogged, args.retries)
    _phase_2a_indexes(conn, work, args.index_jobs)
    _phase_2b_foreign_keys(conn, work, args.index_jobs)
    _phase_2c_sequences(conn, work)
    _phase_2d_global_post_data(conn, args.input_dir)
    _phase_3_analyze(conn, work)
    _phase_4_matviews(conn, matview_work, args.index_jobs)

    LOG.info("\n%s", "=" * 60)
    mv_msg = f", {len(matview_work)} materialized views" if matview_work else ""
    LOG.info(
        "Database-level restore complete: %d tables%s restored successfully.",
        len(work),
        mv_msg,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Parallel COPY-FROM restore of part files into a PostgreSQL table",
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
        help="Optional. Target table (single-table mode).  "
        "Omit to restore all tables from the directory layout.",
    )
    parser.add_argument(
        "--input-dir", default=None, help="Required. Directory containing dump output"
    )
    parser.add_argument(
        "--index-jobs",
        type=int,
        default=None,
        help="Optional. Number of parallel pg_restore jobs for index/constraint builds (default: 1)",
    )
    parser.add_argument(
        "--unlogged",
        action="store_const",
        const=True,
        default=None,
        help="Optional. Set tables to UNLOGGED during bulk load to skip WAL writes, "
        "then SET LOGGED after.  Substantially faster, but the table is "
        "truncated if the server crashes mid-load, so the restore must be "
        "restarted from the beginning.",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=None,
        help="Optional. Max retries for failed COPY workers (default: 0, no retry)",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    apply_config_defaults(args, config, "restore")
    apply_logging_config(args, config)
    validate_connection_args(args)

    if args.input_dir is None:
        LOG.error("ERROR: --input-dir is required (or set restore.input_dir in backup.toml).")
        sys.exit(1)
    if args.index_jobs is None:
        args.index_jobs = 1
    if args.unlogged is None:
        args.unlogged = False
    if args.retries is None:
        args.retries = 0

    if args.index_jobs < 1:
        parser.error("--index-jobs must be at least 1")
    if args.retries < 0:
        parser.error("--retries must be >= 0")

    conn = conn_from_args(args)
    install_signal_handlers()

    # CalledProcessError from psql / pg_restore wrappers already has its
    # stderr logged at ERROR by check_subprocess_result / run_cancellable
    # — letting the exception bubble to Python's default handler would
    # tack a traceback onto the end, burying the useful error under
    # stack-frame noise.  Catch at the top level and exit(1) cleanly.
    #
    # ``psycopg.Error`` gets the same treatment: every ``psql_exec`` /
    # ``psql_query`` / ``tracked_connection`` site can raise it (DROP
    # TABLE on a dropped connection, REFRESH MATERIALIZED VIEW on a
    # missing dep, ANALYZE after a permissions change, etc.).  Without
    # this catch, a server-side failure in any of those control-plane
    # paths surfaces as a Python traceback rather than a clean error
    # line — inconsistent with the subprocess path which is already
    # handled cleanly.  ``str(exc)`` already carries the SQLSTATE and
    # diagnostic detail, so a single ERROR log is sufficient.
    try:
        if args.table:
            schema = args.schema or "public"
            ok = restore_table_data(
                conn,
                schema,
                args.table,
                args.input_dir,
                args.unlogged,
                max_retries=args.retries,
            )
            if not ok:
                sys.exit(1)
            restore_table_post_data(conn, args.input_dir, args.index_jobs)
            restore_sequences(conn, args.input_dir)
            analyze_table(conn, schema, args.table)
        else:
            _restore_database(conn, args)
    except subprocess.CalledProcessError:
        sys.exit(1)
    except psycopg.Error as exc:
        LOG.error("ERROR: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
