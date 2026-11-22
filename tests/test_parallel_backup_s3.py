"""End-to-end dump + restore against an S3-compatible endpoint (moto).

These tests are the real validation of the ``s3://...`` refactor: they
prove that every code path exercised by the local-filesystem integration
tests in ``test_parallel_backup.py`` also works when the output/input is
an S3 bucket.  Coverage includes:

* **Single-table round-trip** — smallest possible full path through the
  pipeline: pg_dump (DDL) -> multipart upload, COPY workers -> multipart
  upload, pg_restore + psql -> localize -> replay.
* **Database-level round-trip** — multiple schemas, sequences, and the
  global pre-data archive.  Exercises directory-style discovery
  (``list_dir``, ``is_dir``, ``is_file``) against S3 prefixes.
* **Sequence round-trip** — ``sequences.sql`` uploaded, then downloaded
  via :func:`~storage.localize` for ``psql -f`` on the restore side.
* **Multipart upload boundary** — a part payload large enough to cross
  the 5 MiB S3 minimum so we test the real multipart-commit path, not
  just the fallback single PutObject.
* **Empty table** — no ``part*.bin`` keys are produced; discovery must
  cope with an empty object listing without hanging or erroring.
* **Idempotent restore** — running the restore twice yields the same
  rows (regression guard for temp-file cleanup and connection re-open
  across ``localize`` context managers).

No real AWS is contacted.  ``moto.server.ThreadedMotoServer`` spins up
an in-process HTTP server that speaks the S3 protocol; the dump and
restore scripts run as subprocesses and talk to it over ``127.0.0.1``
via env-var configuration (``AWS_ENDPOINT_URL_S3`` + dummy credentials).
"""

import os
import subprocess
import sys
import uuid

import boto3
import pytest
from conftest import SCHEMA, TABLE, TEST_DB
from pg_test_helpers import PGHOST, PGPASSWORD, PGPORT, PGUSER, psql, psql_exec

# Only spin up moto on demand — importing it eagerly would slow every
# pytest session, including local-only runs.  Using the submodule name
# is deliberate: it catches both "moto missing" and "moto installed but
# without the [server] extra".
pytest.importorskip("moto.server")

from moto.server import ThreadedMotoServer  # noqa: E402

# ---------------------------------------------------------------------------
# Fixtures: moto server, bucket, unique prefix
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def moto_server():
    """Start moto's S3 server once per session and point env vars at it.

    ``ThreadedMotoServer(port=0)`` asks the OS for a free port so two
    concurrent pytest runs won't collide.  We then mutate ``os.environ``
    so subprocesses (the dump / restore scripts) inherit the endpoint
    and dummy credentials — boto3 inside those subprocesses resolves
    them via the normal chain.

    The server is torn down at session end; env vars are restored to
    whatever they were before, so running these tests doesn't leak
    config into other tests.
    """
    server = ThreadedMotoServer(port=0)
    server.start()
    # moto exposes the bound port via the underlying HTTPServer object.
    port = server._server.server_address[1]
    endpoint_url = f"http://127.0.0.1:{port}"

    # Save any prior values so session teardown is clean.  Using dummy
    # credentials (not "real" AWS creds) is deliberate: moto accepts
    # anything, and this makes leakage visible if a real AWS call ever
    # escaped the test harness.
    overrides = {
        "AWS_ENDPOINT_URL_S3": endpoint_url,
        "AWS_ACCESS_KEY_ID": "testing-key-id",
        "AWS_SECRET_ACCESS_KEY": "testing-secret",
        "AWS_SESSION_TOKEN": "testing-session-token",
        "AWS_REGION": "us-east-1",
        # moto's HTTP server is plain HTTP; don't let any stray CA
        # configuration reject it.
        "AWS_S3_VERIFY_SSL": "false",
    }
    previous: dict[str, str | None] = {k: os.environ.get(k) for k in overrides}
    os.environ.update(overrides)

    yield endpoint_url

    for k, v in previous.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v
    server.stop()


@pytest.fixture
def s3_client(moto_server):
    """A boto3 client pointed at moto.  Used by tests to set up / inspect."""
    return boto3.client(
        "s3",
        endpoint_url=moto_server,
        region_name="us-east-1",
        aws_access_key_id="testing-key-id",
        aws_secret_access_key="testing-secret",
    )


@pytest.fixture
def s3_prefix(s3_client):
    """Return a unique ``s3://bucket/prefix`` URI backed by a fresh bucket.

    Each test gets its own bucket (``backup-test-<uuid>``) so tests
    are fully isolated and teardown is a single ``DeleteBucket`` call
    — no risk of cross-test key collisions.  The bucket is emptied and
    deleted on test exit, even if the test itself leaves objects behind.
    """
    bucket = f"backup-test-{uuid.uuid4().hex[:12]}"
    s3_client.create_bucket(Bucket=bucket)
    uri = f"s3://{bucket}"

    yield uri

    # --- Teardown ----------------------------------------------------------
    # Delete every object and every version (versioning is off by default
    # in moto, but being defensive makes the fixture safe to reuse if a
    # future test enables versioning).
    paginator = s3_client.get_paginator("list_objects_v2")
    to_delete: list[dict] = []
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []) or []:
            to_delete.append({"Key": obj["Key"]})
            # boto3's delete_objects caps at 1000 per call.
            if len(to_delete) == 1000:
                s3_client.delete_objects(Bucket=bucket, Delete={"Objects": to_delete})
                to_delete.clear()
    if to_delete:
        s3_client.delete_objects(Bucket=bucket, Delete={"Objects": to_delete})
    s3_client.delete_bucket(Bucket=bucket)


# ---------------------------------------------------------------------------
# Subprocess helpers
#
# pg_test_helpers.run() uses a frozen PG_ENV snapshot captured at import
# time, so env vars set by the moto fixture wouldn't reach the script.
# These helpers pass the live os.environ through instead.
# ---------------------------------------------------------------------------


def _run(cmd: list[str]) -> None:
    subprocess.run(cmd, check=True, env={**os.environ})


def _base_args() -> list[str]:
    """Connection flags shared by dump / restore.  Always targets TEST_DB."""
    return [
        "--host",
        PGHOST,
        "--port",
        PGPORT,
        "--user",
        PGUSER,
        "--password",
        PGPASSWORD,
        "--database",
        TEST_DB,
    ]


def run_s3_dump(
    output_uri: str,
    num_parts: int,
    *,
    schema: str = SCHEMA,
    table: str | None = TABLE,
) -> None:
    """Invoke ``parallel_dump.py`` with an ``s3://`` output URI.

    *table=None* triggers database-level mode (multi-schema discovery).
    """
    cmd = [sys.executable, "-m", "pgfanout.parallel_dump", *_base_args()]
    if table is not None:
        cmd += ["--schema", schema, "--table", table]
    cmd += ["--num-parts", str(num_parts), "--output-dir", output_uri]
    _run(cmd)


def run_s3_restore(
    input_uri: str,
    *,
    schema: str = SCHEMA,
    table: str | None = TABLE,
    index_jobs: int = 1,
) -> None:
    cmd = [sys.executable, "-m", "pgfanout.parallel_restore", *_base_args()]
    if table is not None:
        cmd += ["--schema", schema, "--table", table]
    cmd += ["--input-dir", input_uri]
    if index_jobs > 1:
        cmd += ["--index-jobs", str(index_jobs)]
    _run(cmd)


def get_sorted_rows(table: str) -> str:
    """Match the helper in test_parallel_backup.py for apples-to-apples assertions."""
    return psql(TEST_DB, f"SELECT id, name, val FROM {table} ORDER BY id;")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_s3_single_table_round_trip(s3_client, s3_prefix, expected_rows):
    """Full single-table dump + restore through an S3 bucket.

    The canary test.  If the whole S3 path is broken this fails first
    and fast; other tests add specialized coverage on top.
    """
    dump_uri = f"{s3_prefix}/single"
    run_s3_dump(dump_uri, num_parts=2)

    # Sanity-check the S3-side artefacts: both DDL blobs and at least
    # one part land in the bucket.  Being explicit here catches "silent
    # success" failures where the script exits 0 but uploads nothing.
    # No sequences.sql is expected for this table — the fixture schema
    # has plain ``bigint``/``text`` columns, no SERIAL or IDENTITY.
    bucket = s3_prefix.removeprefix("s3://")
    objects = _list_keys(s3_client, bucket, "single/")
    assert any(k.endswith("pre_data.sql") for k in objects)
    assert any(k.endswith("post_data.dump") for k in objects)
    assert any(k.startswith("single/part") and k.endswith(".bin") for k in objects)

    run_s3_restore(dump_uri)
    assert get_sorted_rows(TABLE) == expected_rows


def test_s3_restore_twice_is_idempotent(s3_prefix, expected_rows):
    """Two consecutive restores produce identical data (no duplicates).

    The restore drops + recreates the table each time, so running it
    twice against the same S3 dump must yield exactly the same rows.
    Regression guard for stale local state between restore invocations
    (temp-file cleanup, connection re-open, etc.).
    """
    dump_uri = f"{s3_prefix}/idempotent"
    run_s3_dump(dump_uri, num_parts=2)

    run_s3_restore(dump_uri)
    first = get_sorted_rows(TABLE)

    run_s3_restore(dump_uri)
    second = get_sorted_rows(TABLE)

    assert first == second == expected_rows


def test_s3_empty_table(s3_client, s3_prefix):
    """An empty table dumps with no ``part*.bin`` keys and restores cleanly.

    Exercises the "zero pages, zero parts" code path against S3 discovery
    (``list_dir`` returning no ``part*.bin`` entries).  Without the S3
    abstraction, the old ``os.listdir`` call would never have been reached
    for an S3 URI — this test pins the new behaviour.
    """
    psql_exec(TEST_DB, "DROP TABLE IF EXISTS empty_s3;")
    psql_exec(
        TEST_DB,
        "CREATE TABLE empty_s3 (id bigint PRIMARY KEY, name text NOT NULL);",
    )
    try:
        dump_uri = f"{s3_prefix}/empty"
        run_s3_dump(dump_uri, num_parts=4, table="empty_s3")

        bucket = s3_prefix.removeprefix("s3://")
        keys = _list_keys(s3_client, bucket, "empty/")
        # DDL is still produced; just no data parts.
        assert any(k.endswith("pre_data.sql") for k in keys)
        assert not any(k.endswith(".bin") for k in keys), (
            f"Expected no part files for empty table, got: {keys}"
        )

        # Insert a sentinel row so we can prove the restore actually
        # re-ran (vs. silently leaving whatever was there).
        psql_exec(TEST_DB, "INSERT INTO empty_s3 VALUES (999, 'sentinel');")

        run_s3_restore(dump_uri, table="empty_s3")

        assert psql(TEST_DB, "SELECT count(*) FROM empty_s3;") == "0"
    finally:
        psql_exec(TEST_DB, "DROP TABLE IF EXISTS empty_s3;")


def test_s3_database_level_round_trip(s3_client, s3_prefix):
    """Database-level dump + restore across multiple schemas via S3.

    Verifies ``discover_dump_dirs`` on an S3 prefix: two levels of
    "directories" (schema, then table) are found by ``list_objects_v2``
    with ``Delimiter='/'``.  Also confirms the global pre-data archive
    round-trips and the per-schema ``CREATE SCHEMA IF NOT EXISTS`` runs.
    """
    # Test data in two schemas to stress multi-schema discovery.
    psql_exec(TEST_DB, "CREATE SCHEMA IF NOT EXISTS s3_extra;")
    psql_exec(
        TEST_DB,
        """
        DROP TABLE IF EXISTS s3_extra.items CASCADE;
        CREATE TABLE s3_extra.items (
            id    bigint PRIMARY KEY,
            label text NOT NULL
        );
        INSERT INTO s3_extra.items (id, label)
        SELECT i, 'item_' || i FROM generate_series(1, 40) AS s(i);

        DROP TABLE IF EXISTS public.s3_simple CASCADE;
        CREATE TABLE public.s3_simple (
            id bigint PRIMARY KEY,
            v  int NOT NULL
        );
        INSERT INTO public.s3_simple (id, v)
        SELECT i, i * 3 FROM generate_series(1, 40) AS s(i);
        """,
    )
    expected_items = psql(TEST_DB, "SELECT id, label FROM s3_extra.items ORDER BY id;")
    expected_simple = psql(TEST_DB, "SELECT id, v FROM public.s3_simple ORDER BY id;")

    try:
        dump_uri = f"{s3_prefix}/dblevel"
        run_s3_dump(dump_uri, num_parts=2, table=None)

        # Confirm the expected <schema>/<table>/ layout exists in S3.
        bucket = s3_prefix.removeprefix("s3://")
        keys = _list_keys(s3_client, bucket, "dblevel/")
        assert any(k.startswith("dblevel/s3_extra/items/") for k in keys)
        assert any(k.startswith("dblevel/public/s3_simple/") for k in keys)
        assert any(k.endswith("global_pre_data.dump") for k in keys)

        # Tear down the live schema so the restore has to recreate it.
        psql_exec(TEST_DB, "DROP TABLE IF EXISTS s3_extra.items CASCADE;")
        psql_exec(TEST_DB, "DROP SCHEMA IF EXISTS s3_extra;")
        psql_exec(TEST_DB, "DROP TABLE IF EXISTS public.s3_simple CASCADE;")

        run_s3_restore(dump_uri, table=None)

        assert psql(TEST_DB, "SELECT id, label FROM s3_extra.items ORDER BY id;") == expected_items
        assert psql(TEST_DB, "SELECT id, v FROM public.s3_simple ORDER BY id;") == expected_simple
    finally:
        psql_exec(TEST_DB, "DROP TABLE IF EXISTS s3_extra.items CASCADE;")
        psql_exec(TEST_DB, "DROP TABLE IF EXISTS public.s3_simple CASCADE;")
        psql_exec(TEST_DB, "DROP SCHEMA IF EXISTS s3_extra;")


def test_s3_multipart_upload_boundary(s3_client, s3_prefix):
    """A >5 MiB part file forces a real multipart upload (not PutObject).

    Ensures ``_S3MultipartWriter`` actually commits multiple parts end
    to end through moto — the unit tests stub the part-size threshold
    down to single bytes, so a real multi-MiB payload is complementary
    coverage.  We use a wide TEXT column to inflate bytes per row
    cheaply (no extra indexes, no extra tables).
    """
    # 3000 rows × ~3 KiB each ≈ 9 MiB of raw data before COPY encoding,
    # which easily pushes past the 5 MiB multipart threshold once
    # binary COPY format is applied.  Staying deterministic: repeat()
    # rather than random data so the test isn't flaky.
    psql_exec(TEST_DB, "DROP TABLE IF EXISTS s3_big CASCADE;")
    psql_exec(
        TEST_DB,
        """
        CREATE TABLE s3_big (
            id   bigint PRIMARY KEY,
            blob text NOT NULL
        );
        INSERT INTO s3_big (id, blob)
        SELECT i, repeat('x', 3000)
        FROM generate_series(1, 3000) AS s(i);
        """,
    )
    expected_count = psql(TEST_DB, "SELECT count(*) FROM s3_big;")
    expected_sum = psql(TEST_DB, "SELECT sum(length(blob)) FROM s3_big;")

    try:
        dump_uri = f"{s3_prefix}/big"
        # Force a single part so the whole payload lands in one S3 object
        # — guaranteeing the multipart path is exercised (vs. split
        # across many small objects).
        run_s3_dump(dump_uri, num_parts=1, table="s3_big")

        bucket = s3_prefix.removeprefix("s3://")
        part_keys = [
            k
            for k in _list_keys(s3_client, bucket, "big/")
            if k.startswith("big/part") and k.endswith(".bin")
        ]
        assert len(part_keys) == 1
        part_size = s3_client.head_object(Bucket=bucket, Key=part_keys[0])["ContentLength"]
        # 5 MiB is the multipart-minimum the code enforces; we expect
        # to be comfortably above it.
        assert part_size > 5 * 1024 * 1024, (
            f"part too small to exercise multipart upload: {part_size} bytes"
        )

        psql_exec(TEST_DB, "DROP TABLE IF EXISTS s3_big CASCADE;")
        run_s3_restore(dump_uri, table="s3_big")

        assert psql(TEST_DB, "SELECT count(*) FROM s3_big;") == expected_count
        assert psql(TEST_DB, "SELECT sum(length(blob)) FROM s3_big;") == expected_sum
    finally:
        psql_exec(TEST_DB, "DROP TABLE IF EXISTS s3_big CASCADE;")


def test_s3_sequence_values_round_trip(s3_prefix):
    """SERIAL columns' sequence positions are preserved through S3.

    Tests both that sequences.sql is uploaded and that localize()
    downloads it on the restore side before ``psql -f`` replays it.
    """
    psql_exec(TEST_DB, "DROP TABLE IF EXISTS s3_seq CASCADE;")
    psql_exec(
        TEST_DB,
        """
        CREATE TABLE s3_seq (
            id   bigserial PRIMARY KEY,
            name text NOT NULL
        );
        INSERT INTO s3_seq (name)
        SELECT 'seq_' || i FROM generate_series(1, 25) AS s(i);
        """,
    )
    expected_last = psql(TEST_DB, "SELECT last_value FROM s3_seq_id_seq;")

    try:
        dump_uri = f"{s3_prefix}/seqs"
        run_s3_dump(dump_uri, num_parts=2, table="s3_seq")

        psql_exec(TEST_DB, "DROP TABLE IF EXISTS s3_seq CASCADE;")
        run_s3_restore(dump_uri, table="s3_seq")

        assert psql(TEST_DB, "SELECT last_value FROM s3_seq_id_seq;") == expected_last

        # Insert a new row and make sure the next ID comes from a
        # properly-advanced sequence (proves setval happened server-side).
        psql_exec(TEST_DB, "INSERT INTO s3_seq (name) VALUES ('post-restore');")
        new_id = int(psql(TEST_DB, "SELECT id FROM s3_seq WHERE name = 'post-restore';"))
        assert new_id > int(expected_last)
    finally:
        psql_exec(TEST_DB, "DROP TABLE IF EXISTS s3_seq CASCADE;")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _list_keys(s3_client, bucket: str, prefix: str) -> list[str]:
    """Return every object key under *prefix*, paginating transparently."""
    keys: list[str] = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []) or []:
            keys.append(obj["Key"])
    return keys
