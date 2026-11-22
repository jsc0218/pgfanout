"""End-to-end tests for parallel_dump.py and parallel_restore.py.

Single-table tests:
  Creates a test database with sample data (via the ``test_db`` session
  fixture in conftest.py), then for each partition count:
    1. Dumps once via parallel_dump.py (shared via module-scoped fixtures).
    2. Verifies dump output: checks part*.bin files are non-empty.
    3. Restores via parallel_restore.py and verifies row-level correctness.
    4. Runs restore-specific tests (idempotency, --unlogged).
  Edge cases like more-parts-than-pages and empty tables are tested separately.

Database-level (multi-table) tests:
  - Multi-table dump + restore within a single schema, with FK constraints.
  - Multi-schema dump + restore across schemas, with schema re-creation.

Expects a running Postgres server.  Connection is configured via
standard PG environment variables (PGHOST, PGPORT, PGUSER, PGPASSWORD).

Run via pytest:
    pytest tests/ -v

Run via Docker (from repo root):
    python3 tests/run_local.py
"""

import json
import os
import subprocess
import sys

import pytest
from conftest import NUM_ROWS, SCHEMA, TABLE, TEST_DB
from pg_test_helpers import (
    PG_ENV,
    PGHOST,
    PGPASSWORD,
    PGPORT,
    PGUSER,
    psql,
    psql_exec,
    run,
)

from common import (
    GLOBAL_POST_DATA_FILENAME,
    GLOBAL_PRE_DATA_FILENAME,
    MATVIEW_MARKER_FILENAME,
    POST_DATA_FILENAME,
    PRE_DATA_FILENAME,
    SEQUENCES_FILENAME,
)

# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------


def get_sorted_rows(table: str) -> str:
    return psql(TEST_DB, f"SELECT id, name, val FROM {table} ORDER BY id;")


def get_row_count(table: str) -> int:
    return int(psql(TEST_DB, f"SELECT count(*) FROM {table};"))


# ---------------------------------------------------------------------------
# Script invocation helpers
# ---------------------------------------------------------------------------


def _script(name: str) -> str:
    return os.path.join(os.path.dirname(__file__), "..", "src", name)


def run_parallel_dump(output_dir: str, num_parts: int, table: str = TABLE) -> None:
    run(
        [
            sys.executable,
            _script("parallel_dump.py"),
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
            "--schema",
            SCHEMA,
            "--table",
            table,
            "--num-parts",
            str(num_parts),
            "--output-dir",
            output_dir,
        ]
    )


def run_parallel_restore(
    input_dir: str,
    unlogged: bool = False,
    table: str = TABLE,
    index_jobs: int = 1,
) -> None:
    cmd = [
        sys.executable,
        _script("parallel_restore.py"),
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
        "--schema",
        SCHEMA,
        "--table",
        table,
        "--input-dir",
        input_dir,
    ]
    if index_jobs > 1:
        cmd.extend(["--index-jobs", str(index_jobs)])
    if unlogged:
        cmd.append("--unlogged")
    run(cmd)


def run_db_dump(output_dir: str, num_parts: int) -> None:
    run(
        [
            sys.executable,
            _script("parallel_dump.py"),
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
            "--num-parts",
            str(num_parts),
            "--output-dir",
            output_dir,
        ]
    )


def run_db_restore(input_dir: str, index_jobs: int = 1, unlogged: bool = False) -> None:
    cmd = [
        sys.executable,
        _script("parallel_restore.py"),
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
        "--input-dir",
        input_dir,
    ]
    if index_jobs > 1:
        cmd.extend(["--index-jobs", str(index_jobs)])
    if unlogged:
        cmd.append("--unlogged")
    run(cmd)


# ---------------------------------------------------------------------------
# Module-scoped fixtures: shared dump directories
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def dump_4(test_db, tmp_path_factory):
    d = str(tmp_path_factory.mktemp("dump4"))
    run_parallel_dump(d, 4)
    return d


@pytest.fixture(scope="module")
def dump_2(test_db, tmp_path_factory):
    d = str(tmp_path_factory.mktemp("dump2"))
    run_parallel_dump(d, 2)
    return d


@pytest.fixture(scope="module")
def dump_1(test_db, tmp_path_factory):
    d = str(tmp_path_factory.mktemp("dump1"))
    run_parallel_dump(d, 1)
    return d


# ---------------------------------------------------------------------------
# Single-table dump tests
# ---------------------------------------------------------------------------


def _assert_dump_parts_nonempty(dump_dir: str) -> None:
    part_files = [f for f in os.listdir(dump_dir) if f.startswith("part") and f.endswith(".bin")]
    assert part_files, f"No part*.bin files in {dump_dir}"
    for fname in part_files:
        assert os.path.getsize(os.path.join(dump_dir, fname)) > 0, f"{fname} is empty"


def test_ddl_files_present(dump_4):
    """Dump produces non-empty pre_data.sql and post_data.dump files.

    These DDL files are essential for recreating the bare table structure
    (pre-data) and indexes/constraints (post-data) during restore.  If
    either is missing or empty, the restore cannot rebuild the schema.
    """
    for name in (PRE_DATA_FILENAME, POST_DATA_FILENAME):
        path = os.path.join(dump_4, name)
        assert os.path.isfile(path), f"{name} not found"
        assert os.path.getsize(path) > 0, f"{name} is empty"


def test_dump_output_4_parts(dump_4):
    """A 4-part dump creates non-empty binary part files.

    Verifies the TID-range partitioning logic produces data for every
    requested partition when the table has enough pages.
    """
    _assert_dump_parts_nonempty(dump_4)


def test_dump_output_2_parts(dump_2):
    """A 2-part dump creates non-empty binary part files.

    Same check as the 4-part case but with fewer partitions, exercising
    a different page-range split.
    """
    _assert_dump_parts_nonempty(dump_2)


def test_dump_output_1_part(dump_1):
    """A 1-part dump creates a single non-empty binary part file.

    The degenerate single-partition case must still produce a valid COPY
    binary file covering the entire table.
    """
    _assert_dump_parts_nonempty(dump_1)


# ---------------------------------------------------------------------------
# Single-table restore tests
# ---------------------------------------------------------------------------


def test_restore_4_parts(dump_4, expected_rows):
    """Restore from a 4-part dump reproduces the original data exactly.

    The restore must reassemble data from multiple binary COPY files,
    recreate the table from DDL, and load all rows without loss or
    duplication.
    """
    run_parallel_restore(dump_4)
    assert get_sorted_rows(TABLE) == expected_rows


def test_restore_2_parts(dump_2, expected_rows):
    """Restore from a 2-part dump reproduces the original data exactly.

    Exercises the same reassembly path with a different partition count
    to verify the restore is not sensitive to the number of parts.
    """
    run_parallel_restore(dump_2)
    assert get_sorted_rows(TABLE) == expected_rows


def test_restore_1_part(dump_1, expected_rows):
    """Restore from a 1-part dump reproduces the original data exactly.

    Ensures the single-file degenerate case loads correctly.
    """
    run_parallel_restore(dump_1)
    assert get_sorted_rows(TABLE) == expected_rows


def test_restore_twice(dump_2, expected_rows):
    """Running restore twice is idempotent — no duplicated rows.

    The restore drops and recreates the table each time, so a second
    invocation on the same dump must produce the exact same result
    without doubling the data.  This matters for retry / re-run safety.
    """
    run_parallel_restore(dump_2)
    assert get_row_count(TABLE) == NUM_ROWS

    run_parallel_restore(dump_2)
    assert get_row_count(TABLE) == NUM_ROWS
    assert get_sorted_rows(TABLE) == expected_rows


def test_unlogged_restore(dump_2, expected_rows):
    """Single-table --unlogged restore skips WAL during load, then converts back.

    The --unlogged flag sets the table to UNLOGGED before bulk-loading
    (avoiding WAL overhead), then switches it back to LOGGED.  This test
    verifies that the final table is LOGGED (relpersistence='p') and the
    data is intact.  A table left UNLOGGED would lose data on crash.
    """
    run_parallel_restore(dump_2, unlogged=True)

    persistence = psql(
        TEST_DB,
        f"SELECT relpersistence FROM pg_class WHERE oid = '{SCHEMA}.{TABLE}'::regclass;",
    )
    assert persistence == "p", f"Expected relpersistence='p', got '{persistence}'"
    assert get_sorted_rows(TABLE) == expected_rows


def test_parallel_index_restore(dump_2, expected_rows):
    """Restore with --index-jobs=4 rebuilds indexes in parallel.

    Post-data (indexes, PKs, constraints) is applied via pg_restore with
    multiple parallel jobs.  This test confirms the data is correct AND
    the primary key constraint exists, proving pg_restore's parallel
    index build completed successfully.
    """
    run_parallel_restore(dump_2, index_jobs=4)
    assert get_sorted_rows(TABLE) == expected_rows

    has_pk = int(
        psql(
            TEST_DB,
            f"SELECT COUNT(*) FROM pg_constraint "
            f"WHERE conrelid = '{SCHEMA}.{TABLE}'::regclass AND contype = 'p';",
        )
    )
    assert has_pk == 1, "Primary key missing after parallel index restore"


# ---------------------------------------------------------------------------
# Edge-case tests
# ---------------------------------------------------------------------------


def test_more_parts_than_pages(expected_rows, tmp_path):
    """Requesting more partitions than the table has pages still works.

    When num_parts exceeds the physical page count, the dump should
    silently clamp to the actual number of pages and produce fewer part
    files.  This guards against off-by-one errors in the TID-range
    partitioning logic.
    """
    d = str(tmp_path)
    run_parallel_dump(d, NUM_ROWS + 50)
    run_parallel_restore(d)
    assert get_sorted_rows(TABLE) == expected_rows


def test_empty_table(tmp_path):
    """An empty table (zero rows) dumps and restores without errors.

    The dump must produce DDL files but no part*.bin data files.  The
    restore must recreate the table structure (including the PK) even
    though there is no data to load.  An empty table is a common edge
    case in production databases.
    """
    empty_table = "test_empty"
    psql_exec(
        TEST_DB,
        f"""
        CREATE TABLE {empty_table} (
            id   bigint PRIMARY KEY,
            name text NOT NULL
        );
    """,
    )
    try:
        d = str(tmp_path)
        run_parallel_dump(d, 4, table=empty_table)

        for name in (PRE_DATA_FILENAME, POST_DATA_FILENAME):
            assert os.path.isfile(os.path.join(d, name)), f"{name} missing after empty dump"

        part_files = [f for f in os.listdir(d) if f.startswith("part")]
        assert part_files == [], f"Expected 0 part files, got {part_files}"

        run_parallel_restore(d, table=empty_table)

        assert int(psql(TEST_DB, f"SELECT count(*) FROM {empty_table};")) == 0
        has_pk = int(
            psql(
                TEST_DB,
                f"SELECT COUNT(*) FROM pg_constraint "
                f"WHERE conrelid = '{SCHEMA}.{empty_table}'::regclass AND contype = 'p';",
            )
        )
        assert has_pk == 1, "PK missing after empty table restore"
    finally:
        psql_exec(TEST_DB, f"DROP TABLE IF EXISTS {empty_table};")


def test_restore_retry_on_corrupt_part(tmp_path):
    """Restore --retries detects corrupt binary data and retries.

    Overwrites part1.bin with garbage bytes so the COPY import fails.
    Because the underlying data is permanently corrupt, the retry also
    fails.  The test verifies: (1) non-zero exit code, (2) the "Retry
    1/1" message appears in output, and (3) the final "FAILED parts:"
    summary is printed.  This exercises the retry + error-reporting path
    without needing a transient failure.
    """
    d = str(tmp_path)
    run_parallel_dump(d, 2)

    with open(os.path.join(d, "part1.bin"), "wb") as f:
        f.write(b"CORRUPT_DATA_NOT_VALID_BINARY_COPY")

    result = subprocess.run(
        [
            sys.executable,
            _script("parallel_restore.py"),
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
            "--schema",
            SCHEMA,
            "--table",
            TABLE,
            "--input-dir",
            d,
            "--retries",
            "1",
        ],
        env=PG_ENV,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0, "Expected non-zero exit for corrupt part"
    combined = result.stdout + result.stderr
    assert "Retry 1/1" in combined, "Retry message not found in output"
    assert "FAILED parts:" in combined, "'FAILED parts:' not found in output"


# ---------------------------------------------------------------------------
# Sequence tests
# ---------------------------------------------------------------------------


def test_sequence_value_preserved(tmp_path):
    """Bigserial sequence values survive a dump+restore cycle.

    Binary COPY bypasses nextval(), so the sequence position is not
    automatically advanced during restore.  The dump saves the current
    last_value via pg_catalog.setval() in sequences.sql, and the restore
    replays it.  Without this, the first INSERT after restore would
    reuse an already-taken ID and hit a duplicate-key error.
    """
    seq_table = "test_seq_vals"
    psql_exec(TEST_DB, f"DROP TABLE IF EXISTS {seq_table} CASCADE;")
    psql_exec(
        TEST_DB,
        f"""
        CREATE TABLE {seq_table} (
            id bigserial PRIMARY KEY,
            label text NOT NULL
        );
        INSERT INTO {seq_table} (label)
        SELECT 'row_' || i FROM generate_series(1, 100) AS s(i);
    """,
    )

    seq_val_before = psql(TEST_DB, f"SELECT last_value FROM {seq_table}_id_seq;")

    d = str(tmp_path)
    try:
        run_parallel_dump(d, 2, table=seq_table)
        assert os.path.isfile(os.path.join(d, SEQUENCES_FILENAME))

        run_parallel_restore(d, table=seq_table)

        seq_val_after = psql(TEST_DB, f"SELECT last_value FROM {seq_table}_id_seq;")
        assert seq_val_before == seq_val_after

        psql_exec(TEST_DB, f"INSERT INTO {seq_table} (label) VALUES ('new_row');")
        new_id = int(psql(TEST_DB, f"SELECT id FROM {seq_table} WHERE label = 'new_row';"))
        assert new_id > int(seq_val_before)
    finally:
        psql_exec(TEST_DB, f"DROP TABLE IF EXISTS {seq_table} CASCADE;")


def test_config_file_dump_restore(tmp_path):
    """TOML config file can supply all connection parameters.

    Instead of passing --host, --port, --user, --password, --database
    as CLI flags, the user can store them in a TOML file and pass
    --config.  This test writes a config, runs dump+restore through it,
    and verifies the data round-trips correctly.
    """
    psql_exec(
        TEST_DB,
        f"DELETE FROM {TABLE};\n"
        f"INSERT INTO {TABLE} (id, name, val) "
        f"SELECT i, 'row_' || i, i * 10 FROM generate_series(1, {NUM_ROWS}) AS s(i);",
    )

    config_content = (
        "[connection]\n"
        f'host = "{PGHOST}"\n'
        f'port = "{PGPORT}"\n'
        f'user = "{PGUSER}"\n'
        f'password = "{PGPASSWORD}"\n'
        f'database = "{TEST_DB}"\n'
        "\n[dump]\nnum_parts = 2\n"
        "\n[restore]\nindex_jobs = 1\n"
    )
    config_path = str(tmp_path / "backup_test.toml")
    dump_dir = str(tmp_path / "dump")
    os.makedirs(dump_dir, exist_ok=True)

    with open(config_path, "w") as f:
        f.write(config_content)

    run(
        [
            sys.executable,
            _script("parallel_dump.py"),
            "--config",
            config_path,
            "--table",
            TABLE,
            "--output-dir",
            dump_dir,
        ]
    )
    assert os.path.isfile(os.path.join(dump_dir, PRE_DATA_FILENAME))

    run(
        [
            sys.executable,
            _script("parallel_restore.py"),
            "--config",
            config_path,
            "--table",
            TABLE,
            "--input-dir",
            dump_dir,
        ]
    )
    assert get_row_count(TABLE) == NUM_ROWS


# ---------------------------------------------------------------------------
# Database-level (multi-table) tests
# ---------------------------------------------------------------------------


MULTI_TABLES = ["orders", "products", "customers"]
MULTI_ROWS = 50


def _create_multi_table_data() -> dict[str, str]:
    for tbl in MULTI_TABLES:
        psql_exec(TEST_DB, f"DROP TABLE IF EXISTS {tbl} CASCADE;")
    psql_exec(
        TEST_DB,
        f"""
        CREATE TABLE customers (
            id   bigint PRIMARY KEY,
            name text NOT NULL
        );
        INSERT INTO customers (id, name)
        SELECT i, 'cust_' || i FROM generate_series(1, {MULTI_ROWS}) AS s(i);

        CREATE TABLE products (
            id    bigint PRIMARY KEY,
            title text NOT NULL,
            price int  NOT NULL
        );
        INSERT INTO products (id, title, price)
        SELECT i, 'prod_' || i, i * 100 FROM generate_series(1, {MULTI_ROWS}) AS s(i);

        CREATE TABLE orders (
            id          bigint PRIMARY KEY,
            customer_id bigint NOT NULL REFERENCES customers(id),
            product_id  bigint NOT NULL REFERENCES products(id),
            qty         int    NOT NULL
        );
        INSERT INTO orders (id, customer_id, product_id, qty)
        SELECT i, (i % {MULTI_ROWS}) + 1, (i % {MULTI_ROWS}) + 1, i
        FROM generate_series(1, {MULTI_ROWS}) AS s(i);
    """,
    )
    return {
        "customers": psql(TEST_DB, "SELECT id, name FROM customers ORDER BY id;"),
        "products": psql(TEST_DB, "SELECT id, title, price FROM products ORDER BY id;"),
        "orders": psql(TEST_DB, "SELECT id, customer_id, product_id, qty FROM orders ORDER BY id;"),
    }


def _drop_multi_tables() -> None:
    psql_exec(TEST_DB, "DROP TABLE IF EXISTS orders, products, customers CASCADE;")


def test_multi_table_dump_restore(tmp_path):
    """Database-level dump+restore handles multiple tables with FK constraints.

    Creates customers, products, and orders (with two FKs).  After
    dump + drop + restore, verifies: (1) every table's data matches,
    and (2) both FK constraints are recreated.  This exercises the
    global pre/post-data phases and per-table data loading in the
    correct dependency order.
    """
    expected = _create_multi_table_data()
    d = str(tmp_path)
    try:
        run_db_dump(d, num_parts=2)

        for tbl in MULTI_TABLES:
            table_dir = os.path.join(d, SCHEMA, tbl)
            assert os.path.isdir(table_dir), f"Directory {table_dir} missing"
            assert os.path.isfile(os.path.join(table_dir, PRE_DATA_FILENAME))
            assert os.path.isfile(os.path.join(table_dir, POST_DATA_FILENAME))

        _drop_multi_tables()
        run_db_restore(d)

        assert psql(TEST_DB, "SELECT id, name FROM customers ORDER BY id;") == expected["customers"]
        assert (
            psql(TEST_DB, "SELECT id, title, price FROM products ORDER BY id;")
            == expected["products"]
        )
        assert (
            psql(TEST_DB, "SELECT id, customer_id, product_id, qty FROM orders ORDER BY id;")
            == expected["orders"]
        )

        fk_count = int(
            psql(
                TEST_DB,
                f"SELECT COUNT(*) FROM pg_constraint "
                f"WHERE conrelid = '{SCHEMA}.orders'::regclass AND contype = 'f';",
            )
        )
        assert fk_count == 2, f"Expected 2 FK constraints, got {fk_count}"
    finally:
        _drop_multi_tables()


def test_multi_table_parallel_index_restore(tmp_path):
    """Database-level restore with --index-jobs=2 rebuilds indexes in parallel.

    Same multi-table setup as above, but with parallel pg_restore for
    post-data.  Verifies data correctness and FK constraint recreation
    when indexes are built concurrently across tables.
    """
    expected = _create_multi_table_data()
    d = str(tmp_path)
    try:
        run_db_dump(d, num_parts=2)
        _drop_multi_tables()
        run_db_restore(d, index_jobs=2)

        assert psql(TEST_DB, "SELECT id, name FROM customers ORDER BY id;") == expected["customers"]
        assert (
            psql(TEST_DB, "SELECT id, customer_id, product_id, qty FROM orders ORDER BY id;")
            == expected["orders"]
        )

        fk_count = int(
            psql(
                TEST_DB,
                f"SELECT COUNT(*) FROM pg_constraint "
                f"WHERE conrelid = '{SCHEMA}.orders'::regclass AND contype = 'f';",
            )
        )
        assert fk_count == 2
    finally:
        _drop_multi_tables()


def test_multi_schema_dump_restore(tmp_path):
    """Database-level dump+restore spans multiple schemas.

    Creates tables in both ``public`` and a custom ``extra`` schema.
    After dump + drop (including dropping the extra schema itself) +
    restore, verifies that both schemas and their tables are recreated
    with correct data.  This exercises the schema-discovery and
    schema-recreation logic in the dump/restore tools.
    """
    psql_exec(TEST_DB, "CREATE SCHEMA IF NOT EXISTS extra;")
    psql_exec(
        TEST_DB,
        f"""
        DROP TABLE IF EXISTS extra.items;
        CREATE TABLE extra.items (
            id   bigint PRIMARY KEY,
            label text NOT NULL
        );
        INSERT INTO extra.items (id, label)
        SELECT i, 'item_' || i FROM generate_series(1, {MULTI_ROWS}) AS s(i);
    """,
    )
    psql_exec(
        TEST_DB,
        f"""
        DROP TABLE IF EXISTS public.simple;
        CREATE TABLE public.simple (
            id bigint PRIMARY KEY,
            v  int NOT NULL
        );
        INSERT INTO public.simple (id, v)
        SELECT i, i * 2 FROM generate_series(1, {MULTI_ROWS}) AS s(i);
    """,
    )
    expected_items = psql(TEST_DB, "SELECT id, label FROM extra.items ORDER BY id;")
    expected_simple = psql(TEST_DB, "SELECT id, v FROM public.simple ORDER BY id;")

    d = str(tmp_path)
    try:
        run_db_dump(d, num_parts=2)

        for schema, tbl in [("extra", "items"), ("public", "simple")]:
            assert os.path.isdir(os.path.join(d, schema, tbl))

        psql_exec(TEST_DB, "DROP TABLE IF EXISTS extra.items CASCADE;")
        psql_exec(TEST_DB, "DROP SCHEMA IF EXISTS extra;")
        psql_exec(TEST_DB, "DROP TABLE IF EXISTS public.simple CASCADE;")

        run_db_restore(d)

        assert psql(TEST_DB, "SELECT id, label FROM extra.items ORDER BY id;") == expected_items
        assert psql(TEST_DB, "SELECT id, v FROM public.simple ORDER BY id;") == expected_simple
    finally:
        psql_exec(TEST_DB, "DROP TABLE IF EXISTS extra.items CASCADE;")
        psql_exec(TEST_DB, "DROP TABLE IF EXISTS public.simple CASCADE;")
        psql_exec(TEST_DB, "DROP SCHEMA IF EXISTS extra;")


def test_multi_table_sequence_values(tmp_path):
    """Sequence values are preserved per-table in database-level mode.

    Two tables with bigserial PKs (seq_parent and seq_child) are dumped
    together.  Each table's sequences.sql captures its own sequence
    position.  After restore, both sequences resume from the correct
    value, so new INSERTs do not collide with existing IDs.
    """
    psql_exec(TEST_DB, "DROP TABLE IF EXISTS seq_parent CASCADE;")
    psql_exec(TEST_DB, "DROP TABLE IF EXISTS seq_child CASCADE;")
    psql_exec(
        TEST_DB,
        """
        CREATE TABLE seq_parent (
            id bigserial PRIMARY KEY,
            name text NOT NULL
        );
        INSERT INTO seq_parent (name)
        SELECT 'parent_' || i FROM generate_series(1, 30) AS s(i);

        CREATE TABLE seq_child (
            id bigserial PRIMARY KEY,
            parent_id bigint NOT NULL REFERENCES seq_parent(id),
            val int NOT NULL
        );
        INSERT INTO seq_child (parent_id, val)
        SELECT (i % 30) + 1, i * 10 FROM generate_series(1, 60) AS s(i);
    """,
    )

    parent_seq = psql(TEST_DB, "SELECT last_value FROM seq_parent_id_seq;")
    child_seq = psql(TEST_DB, "SELECT last_value FROM seq_child_id_seq;")

    d = str(tmp_path)
    try:
        run_db_dump(d, num_parts=2)
        for tbl in ("seq_parent", "seq_child"):
            assert os.path.isfile(os.path.join(d, "public", tbl, SEQUENCES_FILENAME))

        psql_exec(TEST_DB, "DROP TABLE IF EXISTS seq_child CASCADE;")
        psql_exec(TEST_DB, "DROP TABLE IF EXISTS seq_parent CASCADE;")
        run_db_restore(d)

        assert psql(TEST_DB, "SELECT last_value FROM seq_parent_id_seq;") == parent_seq
        assert psql(TEST_DB, "SELECT last_value FROM seq_child_id_seq;") == child_seq

        psql_exec(TEST_DB, "INSERT INTO seq_parent (name) VALUES ('new_parent');")
        new_id = int(psql(TEST_DB, "SELECT id FROM seq_parent WHERE name = 'new_parent';"))
        assert new_id > int(parent_seq)
    finally:
        psql_exec(TEST_DB, "DROP TABLE IF EXISTS seq_child CASCADE;")
        psql_exec(TEST_DB, "DROP TABLE IF EXISTS seq_parent CASCADE;")


def test_enum_type_dump_restore(tmp_path):
    """Database-level dump+restore preserves custom enum types.

    Enums live in global pre-data (outside any single table).  If the
    global pre-data phase fails to recreate the enum before the table's
    per-table DDL runs, CREATE TABLE would fail because the column type
    does not exist.  This test verifies the enum type and table data
    both survive the round-trip.
    """
    psql_exec(TEST_DB, "DROP TABLE IF EXISTS typed_records CASCADE;")
    psql_exec(TEST_DB, "DROP TYPE IF EXISTS status_type CASCADE;")
    psql_exec(
        TEST_DB,
        """
        CREATE TYPE status_type AS ENUM ('active', 'inactive', 'deleted');
        CREATE TABLE typed_records (
            id bigint PRIMARY KEY,
            name text NOT NULL,
            status status_type NOT NULL DEFAULT 'active'
        );
        INSERT INTO typed_records (id, name, status)
        SELECT i, 'record_' || i,
               CASE WHEN i % 3 = 0 THEN 'deleted'
                    WHEN i % 2 = 0 THEN 'inactive'
                    ELSE 'active'
               END::status_type
        FROM generate_series(1, 30) AS s(i);
    """,
    )
    expected = psql(TEST_DB, "SELECT id, name, status FROM typed_records ORDER BY id;")

    d = str(tmp_path)
    try:
        run_db_dump(d, num_parts=2)
        assert os.path.isfile(os.path.join(d, GLOBAL_PRE_DATA_FILENAME))

        psql_exec(TEST_DB, "DROP TABLE IF EXISTS typed_records CASCADE;")
        psql_exec(TEST_DB, "DROP TYPE IF EXISTS status_type CASCADE;")
        run_db_restore(d)

        type_exists = int(
            psql(
                TEST_DB,
                "SELECT COUNT(*) FROM pg_type t "
                "JOIN pg_namespace n ON t.typnamespace = n.oid "
                "WHERE t.typname = 'status_type' AND n.nspname = 'public';",
            )
        )
        assert type_exists == 1, "status_type enum missing after restore"
        assert psql(TEST_DB, "SELECT id, name, status FROM typed_records ORDER BY id;") == expected
    finally:
        psql_exec(TEST_DB, "DROP TABLE IF EXISTS typed_records CASCADE;")
        psql_exec(TEST_DB, "DROP TYPE IF EXISTS status_type CASCADE;")


def test_extension_dump_restore(tmp_path):
    """Database-level dump+restore preserves extensions (citext).

    Extensions are captured in the global pre-data phase.  A table
    using the citext type requires the extension to be installed before
    the table DDL runs.  Skipped if the citext extension is not
    available on the test server.
    """
    available = int(
        psql(
            TEST_DB,
            "SELECT COUNT(*) FROM pg_available_extensions WHERE name = 'citext';",
        )
    )
    if available == 0:
        pytest.skip("citext extension not available")

    psql_exec(TEST_DB, "DROP TABLE IF EXISTS ci_names CASCADE;")
    psql_exec(TEST_DB, "DROP EXTENSION IF EXISTS citext CASCADE;")
    psql_exec(
        TEST_DB,
        """
        CREATE EXTENSION citext;
        CREATE TABLE ci_names (
            id bigint PRIMARY KEY,
            name citext NOT NULL
        );
        INSERT INTO ci_names (id, name)
        SELECT i, 'Name_' || i FROM generate_series(1, 20) AS s(i);
    """,
    )
    expected = psql(TEST_DB, "SELECT id, name FROM ci_names ORDER BY id;")

    d = str(tmp_path)
    try:
        run_db_dump(d, num_parts=2)
        psql_exec(TEST_DB, "DROP TABLE IF EXISTS ci_names CASCADE;")
        psql_exec(TEST_DB, "DROP EXTENSION IF EXISTS citext CASCADE;")
        run_db_restore(d)

        assert (
            int(psql(TEST_DB, "SELECT COUNT(*) FROM pg_extension WHERE extname = 'citext';")) == 1
        )
        assert psql(TEST_DB, "SELECT id, name FROM ci_names ORDER BY id;") == expected
    finally:
        psql_exec(TEST_DB, "DROP TABLE IF EXISTS ci_names CASCADE;")
        psql_exec(TEST_DB, "DROP EXTENSION IF EXISTS citext CASCADE;")


def test_event_trigger_dump_restore(tmp_path):
    """Database-level dump+restore preserves event triggers and their functions.

    Event triggers and standalone functions are captured in the global
    post-data phase.  This test creates a DDL-logging event trigger,
    dumps, drops everything, restores, and verifies the trigger exists
    in pg_event_trigger and the table data is intact.
    """
    psql_exec(TEST_DB, "DROP EVENT TRIGGER IF EXISTS test_ddl_logger;")
    psql_exec(TEST_DB, "DROP TABLE IF EXISTS evt_data CASCADE;")
    psql_exec(
        TEST_DB,
        """
        CREATE OR REPLACE FUNCTION test_log_ddl() RETURNS event_trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          RAISE NOTICE 'DDL event: %', tg_event;
        END;
        $$;

        CREATE EVENT TRIGGER test_ddl_logger ON ddl_command_end
          EXECUTE FUNCTION test_log_ddl();

        CREATE TABLE evt_data (
            id bigint PRIMARY KEY,
            val text NOT NULL
        );
        INSERT INTO evt_data (id, val)
        SELECT i, 'val_' || i FROM generate_series(1, 20) AS s(i);
    """,
    )
    expected = psql(TEST_DB, "SELECT id, val FROM evt_data ORDER BY id;")

    d = str(tmp_path)
    try:
        run_db_dump(d, num_parts=2)
        assert os.path.isfile(os.path.join(d, GLOBAL_POST_DATA_FILENAME))

        psql_exec(TEST_DB, "DROP EVENT TRIGGER IF EXISTS test_ddl_logger;")
        psql_exec(TEST_DB, "DROP TABLE IF EXISTS evt_data CASCADE;")
        psql_exec(TEST_DB, "DROP FUNCTION IF EXISTS test_log_ddl();")
        run_db_restore(d)

        trigger_exists = int(
            psql(
                TEST_DB,
                "SELECT COUNT(*) FROM pg_event_trigger WHERE evtname = 'test_ddl_logger';",
            )
        )
        assert trigger_exists == 1, "Event trigger missing after restore"
        assert psql(TEST_DB, "SELECT id, val FROM evt_data ORDER BY id;") == expected
    finally:
        psql_exec(TEST_DB, "DROP EVENT TRIGGER IF EXISTS test_ddl_logger;")
        psql_exec(TEST_DB, "DROP TABLE IF EXISTS evt_data CASCADE;")
        psql_exec(TEST_DB, "DROP FUNCTION IF EXISTS test_log_ddl();")


def test_matview_dump_restore(tmp_path):
    """Database-level dump+restore preserves materialized views and their indexes.

    Materialized views are detected during dump via the matview marker
    file, skipped during data loading (they are not regular tables), and
    refreshed during the post-data phase.  This test verifies the source
    table data, the materialized view contents (via REFRESH), and the
    index on the matview are all present after restore.
    """
    psql_exec(TEST_DB, "DROP MATERIALIZED VIEW IF EXISTS mv_summary CASCADE;")
    psql_exec(TEST_DB, "DROP TABLE IF EXISTS mv_source CASCADE;")
    psql_exec(
        TEST_DB,
        """
        CREATE TABLE mv_source (
            id bigint PRIMARY KEY,
            category text NOT NULL,
            amount int NOT NULL
        );
        INSERT INTO mv_source (id, category, amount)
        SELECT i,
               CASE WHEN i % 3 = 0 THEN 'C' WHEN i % 2 = 0 THEN 'B' ELSE 'A' END,
               i * 10
        FROM generate_series(1, 60) AS s(i);

        CREATE MATERIALIZED VIEW mv_summary AS
        SELECT category, COUNT(*) AS cnt, SUM(amount) AS total
        FROM mv_source
        GROUP BY category;

        CREATE INDEX idx_mv_summary_cat ON mv_summary (category);
    """,
    )
    expected_source = psql(TEST_DB, "SELECT id, category, amount FROM mv_source ORDER BY id;")
    expected_mv = psql(TEST_DB, "SELECT category, cnt, total FROM mv_summary ORDER BY category;")

    d = str(tmp_path)
    try:
        run_db_dump(d, num_parts=2)

        mv_dir = os.path.join(d, "public", "mv_summary")
        assert os.path.isdir(mv_dir)
        assert os.path.isfile(os.path.join(mv_dir, MATVIEW_MARKER_FILENAME))

        psql_exec(TEST_DB, "DROP MATERIALIZED VIEW IF EXISTS mv_summary CASCADE;")
        psql_exec(TEST_DB, "DROP TABLE IF EXISTS mv_source CASCADE;")
        run_db_restore(d)

        assert (
            psql(TEST_DB, "SELECT id, category, amount FROM mv_source ORDER BY id;")
            == expected_source
        )
        assert (
            int(
                psql(
                    TEST_DB,
                    "SELECT COUNT(*) FROM pg_matviews "
                    "WHERE schemaname = 'public' AND matviewname = 'mv_summary';",
                )
            )
            == 1
        )
        assert (
            psql(TEST_DB, "SELECT category, cnt, total FROM mv_summary ORDER BY category;")
            == expected_mv
        )
        assert (
            int(
                psql(
                    TEST_DB,
                    "SELECT COUNT(*) FROM pg_indexes "
                    "WHERE schemaname = 'public' AND tablename = 'mv_summary' "
                    "AND indexname = 'idx_mv_summary_cat';",
                )
            )
            == 1
        )
    finally:
        psql_exec(TEST_DB, "DROP MATERIALIZED VIEW IF EXISTS mv_summary CASCADE;")
        psql_exec(TEST_DB, "DROP TABLE IF EXISTS mv_source CASCADE;")


# ---------------------------------------------------------------------------
# Dump retry test
# ---------------------------------------------------------------------------


def test_dump_retry_on_write_failure(tmp_path):
    """Dump --retries retries failed COPY-out workers.

    A dangling symlink at part1.bin causes the first COPY attempt to fail
    with ENOENT.  The retry deletes the symlink and re-runs successfully.
    """
    d = str(tmp_path)
    os.symlink("/proc/nonexistent/file", os.path.join(d, "part1.bin"))

    result = subprocess.run(
        [
            sys.executable,
            _script("parallel_dump.py"),
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
            "--schema",
            SCHEMA,
            "--table",
            TABLE,
            "--num-parts",
            "2",
            "--output-dir",
            d,
            "--retries",
            "1",
        ],
        env=PG_ENV,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, (
        f"Dump failed after retry:\nstdout:\n{result.stdout[-500:]}\nstderr:\n{result.stderr[-500:]}"
    )
    combined = result.stdout + result.stderr
    assert "Retry 1/1" in combined, "Retry message not found in output"
    _assert_dump_parts_nonempty(d)


# ---------------------------------------------------------------------------
# Database-level --unlogged restore
# ---------------------------------------------------------------------------


def test_db_unlogged_restore(tmp_path):
    """Database-level --unlogged restore converts all tables back to LOGGED.

    In database-level mode, --unlogged applies to every table discovered
    in the dump.  After bulk-loading with WAL disabled, each table must
    be switched back to LOGGED.  If any table is left UNLOGGED, it would
    lose data on a server crash.  Verifies data correctness and
    relpersistence='p' for all three multi-table fixtures.
    """
    expected = _create_multi_table_data()
    d = str(tmp_path)
    try:
        run_db_dump(d, num_parts=2)
        _drop_multi_tables()
        run_db_restore(d, unlogged=True)

        assert psql(TEST_DB, "SELECT id, name FROM customers ORDER BY id;") == expected["customers"]
        assert (
            psql(TEST_DB, "SELECT id, title, price FROM products ORDER BY id;")
            == expected["products"]
        )
        assert (
            psql(TEST_DB, "SELECT id, customer_id, product_id, qty FROM orders ORDER BY id;")
            == expected["orders"]
        )

        for tbl in MULTI_TABLES:
            persistence = psql(
                TEST_DB,
                f"SELECT relpersistence FROM pg_class WHERE oid = '{SCHEMA}.{tbl}'::regclass;",
            )
            assert persistence == "p", f"{tbl} not LOGGED after --unlogged restore"
    finally:
        _drop_multi_tables()


# ---------------------------------------------------------------------------
# Composite type test
# ---------------------------------------------------------------------------


def test_composite_type_dump_restore(tmp_path):
    """Database-level dump+restore preserves composite (row) types.

    Composite types created with CREATE TYPE ... AS (...) live in global
    pre-data.  A table column typed as a composite requires the type to
    exist before the table DDL runs.  This test verifies that both the
    type definition and the row-level composite data survive the
    round-trip without corruption.
    """
    psql_exec(TEST_DB, "DROP TABLE IF EXISTS with_composite CASCADE;")
    psql_exec(TEST_DB, "DROP TYPE IF EXISTS address_type CASCADE;")
    psql_exec(
        TEST_DB,
        """
        CREATE TYPE address_type AS (street text, city text, zip text);
        CREATE TABLE with_composite (
            id bigint PRIMARY KEY,
            name text NOT NULL,
            addr address_type
        );
        INSERT INTO with_composite (id, name, addr)
        SELECT i, 'person_' || i,
               ROW('Street ' || i, 'City ' || (i % 5), LPAD(i::text, 5, '0'))::address_type
        FROM generate_series(1, 30) AS s(i);
    """,
    )
    expected = psql(TEST_DB, "SELECT id, name, addr FROM with_composite ORDER BY id;")

    d = str(tmp_path)
    try:
        run_db_dump(d, num_parts=2)

        psql_exec(TEST_DB, "DROP TABLE IF EXISTS with_composite CASCADE;")
        psql_exec(TEST_DB, "DROP TYPE IF EXISTS address_type CASCADE;")
        run_db_restore(d)

        type_exists = int(
            psql(
                TEST_DB,
                "SELECT COUNT(*) FROM pg_type t "
                "JOIN pg_namespace n ON t.typnamespace = n.oid "
                "WHERE t.typname = 'address_type' AND n.nspname = 'public';",
            )
        )
        assert type_exists == 1, "address_type composite not found after restore"
        assert psql(TEST_DB, "SELECT id, name, addr FROM with_composite ORDER BY id;") == expected
    finally:
        psql_exec(TEST_DB, "DROP TABLE IF EXISTS with_composite CASCADE;")
        psql_exec(TEST_DB, "DROP TYPE IF EXISTS address_type CASCADE;")


# ---------------------------------------------------------------------------
# Special character / reserved word table names
# ---------------------------------------------------------------------------


def test_special_char_table_names(tmp_path):
    """Tables with reserved words and hyphens in names survive dump+restore.

    Uses a separate database to isolate from other tests.
    """
    db = "special_chars_test"
    psql_exec("postgres", f"DROP DATABASE IF EXISTS {db};")
    psql_exec("postgres", f"CREATE DATABASE {db};")
    try:
        psql_exec(
            db,
            """
            CREATE TABLE "user" (
                id bigint PRIMARY KEY,
                name text NOT NULL
            );
            INSERT INTO "user" (id, name)
            SELECT i, 'user_' || i FROM generate_series(1, 20) AS s(i);

            CREATE TABLE "order-items" (
                id bigint PRIMARY KEY,
                qty int NOT NULL
            );
            INSERT INTO "order-items" (id, qty)
            SELECT i, i * 5 FROM generate_series(1, 20) AS s(i);
        """,
        )
        expected_user = psql(db, 'SELECT id, name FROM "user" ORDER BY id;')
        expected_items = psql(db, 'SELECT id, qty FROM "order-items" ORDER BY id;')

        d = str(tmp_path)
        run(
            [
                sys.executable,
                _script("parallel_dump.py"),
                "--host",
                PGHOST,
                "--port",
                PGPORT,
                "--user",
                PGUSER,
                "--password",
                PGPASSWORD,
                "--database",
                db,
                "--num-parts",
                "2",
                "--output-dir",
                d,
            ]
        )

        assert os.path.isdir(os.path.join(d, "public", "user"))
        assert os.path.isdir(os.path.join(d, "public", "order-items"))

        psql_exec(db, 'DROP TABLE IF EXISTS "user" CASCADE;')
        psql_exec(db, 'DROP TABLE IF EXISTS "order-items" CASCADE;')

        run(
            [
                sys.executable,
                _script("parallel_restore.py"),
                "--host",
                PGHOST,
                "--port",
                PGPORT,
                "--user",
                PGUSER,
                "--password",
                PGPASSWORD,
                "--database",
                db,
                "--input-dir",
                d,
            ]
        )

        assert psql(db, 'SELECT id, name FROM "user" ORDER BY id;') == expected_user
        assert psql(db, 'SELECT id, qty FROM "order-items" ORDER BY id;') == expected_items
    finally:
        psql_exec("postgres", f"DROP DATABASE IF EXISTS {db};")


# ---------------------------------------------------------------------------
# Standalone sequence value preservation
# ---------------------------------------------------------------------------


def test_standalone_sequence_value_preserved(tmp_path):
    """Standalone sequences (not SERIAL/IDENTITY) preserve values.

    An explicit CREATE SEQUENCE used via DEFAULT nextval() is not
    owned by the table column, so it follows a different pg_depend path
    than bigserial sequences.  Uses database-level mode because pg_dump -t
    does not include standalone sequences in per-table dumps.
    """
    psql_exec(TEST_DB, "DROP TABLE IF EXISTS invoice_data CASCADE;")
    psql_exec(TEST_DB, "DROP SEQUENCE IF EXISTS invoice_num_seq CASCADE;")
    psql_exec(
        TEST_DB,
        """
        CREATE SEQUENCE invoice_num_seq START 1000;
        CREATE TABLE invoice_data (
            id bigint PRIMARY KEY,
            invoice_num bigint NOT NULL DEFAULT nextval('invoice_num_seq')
        );
        INSERT INTO invoice_data (id) SELECT i FROM generate_series(1, 50) AS s(i);
    """,
    )

    seq_val = psql(TEST_DB, "SELECT last_value FROM invoice_num_seq;")

    d = str(tmp_path)
    try:
        run_db_dump(d, num_parts=2)

        seq_file = os.path.join(d, "public", "invoice_data", SEQUENCES_FILENAME)
        assert os.path.isfile(seq_file), "sequences.sql not found for invoice_data"

        psql_exec(TEST_DB, "DROP TABLE IF EXISTS invoice_data CASCADE;")
        psql_exec(TEST_DB, "DROP SEQUENCE IF EXISTS invoice_num_seq CASCADE;")
        run_db_restore(d)

        assert psql(TEST_DB, "SELECT last_value FROM invoice_num_seq;") == seq_val

        psql_exec(TEST_DB, "INSERT INTO invoice_data (id) VALUES (999);")
        new_num = int(psql(TEST_DB, "SELECT invoice_num FROM invoice_data WHERE id = 999;"))
        assert new_num > int(seq_val)
    finally:
        psql_exec(TEST_DB, "DROP TABLE IF EXISTS invoice_data CASCADE;")
        psql_exec(TEST_DB, "DROP SEQUENCE IF EXISTS invoice_num_seq CASCADE;")


def test_standalone_sequence_shared_between_tables(tmp_path):
    """A standalone sequence referenced by multiple tables must restore.

    Each table's ``dump_sequences`` independently sees ``shared_counter``
    as a standalone sequence and prepends its DDL to that table's
    ``pre_data.sql``.  When the second table's pre-data runs, the first
    table has already been restored — so a ``DROP SEQUENCE IF EXISTS``
    there would fail with "cannot drop because other objects depend on
    it".  ``CREATE SEQUENCE IF NOT EXISTS`` is the correct idiom: the
    first table creates the sequence, subsequent tables skip, and
    ``sequences.sql`` still resets the value on each table to the same
    snapshot-time position.
    """
    psql_exec(TEST_DB, "DROP TABLE IF EXISTS ledger_a CASCADE;")
    psql_exec(TEST_DB, "DROP TABLE IF EXISTS ledger_b CASCADE;")
    psql_exec(TEST_DB, "DROP SEQUENCE IF EXISTS shared_counter CASCADE;")
    psql_exec(
        TEST_DB,
        """
        CREATE SEQUENCE shared_counter START 500;
        CREATE TABLE ledger_a (
            id bigint PRIMARY KEY,
            counter bigint NOT NULL DEFAULT nextval('shared_counter')
        );
        CREATE TABLE ledger_b (
            id bigint PRIMARY KEY,
            counter bigint NOT NULL DEFAULT nextval('shared_counter')
        );
        INSERT INTO ledger_a (id) SELECT i FROM generate_series(1, 10) AS s(i);
        INSERT INTO ledger_b (id) SELECT i FROM generate_series(1, 10) AS s(i);
        """,
    )

    expected_a = psql(TEST_DB, "SELECT id, counter FROM ledger_a ORDER BY id;")
    expected_b = psql(TEST_DB, "SELECT id, counter FROM ledger_b ORDER BY id;")

    d = str(tmp_path)
    try:
        run_db_dump(d, num_parts=2)

        psql_exec(TEST_DB, "DROP TABLE IF EXISTS ledger_a CASCADE;")
        psql_exec(TEST_DB, "DROP TABLE IF EXISTS ledger_b CASCADE;")
        psql_exec(TEST_DB, "DROP SEQUENCE IF EXISTS shared_counter CASCADE;")

        # Before the fix, this restore failed at Phase 1 for the second
        # table with "cannot drop sequence shared_counter because other
        # objects depend on it".  After the fix, both tables restore
        # cleanly.
        run_db_restore(d)

        assert psql(TEST_DB, "SELECT id, counter FROM ledger_a ORDER BY id;") == expected_a
        assert psql(TEST_DB, "SELECT id, counter FROM ledger_b ORDER BY id;") == expected_b
    finally:
        psql_exec(TEST_DB, "DROP TABLE IF EXISTS ledger_a CASCADE;")
        psql_exec(TEST_DB, "DROP TABLE IF EXISTS ledger_b CASCADE;")
        psql_exec(TEST_DB, "DROP SEQUENCE IF EXISTS shared_counter CASCADE;")


# ---------------------------------------------------------------------------
# Domain type test
# ---------------------------------------------------------------------------


def test_domain_type_dump_restore(tmp_path):
    """Database-level dump+restore preserves domain types with CHECK constraints.

    Domains (CREATE DOMAIN ... CHECK) are captured in the global
    pre-data phase.  If missing at restore time, the per-table DDL
    would fail because the column type does not exist.  After restore,
    verifies that the domain exists in pg_type with typtype='d' and
    that the table data is intact.
    """
    psql_exec(TEST_DB, "DROP TABLE IF EXISTS measurements CASCADE;")
    psql_exec(TEST_DB, "DROP DOMAIN IF EXISTS positive_int CASCADE;")
    psql_exec(
        TEST_DB,
        """
        CREATE DOMAIN positive_int AS integer CHECK (VALUE > 0);
        CREATE TABLE measurements (
            id bigint PRIMARY KEY,
            value positive_int NOT NULL
        );
        INSERT INTO measurements (id, value)
        SELECT i, i FROM generate_series(1, 30) AS s(i);
    """,
    )
    expected = psql(TEST_DB, "SELECT id, value FROM measurements ORDER BY id;")

    d = str(tmp_path)
    try:
        run_db_dump(d, num_parts=2)

        psql_exec(TEST_DB, "DROP TABLE IF EXISTS measurements CASCADE;")
        psql_exec(TEST_DB, "DROP DOMAIN IF EXISTS positive_int CASCADE;")
        run_db_restore(d)

        domain_exists = int(
            psql(
                TEST_DB,
                "SELECT COUNT(*) FROM pg_type t "
                "JOIN pg_namespace n ON t.typnamespace = n.oid "
                "WHERE t.typname = 'positive_int' AND t.typtype = 'd' AND n.nspname = 'public';",
            )
        )
        assert domain_exists == 1, "positive_int domain not found after restore"
        assert psql(TEST_DB, "SELECT id, value FROM measurements ORDER BY id;") == expected
    finally:
        psql_exec(TEST_DB, "DROP TABLE IF EXISTS measurements CASCADE;")
        psql_exec(TEST_DB, "DROP DOMAIN IF EXISTS positive_int CASCADE;")


# ---------------------------------------------------------------------------
# Config password_env test
# ---------------------------------------------------------------------------


def test_config_password_env(tmp_path):
    """The password_env config option reads the password from an env var.

    Production deployments should not store passwords in config files.
    The password_env option names an environment variable that holds the
    real password.  This test removes PGPASSWORD from the subprocess
    environment and sets only TEST_BACKUP_PW, proving the password is
    resolved through the config indirection — not leaked from the
    inherited environment.
    """
    config_content = (
        "[connection]\n"
        f'host = "{PGHOST}"\n'
        f'port = "{PGPORT}"\n'
        f'user = "{PGUSER}"\n'
        f'password_env = "TEST_BACKUP_PW"\n'
        f'database = "{TEST_DB}"\n'
        "\n[dump]\nnum_parts = 2\n"
    )
    config_path = str(tmp_path / "pw_env_test.toml")
    dump_dir = str(tmp_path / "dump")
    os.makedirs(dump_dir, exist_ok=True)

    with open(config_path, "w") as f:
        f.write(config_content)

    env = {k: v for k, v in PG_ENV.items() if k != "PGPASSWORD"}
    env["TEST_BACKUP_PW"] = PGPASSWORD

    subprocess.run(
        [
            sys.executable,
            _script("parallel_dump.py"),
            "--config",
            config_path,
            "--table",
            TABLE,
            "--output-dir",
            dump_dir,
        ],
        env=env,
        check=True,
    )

    assert os.path.isfile(os.path.join(dump_dir, PRE_DATA_FILENAME))
    _assert_dump_parts_nonempty(dump_dir)


# ---------------------------------------------------------------------------
# JSON logging test
# ---------------------------------------------------------------------------


def test_json_logging_output(tmp_path):
    """--log-format json emits machine-readable structured logs.

    When operators ingest logs into observability systems (ELK, Datadog,
    etc.), JSON output is required.  This test runs a dump with
    --log-format json and verifies: (1) every stdout line is valid JSON,
    (2) each object contains the required ts/level/msg fields, and
    (3) at least one line carries a structured snapshot_id field, proving
    that extra context is attached to JSON log entries.
    """
    d = str(tmp_path)
    result = subprocess.run(
        [
            sys.executable,
            _script("parallel_dump.py"),
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
            "--schema",
            SCHEMA,
            "--table",
            TABLE,
            "--num-parts",
            "2",
            "--output-dir",
            d,
            "--log-format",
            "json",
        ],
        env=PG_ENV,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0

    lines = [line for line in result.stdout.strip().splitlines() if line.strip()]
    assert len(lines) > 0, "No JSON output lines"

    for line in lines:
        obj = json.loads(line)
        assert "ts" in obj
        assert "level" in obj
        assert "msg" in obj

    has_snapshot = any("snapshot_id" in json.loads(line) for line in lines if line.strip())
    assert has_snapshot, "Expected at least one log line with snapshot_id"
