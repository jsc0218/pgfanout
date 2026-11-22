"""Pytest configuration and shared fixtures for backup tests."""

import pytest

# Standalone scripts that happen to start with test_ but require external
# services (real S3 endpoint + credentials) and should not run under pytest.
collect_ignore = [
    "test_s3_connectivity.py",
]

from pg_test_helpers import psql, psql_exec, wait_for_postgres  # noqa: E402

TEST_DB = "parallel_backup_test"
SCHEMA = "public"
TABLE = "test_data"
NUM_ROWS = 200


@pytest.fixture(scope="session", autouse=True)
def test_db():
    """Create the test database with seed data; drop on session teardown."""
    wait_for_postgres()
    psql_exec("postgres", f"DROP DATABASE IF EXISTS {TEST_DB};")
    psql_exec("postgres", f"CREATE DATABASE {TEST_DB};")
    psql_exec(
        TEST_DB,
        f"""
        CREATE TABLE {TABLE} (
            id   bigint PRIMARY KEY,
            name text NOT NULL,
            val  int  NOT NULL
        );
        INSERT INTO {TABLE} (id, name, val)
        SELECT i, 'row_' || i, i * 10
        FROM generate_series(1, {NUM_ROWS}) AS s(i);
        """,
    )
    yield
    psql_exec("postgres", f"DROP DATABASE IF EXISTS {TEST_DB};")


@pytest.fixture(scope="session")
def expected_rows(test_db):
    """Capture the canonical row data once for the entire session."""
    return psql(TEST_DB, f"SELECT id, name, val FROM {TABLE} ORDER BY id;")
