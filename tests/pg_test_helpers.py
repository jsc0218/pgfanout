"""Shared helpers for backup test scripts.

Provides PostgreSQL connection constants, environment setup, and common
psql wrapper functions used by test_parallel_backup.py.
"""

import os
import subprocess
import sys
import time

PGHOST = os.environ.get("PGHOST", "localhost")
PGPORT = os.environ.get("PGPORT", "5432")
PGUSER = os.environ.get("PGUSER", "postgres")
PGPASSWORD = os.environ.get("PGPASSWORD", "")

MAX_RETRIES = 30

PG_ENV = {
    **os.environ,
    "PGHOST": PGHOST,
    "PGPORT": PGPORT,
    "PGUSER": PGUSER,
    "PGPASSWORD": PGPASSWORD,
}


def run(cmd: list[str], **kwargs) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, check=True, env=PG_ENV, **kwargs)


def psql(database: str, sql: str) -> str:
    result = run(
        ["psql", "-d", database, "-v", "ON_ERROR_STOP=1", "-t", "-A", "-X", "-c", sql],
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def psql_exec(database: str, sql: str) -> None:
    run(["psql", "-d", database, "-v", "ON_ERROR_STOP=1", "-X", "-c", sql])


def wait_for_postgres() -> None:
    print("Waiting for Postgres to be ready...")
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            run(["psql", "-d", "postgres", "-X", "-c", r"\q"], capture_output=True)
            print("Postgres is ready.")
            return
        except subprocess.CalledProcessError:
            if attempt >= MAX_RETRIES:
                sys.exit(f"Timed out waiting for Postgres after {attempt}s")
            time.sleep(1)
