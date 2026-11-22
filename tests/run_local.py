#!/usr/bin/env python3
"""Run all backup tests locally using Docker (PG17 client and server).

Spins up a Postgres 17 container, then runs each test script inside a
second container on the same network.

Usage (from repo root):
    python3 tests/run_local.py
"""

import atexit
import subprocess
import sys
import time
from pathlib import Path

IMG = "postgres:17-bookworm"
CONTAINER = "backup-test-pg17"
HOST_PORT = 5434
STARTUP_WAIT = 4  # seconds for Postgres to accept connections

TEST_DIR = "tests/"


def docker(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["docker", *args], check=check)


def remove_container() -> None:
    docker("rm", "-f", CONTAINER, check=False)


def main() -> None:
    repo_root = Path(__file__).resolve().parent.parent

    atexit.register(remove_container)
    remove_container()

    print(f"Starting Postgres server ({IMG}) on host port {HOST_PORT}...")
    docker(
        "run",
        "-d",
        "--name",
        CONTAINER,
        "-e",
        "POSTGRES_PASSWORD=postgres",
        "-p",
        f"{HOST_PORT}:5432",
        IMG,
    )
    time.sleep(STARTUP_WAIT)

    print(f"\n{'=' * 60}")
    print(f"Running pytest on {TEST_DIR}...")
    print(f"{'=' * 60}\n")
    docker(
        "run",
        "--rm",
        "--network",
        "host",
        "-v",
        f"{repo_root}:/repo",
        "-w",
        "/repo",
        "-e",
        "PGHOST=127.0.0.1",
        "-e",
        f"PGPORT={HOST_PORT}",
        "-e",
        "PGUSER=postgres",
        "-e",
        "PGPASSWORD=postgres",
        IMG,
        "bash",
        "-c",
        # Installs from requirements-dev.txt, the same file CI uses, so the
        # two cannot drift into a "works locally, fails in CI" mismatch.
        "apt-get update -qq"
        " && apt-get install -y -qq python3 python3-pip > /dev/null"
        " && pip install --quiet --break-system-packages -r requirements-dev.txt"
        f" && cd {TEST_DIR} && python3 -m pytest -v",
    )

    print("\nAll tests passed.")


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as exc:
        print(f"Command failed with exit code {exc.returncode}", file=sys.stderr)
        sys.exit(exc.returncode)
