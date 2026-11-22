#!/usr/bin/env python3
"""Benchmark pgfanout against itself (two builds) and against pg_dump/pg_restore.

Generates a deterministic dataset once (a single large plain table with no
indexes, which is the case pgfanout targets and where pg_dump -j cannot
split the work, and which isolates raw data dump and load speed), then for
each repeat runs every variant through dump -> restore -> row-count verification. Variants are
interleaved (and their order rotated each round) so that machine noise
affects all of them equally. Results go to a JSON file; use compare.py to
turn that into a report and a pass/fail verdict.

Connection settings come from the standard libpq environment variables
(PGHOST, PGPORT, PGUSER, PGPASSWORD). Needs psql, pg_dump and pg_restore on
PATH, with the same major version as the server.

Variants are given as NAME=KIND[:ARG]:
    base=pgfanout:/path/to/venv/bin    pgfanout-dump/-restore found in that dir
    pg_dump=pgdump                     pg_dump -Fd -Z 0 / pg_restore baseline

Example:
    python3 bench/bench.py --variant pr=pgfanout:.venv/bin \\
        --variant pg_dump=pgdump --size-mb 256 --repeats 3 --out results.json
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

SRC_DB = "pgfanout_bench_src"
DST_DB = "pgfanout_bench_dst"

# Rough heap bytes per generated "big" row, used only to turn --size-mb into a
# row count. The real size is measured and recorded in the results.
BIG_ROW_BYTES = 172


def env_conn() -> dict[str, str]:
    conn = {
        "host": os.environ.get("PGHOST", "localhost"),
        "port": os.environ.get("PGPORT", "5432"),
        "user": os.environ.get("PGUSER", "postgres"),
        "password": os.environ.get("PGPASSWORD", ""),
    }
    if not conn["password"]:
        sys.exit("PGPASSWORD must be set (pgfanout requires --password)")
    return conn


def run(cmd: list[str], *, env: dict[str, str] | None = None) -> float:
    """Run *cmd*, return wall seconds. Exit with the stderr tail on failure."""
    start = time.perf_counter()
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env)
    elapsed = time.perf_counter() - start
    if proc.returncode != 0:
        tail = "\n".join(proc.stderr.strip().splitlines()[-15:])
        sys.exit(f"command failed ({proc.returncode}): {' '.join(cmd[:3])} ...\n{tail}")
    return elapsed


def psql(conn: dict[str, str], db: str, sql: str) -> str:
    env = {**os.environ, "PGPASSWORD": conn["password"]}
    proc = subprocess.run(
        [
            "psql",
            "-X",
            "-At",
            "-v",
            "ON_ERROR_STOP=1",
            "-h",
            conn["host"],
            "-p",
            conn["port"],
            "-U",
            conn["user"],
            "-d",
            db,
            "-c",
            sql,
        ],
        capture_output=True,
        text=True,
        env=env,
    )
    if proc.returncode != 0:
        sys.exit(f"psql failed: {proc.stderr.strip()}")
    return proc.stdout.strip()


def dir_bytes(path: Path) -> int:
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


def generate_dataset(conn: dict[str, str], size_mb: int) -> dict:
    """(Re)create the source database with a fixed, deterministic dataset."""
    psql(conn, "postgres", f"DROP DATABASE IF EXISTS {SRC_DB}")
    psql(conn, "postgres", f"CREATE DATABASE {SRC_DB}")

    rows = int(size_mb * 1024 * 1024 / BIG_ROW_BYTES)
    psql(
        conn,
        SRC_DB,
        f"""
        CREATE TABLE big (
            id bigint NOT NULL,
            a  int NOT NULL,
            b  text NOT NULL,
            c  timestamptz NOT NULL,
            d  numeric(12,2) NOT NULL
        );
        INSERT INTO big
        SELECT i, (i * 7919) % 100000,
               md5(i::text) || md5((i * 3)::text),
               timestamptz '2020-01-01' + (i % 1000000) * interval '1 second',
               ((i * 31) % 1000000) / 100.0
        FROM generate_series(1::bigint, {rows}::bigint) AS s(i);
        """,
    )
    psql(conn, SRC_DB, "VACUUM ANALYZE")
    psql(conn, SRC_DB, "CHECKPOINT")

    tables = psql(
        conn,
        SRC_DB,
        "SELECT relname FROM pg_class WHERE relkind='r' AND relnamespace='public'::regnamespace "
        "ORDER BY 1",
    ).split()
    counts = {t: int(psql(conn, SRC_DB, f"SELECT count(*) FROM {t}")) for t in tables}
    db_bytes = int(psql(conn, SRC_DB, f"SELECT pg_database_size('{SRC_DB}')"))
    return {"row_counts": counts, "source_db_bytes": db_bytes}


def verify(conn: dict[str, str], expected: dict[str, int], variant: str) -> None:
    for table, want in expected.items():
        got = int(psql(conn, DST_DB, f"SELECT count(*) FROM {table}"))
        if got != want:
            sys.exit(f"{variant}: {table} has {got} rows after restore, expected {want}")


def pgfanout_cmds(bindir: str, conn: dict[str, str], jobs: int, out: Path):
    common = [
        "--host",
        conn["host"],
        "--port",
        conn["port"],
        "--user",
        conn["user"],
        "--password",
        conn["password"],
    ]
    dump = [
        str(Path(bindir) / "pgfanout-dump"),
        *common,
        "--database",
        SRC_DB,
        "--num-parts",
        str(jobs),
        "--output-dir",
        str(out),
    ]
    restore = [
        str(Path(bindir) / "pgfanout-restore"),
        *common,
        "--database",
        DST_DB,
        "--index-jobs",
        str(jobs),
        "--input-dir",
        str(out),
    ]
    return dump, restore


def pgdump_cmds(conn: dict[str, str], jobs: int, out: Path):
    base = ["-h", conn["host"], "-p", conn["port"], "-U", conn["user"]]
    dump = ["pg_dump", *base, "-Fd", "-Z", "0", "-j", str(jobs), "-d", SRC_DB, "-f", str(out)]
    restore = ["pg_restore", *base, "-j", str(jobs), "-d", DST_DB, str(out)]
    return dump, restore


def parse_variant(spec: str) -> tuple[str, str, str]:
    name, _, rest = spec.partition("=")
    kind, _, arg = rest.partition(":")
    if not name or kind not in ("pgfanout", "pgdump") or (kind == "pgfanout" and not arg):
        sys.exit(f"bad --variant {spec!r}; use NAME=pgfanout:BINDIR or NAME=pgdump")
    return name, kind, arg


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--variant", action="append", required=True, metavar="NAME=KIND[:ARG]")
    ap.add_argument("--size-mb", type=int, default=1024, help="approximate dataset size")
    ap.add_argument("--repeats", type=int, default=5)
    ap.add_argument(
        "--warmup",
        type=int,
        default=1,
        help="unrecorded rounds first, so cold caches do not skew the first variant",
    )
    ap.add_argument("--jobs", type=int, default=4, help="parallel parts / jobs for every tool")
    ap.add_argument("--workdir", default=None, help="where dumps are written (default: temp dir)")
    ap.add_argument("--out", default="bench-results.json")
    args = ap.parse_args()

    variants = [parse_variant(v) for v in args.variant]
    if len({n for n, _, _ in variants}) != len(variants):
        sys.exit("variant names must be unique")
    conn = env_conn()
    env = {**os.environ, "PGPASSWORD": conn["password"]}

    print(f"Generating ~{args.size_mb} MB dataset ...", flush=True)
    gen_start = time.perf_counter()
    data = generate_dataset(conn, args.size_mb)
    print(
        f"  source database {data['source_db_bytes'] / 1e6:.0f} MB, "
        f"generated in {time.perf_counter() - gen_start:.0f}s",
        flush=True,
    )

    workroot = Path(args.workdir or tempfile.mkdtemp(prefix="pgfanout-bench-"))
    workroot.mkdir(parents=True, exist_ok=True)
    results: dict[str, dict] = {
        n: {"dump_s": [], "restore_s": [], "dump_bytes": None} for n, _, _ in variants
    }

    for rnd in range(-args.warmup, args.repeats):
        # Rotate the order each round so no variant always runs first (warm cache).
        shift = rnd % len(variants)
        order = variants[shift:] + variants[:shift]
        for name, kind, arg in order:
            out = workroot / f"{name}-dump"
            shutil.rmtree(out, ignore_errors=True)
            if kind == "pgfanout":
                out.mkdir()
                dump_cmd, restore_cmd = pgfanout_cmds(arg, conn, args.jobs, out)
            else:
                dump_cmd, restore_cmd = pgdump_cmds(conn, args.jobs, out)

            psql(conn, "postgres", f"DROP DATABASE IF EXISTS {DST_DB}")
            psql(conn, "postgres", f"CREATE DATABASE {DST_DB}")

            dump_s = run(dump_cmd, env=env)
            results[name]["dump_bytes"] = dir_bytes(out)
            restore_s = run(restore_cmd, env=env)
            verify(conn, data["row_counts"], name)

            if rnd >= 0:
                results[name]["dump_s"].append(round(dump_s, 3))
                results[name]["restore_s"].append(round(restore_s, 3))
            shutil.rmtree(out, ignore_errors=True)
            label = f"round {rnd + 1}/{args.repeats}" if rnd >= 0 else "warm-up"
            print(
                f"{label:>11} {name:>10}: dump {dump_s:6.1f}s  restore {restore_s:6.1f}s",
                flush=True,
            )

    psql(conn, "postgres", f"DROP DATABASE IF EXISTS {DST_DB}")
    psql(conn, "postgres", f"DROP DATABASE IF EXISTS {SRC_DB}")
    if not args.workdir:
        shutil.rmtree(workroot, ignore_errors=True)

    meta = {
        "size_mb_requested": args.size_mb,
        "repeats": args.repeats,
        "warmup": args.warmup,
        "jobs": args.jobs,
        "cpus": os.cpu_count(),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "server_version": psql(conn, "postgres", "SHOW server_version"),
        **data,
    }
    for r in results.values():
        r["dump_median_s"] = round(statistics.median(r["dump_s"]), 3)
        r["restore_median_s"] = round(statistics.median(r["restore_s"]), 3)
    Path(args.out).write_text(json.dumps({"meta": meta, "results": results}, indent=2) + "\n")
    print(f"Wrote {args.out}")


if __name__ == "__main__":
    main()
