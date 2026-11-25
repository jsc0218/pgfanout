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
import re
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


def cpu_info() -> dict[str, object]:
    """CPU model and core/thread layout (Linux only; empty elsewhere).

    os.cpu_count() reports logical CPUs, which on a cloud VM are usually
    hyper-threads, so it overstates how many real cores a parallel job has.
    """
    info: dict[str, object] = {"logical_cpus": os.cpu_count()}
    try:
        text = subprocess.run(["lscpu"], capture_output=True, text=True, check=True).stdout
    except (OSError, subprocess.CalledProcessError):
        return info
    fields = {k.strip(): v.strip() for k, _, v in (ln.partition(":") for ln in text.splitlines())}
    info["model"] = fields.get("Model name")
    info["hypervisor"] = fields.get("Hypervisor vendor")
    try:
        threads = int(fields["Thread(s) per core"])
        cores = int(fields["Core(s) per socket"]) * int(fields["Socket(s)"])
        info["threads_per_core"] = threads
        info["physical_cores"] = cores
    except (KeyError, ValueError):
        pass
    return info


def is_local_host(host: str) -> bool:
    return host in ("", "localhost", "127.0.0.1", "::1") or host.startswith("/")


def flush_after_fsync_off(conn: dict[str, str]) -> None:
    """Force data written while fsync was off to durable storage.

    PostgreSQL requires this when fsync is switched back on: without it, a
    crash can corrupt the cluster even though the setting is restored.
    CHECKPOINT writes the shared buffers out and os.sync() flushes the kernel
    buffers, which only reaches the server's disk because fsync is only ever
    turned off for a server on this machine (see is_local_host()).
    """
    psql(conn, "postgres", "CHECKPOINT")
    os.sync()


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


def s3_split(uri: str) -> tuple[str, str]:
    """('s3://bucket/some/prefix') -> ('bucket', 'some/prefix/')."""
    bucket, _, prefix = uri[len("s3://") :].partition("/")
    return bucket, (prefix.rstrip("/") + "/") if prefix else ""


def s3_client():
    # boto3 is only needed for --storage, so import it lazily. The endpoint
    # and region follow the same variables pgfanout reads (see its README).
    try:
        import boto3
    except ImportError:
        sys.exit("--storage needs boto3: run bench.py with the Python of a pgfanout install")
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("AWS_ENDPOINT_URL_S3")
        or os.environ.get("AWS_S3_ENDPOINT_URL")
        or None,
        region_name=os.environ.get("AWS_REGION") or "us-east-1",
    )


def s3_bytes(uri: str) -> int:
    bucket, prefix = s3_split(uri)
    total = 0
    pages = s3_client().get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
    for page in pages:
        total += sum(o["Size"] for o in page.get("Contents", []))
    return total


def s3_clear(uri: str) -> None:
    """Delete every object under *uri*, and abort any unfinished multipart upload."""
    bucket, prefix = s3_split(uri)
    client = s3_client()
    pages = client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
    for page in pages:
        keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if keys:
            client.delete_objects(Bucket=bucket, Delete={"Objects": keys, "Quiet": True})
    pages = client.get_paginator("list_multipart_uploads").paginate(Bucket=bucket, Prefix=prefix)
    for page in pages:
        for up in page.get("Uploads", []):
            client.abort_multipart_upload(Bucket=bucket, Key=up["Key"], UploadId=up["UploadId"])


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

    tables = psql(
        conn,
        SRC_DB,
        "SELECT relname FROM pg_class WHERE relkind='r' AND relnamespace='public'::regnamespace "
        "ORDER BY 1",
    ).split()
    counts = {t: int(psql(conn, SRC_DB, f"SELECT count(*) FROM {t}")) for t in tables}
    db_bytes = int(psql(conn, SRC_DB, f"SELECT pg_database_size('{SRC_DB}')"))
    return {"row_counts": counts, "source_db_bytes": db_bytes}


def settle(conn: dict[str, str]) -> None:
    """Flush dirty data so a timed run starts from a quiet system.

    A restore writes about as much WAL as it loads data. With the default
    max_wal_size that triggers a checkpoint part-way through the run, and
    how much work it has to do depends on whatever the previous run left
    dirty, which is the main source of run-to-run noise. Checkpointing and
    syncing right before each timed command removes that carry-over.
    """
    psql(conn, "postgres", "CHECKPOINT")
    os.sync()
    time.sleep(0.5)


TUNED_SETTINGS = ("max_wal_size", "checkpoint_timeout", "fsync")


def read_overrides(conn: dict[str, str]) -> dict[str, str | None]:
    """Current postgresql.auto.conf value of each tuned setting, or None.

    ALTER SYSTEM RESET deletes an override instead of restoring the previous
    one, so remember what was there to put it back exactly.
    """
    names = ",".join(f"'{n}'" for n in TUNED_SETTINGS)
    rows = psql(
        conn,
        "postgres",
        "SELECT name || '=' || setting FROM pg_file_settings "
        f"WHERE sourcefile LIKE '%postgresql.auto.conf' AND name IN ({names}) "
        "ORDER BY seqno",
    )
    found: dict[str, str | None] = dict.fromkeys(TUNED_SETTINGS)
    for row in rows.splitlines():
        name, _, value = row.partition("=")
        found[name] = value  # later entries win, as they do on the server
    return found


def alter_system(conn: dict[str, str], name: str, value: str | None) -> None:
    if value is None:
        psql(conn, "postgres", f"ALTER SYSTEM RESET {name}")
    else:
        quoted = value.replace("'", "''")
        psql(conn, "postgres", f"ALTER SYSTEM SET {name} = '{quoted}'")


def stabilize_server(
    conn: dict[str, str], size_mb: int, fsync_off: bool, touched: list[str]
) -> None:
    """Take disk-sync and checkpoint noise out of the timed commands.

    The aim is to catch code regressions, not to produce absolute numbers, so
    the server is tuned for repeatability:
      * max_wal_size / checkpoint_timeout are raised so no checkpoint starts
        part-way through a restore;
      * fsync is turned off (unless fsync_off is False) so the result does not
        depend on how fast the runner's disk can sync, which is both noisy and
        outside the code under test.
    Only settings that take effect on reload are touched. Each setting is
    appended to *touched* as soon as it has been changed, so that
    restore_server() undoes exactly what was done even if this fails part-way.
    Do not publish numbers taken this way; use --keep-fsync for those.
    """
    wal_mb = max(4096, 4 * size_mb)
    changes = [("max_wal_size", f"{wal_mb}MB"), ("checkpoint_timeout", "1h")]
    if fsync_off:
        changes.append(("fsync", "off"))
    for name, value in changes:
        alter_system(conn, name, value)
        touched.append(name)
    psql(conn, "postgres", "SELECT pg_reload_conf()")
    time.sleep(1)


def restore_server(
    conn: dict[str, str], originals: dict[str, str | None], touched: list[str]
) -> None:
    """Put back the settings stabilize_server() changed, and only those."""
    for name in touched:
        alter_system(conn, name, originals[name])
    if touched:
        psql(conn, "postgres", "SELECT pg_reload_conf()")


def verify(conn: dict[str, str], expected: dict[str, int], variant: str) -> None:
    for table, want in expected.items():
        got = int(psql(conn, DST_DB, f"SELECT count(*) FROM {table}"))
        if got != want:
            sys.exit(f"{variant}: {table} has {got} rows after restore, expected {want}")


def pgfanout_cmds(bindir: str, conn: dict[str, str], jobs: int, out: Path | str, config: Path):
    # An explicit (empty) config file stops pgfanout from picking up a
    # ./pgfanout.toml or ~/.config/pgfanout/pgfanout.toml, which could change
    # the table, the unlogged mode or the part count being measured.
    common = [
        "--config",
        str(config),
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


def parse_variant(spec: str) -> tuple[str, str, str, int | None]:
    name, _, rest = spec.partition("=")
    # An optional trailing @N pins the variant to N jobs in a --jobs-sweep run.
    pinned = None
    m = re.search(r"@(\d+)$", rest)
    if m:
        pinned = int(m.group(1))
        rest = rest[: m.start()]
        if pinned < 1:
            sys.exit(f"bad --variant {spec!r}: @JOBS must be at least 1")
    kind, _, arg = rest.partition(":")
    # The name becomes a directory under the work dir that is deleted
    # recursively, so it must not be able to point anywhere else.
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", name):
        sys.exit(f"bad variant name {name!r}: use letters, digits, '_', '-' and '.' only")
    if kind not in ("pgfanout", "pgdump") or (kind == "pgfanout" and not arg):
        sys.exit(f"bad --variant {spec!r}; use NAME=pgfanout:BINDIR or NAME=pgdump")
    return name, kind, arg, pinned


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
    ap.add_argument(
        "--jobs-sweep",
        default=None,
        metavar="N,N,...",
        help="run every variant at each of these job counts (e.g. 1,2,4,8,16) in one "
        "interleaved run, to see how each tool scales; results are keyed NAME@JOBS "
        "and --jobs is ignored. Summarize with sweep_report.py",
    )
    ap.add_argument(
        "--no-stabilize",
        action="store_true",
        help="leave server checkpoint settings alone and skip the pre-run checkpoint",
    )
    ap.add_argument(
        "--keep-fsync",
        action="store_true",
        help="leave fsync on (realistic disk behaviour, noisier). By default fsync is "
        "turned off, for a server on this machine only",
    )
    ap.add_argument(
        "--storage",
        default=None,
        metavar="s3://BUCKET/PREFIX",
        help="write the dump to S3 (or an S3-compatible store) instead of local disk and "
        "restore from it. Only pgfanout variants can do this. Each run uses its own "
        "unique prefix under PREFIX and deletes it afterwards. Credentials, endpoint "
        "and region come from the usual AWS_* environment variables",
    )
    ap.add_argument("--workdir", default=None, help="where dumps are written (default: temp dir)")
    ap.add_argument("--out", default="bench-results.json")
    args = ap.parse_args()

    if args.repeats < 1:
        ap.error("--repeats must be at least 1")
    if args.warmup < 0:
        ap.error("--warmup must not be negative")
    if args.jobs_sweep is None and args.jobs < 1:
        ap.error("--jobs must be at least 1")
    if args.jobs_sweep is not None:
        try:
            args.sweep = [int(x) for x in args.jobs_sweep.split(",")]
        except ValueError:
            ap.error("--jobs-sweep takes comma-separated integers, e.g. 1,2,4,8")
        if not args.sweep or min(args.sweep) < 1 or len(set(args.sweep)) != len(args.sweep):
            ap.error("--jobs-sweep needs distinct job counts of at least 1")
    else:
        args.sweep = None
    if args.size_mb < 1:
        ap.error("--size-mb must be at least 1")
    variants = [parse_variant(v) for v in args.variant]
    if len({v[0] for v in variants}) != len(variants):
        ap.error("variant names must be unique")
    if args.sweep is None and any(v[3] for v in variants):
        ap.error("NAME=...@JOBS pins a variant's job count and needs --jobs-sweep")
    if args.storage is not None:
        if not re.fullmatch(r"s3://[^/\s]+(/\S*)?", args.storage):
            ap.error("--storage must look like s3://BUCKET/PREFIX")
        if any(v[1] == "pgdump" for v in variants):
            ap.error("--storage only works with pgfanout variants; pg_dump cannot write to S3")
        # A prefix of our own, so a leftover or concurrent run is never touched.
        args.storage_run = f"{args.storage.rstrip('/')}/bench-{int(time.time())}-{os.getpid()}"
    else:
        args.storage_run = None
    conn = env_conn()
    env = {**os.environ, "PGPASSWORD": conn["password"]}

    workroot = Path(args.workdir or tempfile.mkdtemp(prefix="pgfanout-bench-"))
    workroot.mkdir(parents=True, exist_ok=True)
    empty_config = workroot / "empty.toml"
    empty_config.write_text("")

    # fsync is only turned off for a server on this machine: switching it back
    # on safely needs the server's kernel buffers flushed (flush_after_fsync_off),
    # which os.sync() can only do locally.
    fsync_off = not args.no_stabilize and not args.keep_fsync and is_local_host(conn["host"])
    if not args.no_stabilize and not args.keep_fsync and not fsync_off:
        print(f"note: leaving fsync on, {conn['host']!r} is not a local server", file=sys.stderr)
    originals = read_overrides(conn) if not args.no_stabilize else {}
    touched: list[str] = []
    try:
        if not args.no_stabilize:
            stabilize_server(conn, args.size_mb, fsync_off, touched)
        data, results = run_benchmark(args, variants, conn, env, workroot, empty_config)
    finally:
        # Runs on success, failure and Ctrl-C alike, and must not hide the
        # original error if cleanup itself fails.
        cleanup = [
            ("drop databases", lambda: drop_databases(conn)),
            ("restore server settings", lambda: restore_server(conn, originals, touched)),
        ]
        if args.storage_run:
            cleanup.append(("delete the S3 test objects", lambda: s3_clear(args.storage_run)))
        if "fsync" in touched:
            cleanup.append(
                ("flush data written with fsync off", lambda: flush_after_fsync_off(conn))
            )
        for label, step in cleanup:
            try:
                step()
            except BaseException as exc:  # noqa: BLE001
                print(f"warning: could not {label}: {exc}", file=sys.stderr)
        if not args.workdir:
            shutil.rmtree(workroot, ignore_errors=True)

    meta = {
        "size_mb_requested": args.size_mb,
        "repeats": args.repeats,
        "warmup": args.warmup,
        "stabilized": not args.no_stabilize,
        "fsync_off": fsync_off,
        "jobs": max(args.sweep) if args.sweep else args.jobs,
        "jobs_sweep": args.sweep,
        "storage": "s3" if args.storage else "local",
        "cpus": os.cpu_count(),
        "cpu_info": cpu_info(),
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


def drop_databases(conn: dict[str, str]) -> None:
    psql(conn, "postgres", f"DROP DATABASE IF EXISTS {DST_DB}")
    psql(conn, "postgres", f"DROP DATABASE IF EXISTS {SRC_DB}")


def run_benchmark(args, variants, conn, env, workroot: Path, empty_config: Path):
    print(f"Generating ~{args.size_mb} MB dataset ...", flush=True)
    gen_start = time.perf_counter()
    data = generate_dataset(conn, args.size_mb)
    print(
        f"  source database {data['source_db_bytes'] / 1e6:.0f} MB, "
        f"generated in {time.perf_counter() - gen_start:.0f}s",
        flush=True,
    )

    # One entry per (variant, job count); without a sweep that is just the
    # variants. A variant pinned with @JOBS runs once, at that job count, and
    # keeps its plain name (a reference point beside the sweep).
    combos = []
    for j in args.sweep or [args.jobs]:
        for n, k, a, pinned in variants:
            if pinned is None:
                combos.append((f"{n}@{j}" if args.sweep else n, k, a, j))
    combos += [(n, k, a, pinned) for n, k, a, pinned in variants if pinned]
    results: dict[str, dict] = {
        key: {"dump_s": [], "restore_s": [], "dump_bytes": None, "jobs": jobs}
        for key, _, _, jobs in combos
    }
    for rnd in range(-args.warmup, args.repeats):
        # Rotate the order each round so no variant always runs first (warm cache).
        shift = rnd % len(combos)
        order = combos[shift:] + combos[:shift]
        for name, kind, arg, jobs in order:
            if args.storage_run:
                out = f"{args.storage_run}/{name}-dump"
                s3_clear(out)
            else:
                out = workroot / f"{name}-dump"
                shutil.rmtree(out, ignore_errors=True)
                if kind == "pgfanout":
                    out.mkdir()
            if kind == "pgfanout":
                dump_cmd, restore_cmd = pgfanout_cmds(arg, conn, jobs, out, empty_config)
            else:
                dump_cmd, restore_cmd = pgdump_cmds(conn, jobs, out)

            psql(conn, "postgres", f"DROP DATABASE IF EXISTS {DST_DB}")
            psql(conn, "postgres", f"CREATE DATABASE {DST_DB}")

            if not args.no_stabilize:
                settle(conn)
            dump_s = run(dump_cmd, env=env)
            results[name]["dump_bytes"] = s3_bytes(out) if args.storage_run else dir_bytes(out)
            if not args.no_stabilize:
                settle(conn)
            restore_s = run(restore_cmd, env=env)
            verify(conn, data["row_counts"], name)

            if rnd >= 0:
                results[name]["dump_s"].append(round(dump_s, 3))
                results[name]["restore_s"].append(round(restore_s, 3))
            if args.storage_run:
                s3_clear(out)
            else:
                shutil.rmtree(out, ignore_errors=True)
            label = f"round {rnd + 1}/{args.repeats}" if rnd >= 0 else "warm-up"
            print(
                f"{label:>11} {name:>14}: dump {dump_s:6.1f}s  restore {restore_s:6.1f}s",
                flush=True,
            )
    return data, results


if __name__ == "__main__":
    main()
