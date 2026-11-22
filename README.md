# pgfanout

<p align="center"><b>Parallel logical dump and restore for PostgreSQL, streaming straight to S3 or local disk.</b></p>

[![CI](https://github.com/jsc0218/pgfanout/actions/workflows/ci.yml/badge.svg)](https://github.com/jsc0218/pgfanout/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/pgfanout.svg?logo=pypi&logoColor=white)](https://pypi.org/project/pgfanout/)
[![Python](https://img.shields.io/pypi/pyversions/pgfanout.svg?logo=python&logoColor=white)](https://pypi.org/project/pgfanout/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14%20%7C%2015%20%7C%2016%20%7C%2017%20%7C%2018-336791.svg?logo=postgresql&logoColor=white)](#-install)
[![S3](https://img.shields.io/badge/S3-compatible-569A31.svg?logo=amazons3&logoColor=white)](#-s3-configuration)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/jsc0218/pgfanout/blob/main/LICENSE)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Stars](https://img.shields.io/github/stars/jsc0218/pgfanout?logo=github)](https://github.com/jsc0218/pgfanout/stargazers)
[![Last commit](https://img.shields.io/github/last-commit/jsc0218/pgfanout?logo=git&logoColor=white)](https://github.com/jsc0218/pgfanout/commits/main)
[![Issues](https://img.shields.io/github/issues/jsc0218/pgfanout?logo=github)](https://github.com/jsc0218/pgfanout/issues)

`pgfanout-dump` and `pgfanout-restore` replace `pg_dump` / `pg_restore`
for logical backups of a PostgreSQL database.

## ✨ Highlights

* ⚡ **Parallel inside a single table.** One table is split across page-range
  workers (`ctid` scans), all under one consistent snapshot.
* ☁️ **Straight to S3 or local disk.** Table data is streamed, never staged:
  no local copy of the dump and no server-side file privileges.
* 🔁 **Parallel index rebuild** on restore (`--index-jobs`), with the load
  phases laid out explicitly: data, indexes, FKs, sequences, matviews.
* 🐍 **Python on the control plane.** Snapshot export, catalog discovery,
  cancellation and retries are Python; the bytes flow through native tools.
* 🔌 **Any PostgreSQL 14+ you can reach over libpq.** Nothing vendor-specific.
* 📊 **Benchmarked in CI.** Every PR is compared against `main` on the same
  runner (see [Benchmarks](#-benchmarks)).

Data bytes flow from Postgres through a `psql` subprocess and a pipe
into the storage sink (local file or S3 multipart upload), chunk by
chunk, so a 1 TB table does not need 1 TB of local disk. Python runs the
*control* plane (snapshot export, catalog discovery, SIGINT cancellation
via `psycopg`). For local files it stays off the data path entirely; for
S3 it relays each chunk so it can be sent as a multipart upload. See
[Architecture at a glance](#-architecture-at-a-glance).

## 📚 Contents

- [When to reach for this](#-when-to-reach-for-this)
- [Architecture at a glance](#-architecture-at-a-glance)
- [Install](#-install)
- [Quick start](#-quick-start)
- [CLI reference](#-cli-reference)
- [S3 configuration](#-s3-configuration)
- [Config file](#-config-file)
- [Benchmarks](#-benchmarks)
- [Running the tests](#-running-the-tests)

## 🎯 When to reach for this

Use this tool when the builtin `pg_dump -Fd -j N` / `pg_restore -j N` is
too slow or does not fit your operational constraints:

* **Very large tables**, where `pg_dump -Fd -j N` still dumps each
  relation with a single worker. pgfanout splits one table across
  physical page ranges (`ctid`-range scans) within one consistent
  snapshot, so its workers run in parallel.
* **Stateless compute**, where you cannot afford to materialize the
  dump on local disk before uploading.  This tool can stream every
  byte directly to S3.
* **Multi-tenant / multi-schema restores**, where you want the restore
  phases (data → indexes → FKs → sequences → matviews) laid out
  explicitly.
* **Slow indexes**, where you want to restore data first at full
  bandwidth and then rebuild indexes in parallel at the end
  (`--index-jobs`).

## 🧩 Architecture at a glance

```
pgfanout-dump                     pgfanout-restore
─────────────                     ────────────────
  1. pg_export_snapshot()           1. discover <schema>/<table>/ dirs
       (psycopg, held open)         2. apply pre_data.sql     (psql -f)
  2. pg_dump DDL                    3. COPY FROM workers:
       pg_dump → stream → Blob           Blob → stream → psql FROM STDIN
  3. TID-range COPY workers:        4. pg_restore post_data.dump
       psql TO STDOUT → stream → Blob    (indexes + constraints)
  4. sequences.sql                  5. setval() sequences
                                    6. global_post_data (event triggers…)
                                    7. ANALYZE; matviews (create/refresh)
```

### Control plane vs data plane

The split is deliberate: the GIL would otherwise serialise per-chunk
COPY parsing across parallel workers.

| Plane   | Carrier            | What runs there                                                    |
|---------|--------------------|--------------------------------------------------------------------|
| Control | `psycopg` in Python | snapshot export/hold, catalog queries (schemas, tables, matviews), `pg_relation_size` for page-range planning, `cancel_safe()` on SIGINT |
| Data    | `psql` subprocess   | COPY TO STDOUT / COPY FROM STDIN for every data part; `pg_dump` / `pg_restore` for DDL and index rebuilds |

For local files, Python never touches the payload bytes in the COPY
path; for S3 it relays them chunk by chunk (see the second path below).
`pgfanout.common.stream_subprocess_to_sink` (dump) and
`pgfanout.common.stream_source_to_subprocess` (restore) connect the `psql`
subprocess to the `Blob` via one of two paths, picked automatically
per-call based on the destination type:

* **OS-direct fast-path** — when the `Blob` exposes a real OS file
  descriptor (every `LocalBlob`), the helpers hand the fd straight to `subprocess.Popen` as
  `stdout=` (dump) or `stdin=` (restore).  Popen calls `dup2(fd, 1/0)`
  in the child, so the subprocess writes to (or reads from) the file
  via its own libc `write()` / `read()` syscalls — *zero* Python in
  the data path of our process, no GIL traffic from the streaming
  step.  N parallel workers therefore do not contend on the GIL.  A small daemon thread polls `os.fstat(fd).st_size` once a
  second to feed live progress into the shared `bytes_written` dict
  that `progress_monitor` consumes.
* **Python read/write fallback** — when the `Blob` is a Python class
  (`_S3MultipartWriter`, `_S3StreamReader`) the bytes have to traverse
  Python so multipart upload buffers can chunk them.  The familiar
  `while True: chunk = read(); write(chunk)` loop pumps the data with
  per-chunk `bytes_callback` updates.  This is
  also the path used by tests that hand in `io.BytesIO`.

Either way the subprocess stays registered in the cancel registry, a
dedicated thread drains stderr so warnings can't deadlock the
pipeline, and a non-zero exit surfaces as `CalledProcessError` with
the captured stderr attached.

### Storage abstraction

Storage is abstracted behind a single `Blob` interface
(`src/pgfanout/storage.py`) with two backends:

| Backend     | URI form                   | Notes                                    |
|-------------|----------------------------|------------------------------------------|
| `LocalBlob` | `/absolute/path` or `./rel` | Plain files, parent dirs created on open |
| `S3Blob`    | `s3://bucket/key`          | Streaming multipart upload / range read  |

The restore side uses `storage.localize(blob)` for tools that demand a
real on-disk path (`psql -f`, `pg_restore`): local blobs are zero-copy,
S3 blobs are streamed to a `tempfile.NamedTemporaryFile` that is
cleaned up even on exceptions.

## 📦 Install

```bash
pip install pgfanout
```

This installs two commands, `pgfanout-dump` and `pgfanout-restore`.

Requirements:

* Python 3.10 or newer.
* A PostgreSQL 14+ server (the dump uses TID range scans, added in 14).
* The PostgreSQL client tools (`psql`, `pg_dump`, `pg_restore`) on your
  `PATH`, with the same major version as the server, because `pg_dump`
  refuses to talk to a newer server.

## 🚀 Quick start

### Local disk

```bash
# Dump a whole database in 8 parallel parts
pgfanout-dump \
    --host db.example.com --port 5432 \
    --user backup_user --password "$PGPASSWORD" \
    --database mydb --num-parts 8 \
    --output-dir /data/dumps/mydb

# Restore it into a fresh database with 4 index-build jobs
pgfanout-restore \
    --host db.example.com --port 5432 \
    --user restore_user --password "$PGPASSWORD" \
    --database mydb_restored --index-jobs 4 \
    --input-dir /data/dumps/mydb
```

### S3-compatible object storage

Point `--output-dir` / `--input-dir` at an `s3://` URI and set the AWS
env vars (see [S3 configuration](#-s3-configuration)):

```bash
export AWS_ENDPOINT_URL_S3=https://ceph-rgw.example.com:7483
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_S3_VERIFY_SSL=false      # only if the endpoint has a self-signed cert

pgfanout-dump \
    --host db.example.com \
    --user backup_user --password "$PGPASSWORD" \
    --database mydb --num-parts 8 \
    --output-dir s3://my-bucket/dumps/mydb

pgfanout-restore \
    --host db.example.com \
    --user restore_user --password "$PGPASSWORD" \
    --database mydb_restored --index-jobs 4 \
    --input-dir s3://my-bucket/dumps/mydb
```

## 🔧 CLI reference

### Shared options (dump + restore)

| Flag            | Required | Default | Notes                                         |
|-----------------|:--------:|--------:|-----------------------------------------------|
| `--host`        | ✓        |         | Postgres host                                 |
| `--port`        |          | `5432`  |                                               |
| `--user`        | ✓        |         | DB user with SELECT (dump) / CREATE (restore) |
| `--password`    | ✓        |         | Or via `password_env = "PGPASSWORD"` in config |
| `--database`    | ✓        |         |                                               |
| `--config PATH` |          | auto    | TOML file; see [Config file](#-config-file)    |
| `--retries N`   |          | `0`     | Per-part retry budget on transient errors     |
| `--log-format`  |          | `text`  | `text` or `json`                              |
| `--log-level`   |          | `info`  | `debug`, `info`, `warning` or `error`         |

### `pgfanout-dump`

| Flag              | Default | Notes                                                               |
|-------------------|--------:|---------------------------------------------------------------------|
| `--table T`       |         | Omit for **database-level** mode (all user schemas + tables)        |
| `--schema S`      | `public`| Only with `--table`                                                 |
| `--num-parts N`   | `4`     | TID-range partitions per table; cap at number of pages              |
| `--output-dir D`  | `/tmp`  | Local path **or** `s3://bucket/prefix`                              |

Output layout:

* Single-table mode: files land directly under `--output-dir`
  (`pre_data.sql`, `post_data.dump`, `sequences.sql`, `part*.bin`).
* Database-level mode: `<output-dir>/<schema>/<table>/` per table,
  plus top-level `global_pre_data.dump` and `global_post_data.dump`
  for extensions, enums, event triggers, etc.

### `pgfanout-restore`

| Flag              | Default | Notes                                                                        |
|-------------------|--------:|------------------------------------------------------------------------------|
| `--table T`       |         | Omit for database-level restore (auto-detects `<schema>/<table>/` layout)    |
| `--schema S`      | `public`| Only with `--table`                                                          |
| `--index-jobs N`  | `1`     | Parallel `pg_restore -j N` for index / constraint build                      |
| `--unlogged`      | off     | `ALTER TABLE … SET UNLOGGED` during load, `SET LOGGED` after; much faster, but a server crash mid-load truncates the table and the restore must start over |
| `--input-dir D`   |         | **Required.** Local path **or** `s3://bucket/prefix`                         |

The restore is idempotent within a database: each invocation drops and
recreates its tables before loading. In database-level mode the drop uses
`CASCADE`, so objects that depend on those tables are dropped too.

## 🪣 S3 configuration

All S3 configuration flows through standard AWS environment variables,
resolved inside `src/pgfanout/storage.py::make_s3_client`.  No flags or
config keys are involved — this keeps credentials out of TOML files
and out of `ps` output.

| Env var                          | Purpose                                                         |
|----------------------------------|-----------------------------------------------------------------|
| `AWS_ENDPOINT_URL_S3`            | Override endpoint (Ceph RGW, MinIO, LocalStack). AWS users omit |
| `AWS_S3_ENDPOINT_URL`            | Legacy alias (same behavior)                                    |
| `AWS_ACCESS_KEY_ID`              | Standard boto3 credential chain                                 |
| `AWS_SECRET_ACCESS_KEY`          | Standard boto3 credential chain                                 |
| `AWS_SESSION_TOKEN`              | If using STS / assumed-role credentials                         |
| `AWS_REGION`                     | Defaults to `us-east-1` when unset (most Ceph/MinIO accept it)  |
| `AWS_S3_VERIFY_SSL`              | `false` to disable TLS verify, or a path to a CA bundle         |

Credentials are also read from `~/.aws/credentials` if not set
explicitly — boto3's default chain applies.

### Ceph RGW example

```bash
export AWS_ENDPOINT_URL_S3=https://object-store-cluster.example.com:7483
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_S3_VERIFY_SSL=false       # self-signed certificate
export AWS_REGION=us-east-1          # Ceph is region-agnostic, but boto3 wants a value

pgfanout-dump ... --output-dir s3://rdb-poc/backups/mydb
```

### Multipart semantics

* Upload: `_S3MultipartWriter` streams data in fixed-size chunks
  (default 16 MiB, minimum 5 MiB per S3 spec) and completes the
  multipart upload on close.  The final part may be smaller than the
  chunk size.  An empty object is written with a plain `PutObject`,
  because S3 rejects a multipart upload with zero parts.
* Download / restore: `S3Blob.open_read()` returns a boto3
  `StreamingBody` that supports chunked `read()` — no full-object
  materialization in memory.
* Failure mode: any exception in the writer aborts the multipart
  upload, preventing orphaned parts from accumulating cost.

## 📝 Config file

CLI flags can be defaulted from a TOML config.  Load order (first match wins):

1. `--config /path/to/pgfanout.toml`
2. `./pgfanout.toml`
3. `~/.config/pgfanout/pgfanout.toml`

CLI flags always override config values.

```toml
[connection]
host = "db.example.com"
port = "5432"
user = "backup_user"
password_env = "PGPASSWORD"    # read from env instead of inlining
database = "mydb"

[dump]
num_parts = 8
output_dir = "s3://my-bucket/dumps/mydb"
retries = 2

[logging]
format = "text"
level = "info"

[restore]
input_dir = "s3://my-bucket/dumps/mydb"
index_jobs = 4
unlogged = false
retries = 1
```

## 📊 Benchmarks

`bench/bench.py` generates a deterministic dataset, a single large plain
table with no indexes (so it measures raw data dump and load speed, not
index builds), then runs dump, restore and a row-count check for each
variant you give it, interleaving the
variants and discarding a warm-up round so machine noise hits all of them
equally. `bench/compare.py` turns the JSON into a table and a verdict.

```bash
export PGHOST=localhost PGPORT=5432 PGUSER=postgres PGPASSWORD=postgres
python3 bench/bench.py \
    --variant pr=pgfanout:.venv/bin \
    --variant pg_dump=pgdump \
    --size-mb 1024 --repeats 5 --out results.json
python3 bench/compare.py results.json --base pr --pr pr --reference pg_dump
```

The harness is built to catch code regressions, not to produce absolute
numbers, so it tunes the server for repeatability. Before each timed command
it runs `CHECKPOINT` and `sync`. For the duration of the run it raises
`max_wal_size` and `checkpoint_timeout` (a restore writes about as much WAL
as it loads data, and a checkpoint landing part-way through was the main
source of run-to-run noise) and turns `fsync` off, so results do not depend
on how fast the disk can sync. All of it is reset at the end. Numbers taken
this way flatter the disk-bound restore phase, so do not publish them: use
`--keep-fsync` to leave `fsync` on, or `--no-stabilize` to leave the server
completely alone.

The `pg_dump` variant runs `pg_dump -Fd -Z 0 -j N` and `pg_restore -j N`,
uncompressed so the comparison is like for like. Use `--variant
NAME=pgfanout:BINDIR` once per build to compare two builds, for example an
installed release and your working tree.

The **Benchmark** workflow does this on pull requests that touch `src/` or
`bench/`: it builds the base branch and the PR into separate virtualenvs on
the same runner, compares the median times, and writes a table to the job
summary. It is advisory by default and never runs on a schedule. Start it
by hand from the Actions tab (**Benchmark → Run workflow**) to choose the
dataset size, the number of rounds, the threshold, and whether a regression
fails the job. GitHub-hosted runners are noisy, so it only catches large
regressions (the default threshold is 20%); use a dedicated machine for
numbers you intend to publish.

## 🧪 Running the tests

The easiest way is the Docker runner, which brings up a Postgres
container, installs the dependencies, and runs pytest inside:

```bash
python3 tests/run_local.py
```

Against a Postgres you already have running, install the test
dependencies and point the standard libpq environment variables at it.
The client tools on your `PATH` must be the same major version as the
server, since `pg_dump` refuses to talk to a newer one:

```bash
pip install -r requirements-dev.txt

export PGHOST=localhost PGPORT=5432 PGUSER=postgres PGPASSWORD=postgres
cd tests && python3 -m pytest -v
```

`PGPASSWORD` has to be non-empty even against a `trust` server, because
the tools require `--password` to be supplied.

CI runs this on every push against PostgreSQL 14 through 18 service
containers, plus `ruff format --check`, `ruff check`, and a package
build check. See `.github/workflows/ci.yml`.

Test layout:

| File                              | What it covers                                                        |
|-----------------------------------|-----------------------------------------------------------------------|
| `test_common.py`                  | `ConnInfo`, `psql_query/exec`, `stream_subprocess_to_sink`, `stream_source_to_subprocess`, `_real_fd` / OS-direct fast-path |
| `test_cancel.py`                  | SIGINT-driven subprocess + connection cancellation, `tracked_connection` semantics |
| `test_snapshot.py`                | `export_snapshot`, snapshot-holder watchdog, dump-side progress monitor |
| `test_storage.py`                 | `LocalBlob`, `S3Blob` (via `botocore.stub.Stubber`), multipart upload, `localize`, scheme-aware path helpers |
| `test_pg_restore_stderr.py`       | Classification of `pg_restore` stderr — benign "already exists" triplets vs. real failures |
| `test_phase_orchestration.py`     | Database-level restore phases forward `--index-jobs` to pg_restore (regression guard for serial FK validation) |
| `test_parallel_backup.py`         | End-to-end dump + restore round-trip on local disk                    |
| `test_parallel_backup_s3.py`      | End-to-end dump + restore round-trip on S3 (via `moto.server`)        |

`test_s3_connectivity.py` is a standalone script, not a unit test. It is
excluded from pytest discovery via `conftest.py::collect_ignore` and needs
real credentials and a real S3 endpoint. It reads `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, `S3_BUCKET`, and the optional endpoint/TLS/region
variables from the environment.

It is **not read-only**: it uploads, downloads and then deletes an object at
`S3_TEST_KEY` (default `backup-test/connectivity-test.txt`). An existing
object at that key is overwritten and then deleted, so point `S3_TEST_KEY`
at an unused key before running it against a bucket that holds real data.

## 📄 License

Apache License 2.0. See [LICENSE](https://github.com/jsc0218/pgfanout/blob/main/LICENSE).
