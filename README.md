# pgfanout

Parallel logical dump and restore for PostgreSQL. It fans a single
table out across parallel page-range workers and rebuilds indexes in
parallel on restore, and it streams straight to **local disk or
S3-compatible object storage**. It is a `pg_dump` / `pg_restore`
replacement with no intermediate staging, no server-side file
privileges, and no `pg_dump`/`pg_restore` parallelism caps (we roll our
own).

It works against any PostgreSQL server you can reach over libpq — there
is nothing vendor-specific in it. Data bytes flow from Postgres through
a `psql` subprocess and a pipe straight into the storage sink (local
file or S3 multipart upload), chunk by chunk, so a 1 TB table does not
need 1 TB of local disk.  Python sits on the
*control* plane (snapshot export, catalog discovery, SIGINT
cancellation via `psycopg`) but is deliberately kept off the data hot
path — a design choice dictated by the GIL, which would otherwise
serialise per-chunk COPY parsing across parallel workers.

## Contents

- [When to reach for this](#when-to-reach-for-this)
- [Architecture at a glance](#architecture-at-a-glance)
- [Quick start](#quick-start)
- [CLI reference](#cli-reference)
- [S3 configuration](#s3-configuration)
- [Config file](#config-file)
- [Running the tests](#running-the-tests)

## When to reach for this

Use this tool when the builtin `pg_dump -Fd -j N` / `pg_restore -j N` is
too slow or does not fit your operational constraints:

* **Very large tables**, where parallel dumping within a consistent
  snapshot across physical page ranges (via `ctid`-range scans) is
  significantly faster than `pg_dump`'s one-worker-per-relation split.
* **Stateless compute**, where you cannot afford to materialize the
  dump on local disk before uploading.  This tool can stream every
  byte directly to S3.
* **Multi-tenant / multi-schema restores**, where you want the restore
  phases (data → indexes → FKs → sequences → matviews) laid out
  explicitly and tunable per phase.
* **Slow indexes**, where you want to restore data first at full
  bandwidth and then rebuild indexes in parallel at the end
  (`--index-jobs`).

## Architecture at a glance

```
parallel_dump.py                  parallel_restore.py
─────────────────                 ──────────────────────
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

The split is deliberate:

| Plane   | Carrier            | What runs there                                                    |
|---------|--------------------|--------------------------------------------------------------------|
| Control | `psycopg` in Python | snapshot export/hold, catalog queries (schemas, tables, matviews), `pg_relation_size` for page-range planning, `cancel_safe()` on SIGINT |
| Data    | `psql` subprocess   | COPY TO STDOUT / COPY FROM STDIN for every data part; `pg_dump` / `pg_restore` for DDL and index rebuilds |

Python code never touches the payload bytes in the COPY path.
`common.stream_subprocess_to_sink` (dump) and
`common.stream_source_to_subprocess` (restore) connect the `psql`
subprocess to the `Blob` via one of two paths, picked automatically
per-call based on the destination type:

* **OS-direct fast-path** — when the `Blob` exposes a real OS file
  descriptor (every `LocalBlob`, plus any future backend with a real
  fd), the helpers hand the fd straight to `subprocess.Popen` as
  `stdout=` (dump) or `stdin=` (restore).  Popen calls `dup2(fd, 1/0)`
  in the child, so the subprocess writes to (or reads from) the file
  via its own libc `write()` / `read()` syscalls — *zero* Python in
  the data path of our process, no GIL traffic from the streaming
  step.  N parallel workers scale linearly with CPU cores the same
  way the pre-refactor `psql … > part.bin` shell-redirected pipeline
  did.  A small daemon thread polls `os.fstat(fd).st_size` once a
  second to feed live progress into the shared `bytes_written` dict
  that `progress_monitor` consumes.
* **Python read/write fallback** — when the `Blob` is a Python class
  (`_S3MultipartWriter`, `_S3StreamReader`) the bytes have to traverse
  Python so multipart upload buffers can chunk them.  The familiar
  `while True: chunk = read(); write(chunk)` loop pumps the data with
  per-chunk `bytes_callback` updates, exactly as before.  This is
  also the path used by tests that hand in `io.BytesIO`.

Either way the subprocess stays registered in the cancel registry, a
dedicated thread drains stderr so warnings can't deadlock the
pipeline, and a non-zero exit surfaces as `CalledProcessError` with
the captured stderr attached.

### Storage abstraction

Storage is abstracted behind a single `Blob` interface
(`src/storage.py`) with two backends:

| Backend     | URI form                   | Notes                                    |
|-------------|----------------------------|------------------------------------------|
| `LocalBlob` | `/absolute/path` or `./rel` | Plain files, parent dirs created on open |
| `S3Blob`    | `s3://bucket/key`          | Streaming multipart upload / range read  |

The restore side uses `storage.localize(blob)` for tools that demand a
real on-disk path (`psql -f`, `pg_restore`): local blobs are zero-copy,
S3 blobs are streamed to a `tempfile.NamedTemporaryFile` that is
cleaned up even on exceptions.

## Quick start

### Local disk

```bash
# Dump a whole database in 8 parallel parts
python3 src/parallel_dump.py \
    --host db.example.com --port 5432 \
    --user backup_user --password "$PGPASSWORD" \
    --database mydb --num-parts 8 \
    --output-dir /data/dumps/mydb

# Restore it into a fresh database with 4 index-build jobs
python3 src/parallel_restore.py \
    --host db.example.com --port 5432 \
    --user restore_user --password "$PGPASSWORD" \
    --database mydb_restored --index-jobs 4 \
    --input-dir /data/dumps/mydb
```

### S3-compatible object storage

Point `--output-dir` / `--input-dir` at an `s3://` URI and set the AWS
env vars (see [S3 configuration](#s3-configuration)):

```bash
export AWS_ENDPOINT_URL_S3=https://ceph-rgw.example.com:7483
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_S3_VERIFY_SSL=false      # only if the endpoint has a self-signed cert

python3 src/parallel_dump.py \
    --host db.example.com \
    --user backup_user --password "$PGPASSWORD" \
    --database mydb --num-parts 8 \
    --output-dir s3://my-bucket/dumps/mydb

python3 src/parallel_restore.py \
    --host db.example.com \
    --user restore_user --password "$PGPASSWORD" \
    --database mydb_restored --index-jobs 4 \
    --input-dir s3://my-bucket/dumps/mydb
```

## CLI reference

### Shared options (dump + restore)

| Flag            | Required | Default | Notes                                         |
|-----------------|:--------:|--------:|-----------------------------------------------|
| `--host`        | ✓        |         | Postgres host                                 |
| `--port`        |          | `5432`  |                                               |
| `--user`        | ✓        |         | DB user with SELECT (dump) / CREATE (restore) |
| `--password`    | ✓        |         | Or via `password_env = "PGPASSWORD"` in config |
| `--database`    | ✓        |         |                                               |
| `--config PATH` |          | auto    | TOML file; see [Config file](#config-file)    |
| `--retries N`   |          | `0`     | Per-part retry budget on transient errors     |

### `parallel_dump.py`

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

### `parallel_restore.py`

| Flag              | Default | Notes                                                                        |
|-------------------|--------:|------------------------------------------------------------------------------|
| `--table T`       |         | Omit for database-level restore (auto-detects `<schema>/<table>/` layout)    |
| `--schema S`      | `public`| Only with `--table`                                                          |
| `--index-jobs N`  | `1`     | Parallel `pg_restore -j N` for index / constraint build                      |
| `--unlogged`      | off     | `ALTER TABLE … SET UNLOGGED` during load, `SET LOGGED` after — big speedup   |
| `--input-dir D`   |         | Local path **or** `s3://bucket/prefix`                                       |

The restore is idempotent within a database: each invocation drops and
recreates its tables before loading.

## S3 configuration

All S3 configuration flows through standard AWS environment variables,
resolved inside `src/storage.py::make_s3_client`.  No flags or
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

python3 src/parallel_dump.py ... --output-dir s3://rdb-poc/backups/mydb
```

### Multipart semantics

* Upload: `_S3MultipartWriter` streams data in fixed-size chunks
  (default 16 MiB, minimum 5 MiB per S3 spec) and completes the
  multipart upload on close.  Objects smaller than one chunk fall
  back to a single `PutObject`.
* Download / restore: `S3Blob.open_read()` returns a boto3
  `StreamingBody` that supports chunked `read()` — no full-object
  materialization in memory.
* Failure mode: any exception in the writer aborts the multipart
  upload, preventing orphaned parts from accumulating cost.

## Config file

CLI flags can be defaulted from a TOML config.  Load order (first match wins):

1. `--config /path/to/backup.toml`
2. `./backup.toml`
3. `~/.config/pg-parallel-backup/backup.toml`

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

[restore]
input_dir = "s3://my-bucket/dumps/mydb"
index_jobs = 4
unlogged = false
retries = 1
```

## Running the tests

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

CI runs this on every push against PostgreSQL 14 through 18 service containers,
plus `ruff format --check` and `ruff check`. See
`.github/workflows/ci.yml`.

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

The standalone scripts below are **not** unit tests (they're excluded
from pytest discovery via `conftest.py::collect_ignore`); they require
real credentials and a real S3 endpoint:

* `test_s3_connectivity.py` — confirm the bucket is reachable (reads
  `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `S3_BUCKET`, and the
  optional endpoint/TLS/region vars from the environment — never hard-code
  credentials in source)

## License

Apache License 2.0. See [LICENSE](LICENSE).
