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
  runner (see [`bench/`](bench/README.md)).

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
path: `psql` writes (or reads) the file directly. For S3 it relays them chunk
by chunk so they can be sent as a multipart upload. The two paths, and what
the two helpers `stream_subprocess_to_sink` / `stream_source_to_subprocess`
do for cancellation and error handling, are described in
[docs/architecture.md](docs/architecture.md).

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

The multipart upload parts are 16 MiB (5 MiB is the S3 minimum). A failed
upload is aborted on a best-effort basis: if the abort call itself fails (the
endpoint is down, or `s3:AbortMultipartUpload` is not allowed), the uploaded
parts stay and keep accruing storage charges, so add a bucket lifecycle rule
that aborts incomplete multipart uploads. See `src/pgfanout/storage.py`.

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

## 🧪 Running the tests

```bash
python3 tests/run_local.py      # starts a Postgres container and runs pytest in it
```

or `cd tests && python3 -m pytest -v` against a Postgres you already have
running. The setup details and the test layout are in
[tests/README.md](tests/README.md).

## 📄 License

Apache License 2.0. See [LICENSE](https://github.com/jsc0218/pgfanout/blob/main/LICENSE).
