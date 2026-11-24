# Tests

## Run them

The easiest way is the Docker runner, which brings up a Postgres
container, installs the dependencies, and runs pytest inside. From the
repository root:

```bash
python3 tests/run_local.py
```

> **Warning:** the runner force-removes any existing container named
> `backup-test-pg17` before starting its own, and publishes the server on host
> port 5434. If you have a container with that name, rename it first.

Against a Postgres you already have running, install the test
dependencies and point the standard libpq environment variables at it.
The client tools on your `PATH` must be the same major version as the
server, since `pg_dump` refuses to talk to a newer one.

> **Warning:** the suite drops and recreates the databases
> `parallel_backup_test` and `special_chars_test` on that server, and drops
> them again when it finishes. Any data in databases with those names is
> lost, so use a disposable server.

```bash
pip install -r requirements-dev.txt

export PGHOST=localhost PGPORT=5432 PGUSER=postgres PGPASSWORD=postgres
cd tests && python3 -m pytest -v
```

`PGPASSWORD` has to be non-empty even against a `trust` server, because
the tools require `--password` to be supplied.

CI runs this on every push against PostgreSQL 14 through 19 service
containers, plus `ruff format --check`, `ruff check`, and a package
build check. See [`.github/workflows/ci.yml`](../.github/workflows/ci.yml).

## Layout

| File                              | What it covers                                                        |
|-----------------------------------|-----------------------------------------------------------------------|
| `test_common.py`                  | `ConnInfo`, `psql_query/exec`, `stream_subprocess_to_sink`, `stream_source_to_subprocess`, `_real_fd` / OS-direct fast-path |
| `test_cancel.py`                  | SIGINT-driven subprocess + connection cancellation, `tracked_connection` semantics |
| `test_snapshot.py`                | `export_snapshot`, snapshot-holder watchdog, dump-side progress monitor |
| `test_storage.py`                 | `LocalBlob`, `S3Blob` (via `botocore.stub.Stubber`), multipart upload, `localize`, scheme-aware path helpers |
| `test_pg_restore_stderr.py`       | Classification of `pg_restore` stderr: benign "already exists" triplets vs. real failures |
| `test_phase_orchestration.py`     | Database-level restore phases forward `--index-jobs` to pg_restore (regression guard for serial FK validation) |
| `test_parallel_backup.py`         | End-to-end dump + restore round-trip on local disk                    |
| `test_parallel_backup_s3.py`      | End-to-end dump + restore round-trip on S3 (via `moto.server`)        |

## The S3 connectivity script

`test_s3_connectivity.py` is a standalone script, not a unit test. It is
excluded from pytest discovery via `conftest.py::collect_ignore` and needs
real credentials and a real S3 endpoint. It reads `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, `S3_BUCKET`, and the optional endpoint/TLS/region
variables from the environment.

It is **not read-only**: it uploads, downloads and then deletes an object at
`S3_TEST_KEY` (default `backup-test/connectivity-test.txt`). An existing
object at that key is overwritten and then deleted, so point `S3_TEST_KEY`
at an unused key before running it against a bucket that holds real data.
