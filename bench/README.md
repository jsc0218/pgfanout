# Benchmarks

Tools for measuring pgfanout's dump and restore speed and for catching
performance regressions in pull requests.

- [`bench.py`](bench.py) generates a deterministic single large table (plain,
  no indexes, so it measures raw data dump and load speed rather than index
  builds) and, for each variant you give it, runs dump, restore and a
  row-count check. Variants are interleaved with the order rotated each
  round, and a warm-up round is discarded, so machine noise hits all of them
  equally.
- [`compare.py`](compare.py) turns the JSON into a Markdown table and a
  verdict.
- [`sweep_report.py`](sweep_report.py) summarizes a `--jobs-sweep` run: time,
  speedup and parallel efficiency at each job count.

## Run it

You need `psql`, `pg_dump` and `pg_restore` of the same major version as the
server on your `PATH`, and a **disposable** PostgreSQL server, because by
default the harness changes server-wide settings (see
[What the numbers mean](#what-the-numbers-mean)). That needs a superuser: it
reads `pg_file_settings`, runs `ALTER SYSTEM` and reloads the configuration,
and issues cluster-wide `CHECKPOINT`s. It turns `fsync` off for **every
database on the server**, and only when the server is on the local machine.
Do not point it at a shared or production server; use `--no-stabilize` there,
which only needs permission to create and drop databases.

```bash
export PGHOST=localhost PGPORT=5432 PGUSER=postgres PGPASSWORD=postgres
python3 bench/bench.py \
    --variant pr=pgfanout:.venv/bin \
    --variant pg_dump=pgdump \
    --size-mb 1024 --repeats 5 --out results.json
python3 bench/compare.py results.json --base pr --pr pr --reference pg_dump
```

A variant is `NAME=pgfanout:BINDIR` (the directory holding that build's
`pgfanout-dump` and `pgfanout-restore`) or `NAME=pgdump`, which runs
`pg_dump -Fd -Z 0 -j N` and `pg_restore -j N` (uncompressed, so the
comparison is like for like). Give `pgfanout:` once per build to compare two
builds, for example an installed release and your working tree, and pass
them to `compare.py` as `--base` and `--pr`. Run `python3 bench/bench.py
--help` for all options (`--size-mb`, `--repeats`, `--warmup`, `--jobs`).

## Scaling sweep

To see how each tool scales with the job count, run every variant at several
job counts in one interleaved run and summarize it:

```bash
python3 bench/bench.py \
    --variant pgfanout=pgfanout:.venv/bin --variant pg_dump=pgdump@1 \
    --size-mb 20000 --repeats 3 --jobs-sweep 1,2,4,8,16 --out sweep.json
python3 bench/sweep_report.py sweep.json
```

Results are keyed `NAME@JOBS`, and `--jobs` is ignored. A trailing `@N` on a
variant pins it to `N` jobs, so it runs once as a reference row instead of at
every count: `--variant pg_dump=pgdump@1` times `pg_dump` once with one job,
which is the useful baseline because `pg_dump -j` cannot split a single table. Efficiency is the
speedup divided by the growth in job count, so about 100% means it still
scales and a fall-off shows where the CPU, the server or the disk becomes the
limit. Use a dataset large enough that the fixed startup cost (about 0.1-0.7s)
is small next to the transfer, or nothing will appear to scale. The number of
runs is variants x job counts x (repeats + warm-up), so keep `--repeats` low on
a big dataset. On a machine where the server and the workers share CPUs, pin
them to separate cores (`taskset`) or the curve will flatten early.

## Benchmarking S3

`--storage s3://BUCKET/PREFIX` sends the dump to S3 (or an S3-compatible
store) and restores from it, instead of using local disk:

```bash
export AWS_REGION=us-east-1   # credentials: instance role or AWS_* variables
python3 bench/bench.py --variant pgfanout=pgfanout:.venv/bin \
    --storage s3://my-bucket/pgfanout-bench \
    --size-mb 20000 --repeats 3 --jobs-sweep 1,2,4,8,16 --out s3-sweep.json
python3 bench/sweep_report.py s3-sweep.json
```

- Run `bench.py` with the Python of a pgfanout install, because it needs
  `boto3`. The endpoint, region and credentials come from the same `AWS_*`
  variables pgfanout reads (see the README's S3 section).
- Only pgfanout variants work: `pg_dump` cannot write to S3.
- Every run writes under its own random `PREFIX/bench-<id>/` and deletes it
  afterwards, including unfinished multipart uploads, so it never touches
  other objects. The prefix is printed when the run starts; if a run is killed,
  delete it by hand. If the final cleanup fails the results are still saved, but
  the run exits with an error naming the prefix. Use an
  **unversioned** bucket: in a versioned one a delete only adds a marker and
  keeps the data, so the harness refuses it. `PREFIX` must not contain `?`, `#`
  or a leading `//`.
- Run it from an instance in the bucket's region, reaching S3 through a VPC
  gateway endpoint. Through a NAT gateway you pay per GB, and a large sweep
  moves hundreds of GB.
- Find the instance's raw S3 ceiling first (a plain parallel `aws s3 cp`),
  as you would check a disk with `fio`, so you know what pgfanout is being
  held against. The restore still writes the database and its WAL to the
  server's disk, so its ceiling is often that disk, not S3.
- S3 timings vary more than a local disk. Use at least 3 repeats and read the
  min-max column.

## S3 in CI

The Benchmark workflow also runs the dump and restore through S3: a local
versitygw (a POSIX-backed S3 gateway; pinned release, HTTPS with a self-signed
certificate, data on `/mnt`) stands in for S3, `bench.py --storage` runs the
base and the PR against it, and `compare.py` applies the same rules as for the
local run. HTTPS matters because boto3 only wraps each part in its
trailing-checksum encoding over TLS. It catches breakage and large slowdowns in
the S3 read and write paths, but the runner's 2 physical cores are shared by
Postgres, `psql`, pgfanout and the server, so a dump to S3 scales little beyond
2 jobs and a client-side cost can stay hidden. Use `--storage` on a dedicated
machine for that. versitygw was chosen over SeaweedFS because it needs about a
third of the CPU per dump, which leaves the runner's cores to the thing being
measured.

## One job against `pg_dump`

The local report's `vs pg_dump-j1` column is pgfanout at 2 jobs against
`pg_dump -Fd -Z 0 -j 1`; a higher `-j` does not help `pg_dump` on a single table
(7.3s at 1, 2 and 4 jobs in a sweep), so its job count hardly matters on this
dataset, and the one-job pgfanout row of the speedup table is the like-for-like
single-thread figure. They are not expected to take the same time, and the difference is not the
harness. `pg_dump` has no binary format for table data (its directory and custom
formats carry text COPY), whereas pgfanout copies in binary, which is cheaper to
produce and to load: on the CI runner a plain `psql` COPY of the table took 3.1s
as text and 2.5s as binary, and loading it took 3.9s as text and 2.6s as binary.
On top of that `pg_dump` itself needs 6.6s for what a plain text COPY does in
3.1s. One job of pgfanout (3.1s) therefore lands near a plain text COPY, and
`pg_restore` (4.1s) near a plain text load.

## Reference numbers

A scaling sweep of the code at commit `9722520`, to compare a future build
against when the CI job (which only runs 1 and 2 jobs) is not enough.

Machine: AWS c6id.8xlarge (32 vCPU, Intel Xeon Platinum 8375C, local NVMe
for the server and the dumps), Ubuntu with Linux 7.0, Python 3.14,
PostgreSQL 18.7 in a container on the same host, default stabilisation
(`fsync` off). The S3 runs used a bucket in the same region as the instance.
Each cell is the median of 3 interleaved rounds after a warm-up, in seconds,
with the minimum and maximum in brackets. Dump and restore use the same
number of workers (parts).

Local disk (`--size-mb 8000`: a 6.2 GB database, a 5.75 GB dump):

| workers | dump | restore |
|---|---|---|
| 1 | 20.1 (20.1-21.1) | 29.2 (29.0-30.1) |
| 2 | 10.4 (10.3-10.4) | 16.2 (16.1-16.2) |
| 4 | 5.4 (5.3-5.4) | 10.0 (10.0-10.0) |
| 8 | 3.9 (3.6-3.9) | 7.9 (7.7-7.9) |
| 16 | 2.8 (2.8-2.8) | 8.8 (8.8-8.8) |
| 32 | 2.9 (2.8-2.9) | 9.0 (8.9-9.1) |

S3 (`--size-mb 20000`: a 15.4 GB database, a 14.4 GB dump):

| workers | dump | restore |
|---|---|---|
| 1 | 82.5 (82.4-83.1) | 171.0 (161.8-173.8) |
| 2 | 42.5 (42.3-43.1) | 84.4 (79.6-85.0) |
| 4 | 22.4 (22.1-22.7) | 44.1 (40.9-44.5) |
| 8 | 15.0 (14.9-15.1) | 24.4 (24.4-25.6) |
| 16 | 16.2 (16.1-16.3) | 24.0 (23.6-30.7) |
| 32 | 17.5 (17.5-18.0) | 24.9 (24.7-25.0) |

Reading them:

- Compare a build against the same row, with the usual 5% and 0.2s rule.
  Local dump at 8 workers is the exception: it sits close to the limit of
  one Python process and flips between about 3.0s and 3.7s on the same code,
  so allow about 15% there (the median of 7 rounds was 3.5s).
- Nothing gets faster past 16 workers. Dump is limited by the one Python
  process (about 1 GB/s to S3), restore by PostgreSQL.
- Absolute times move by about 10% from one machine or day to the next, so
  rerun the previous build on the same machine in the same session before
  calling a regression from these numbers alone.
- The S3 restore rows predate the read-ahead of S3 parts (the restore of a
  part is one GET stream of about 100 MB/s); refresh them when that merges.

## What the numbers mean

By default the harness tunes the server for repeatability: it raises
`max_wal_size` and `checkpoint_timeout`, runs `CHECKPOINT` and `sync` before
each timed command, and turns `fsync` off (local servers only). All of it is
reset afterwards, to the settings that were there before, and the data
written while `fsync` was off is flushed to disk first, as PostgreSQL
requires when switching it back on. That makes the numbers good for
catching regressions between builds and **not suitable for publishing**,
because they flatter the disk-bound restore phase.

- `--keep-fsync` leaves `fsync` on (realistic disk behaviour, noisier).
- `--no-stabilize` leaves the server settings completely alone.

`compare.py` flags a build that is slower than the base by more than 5%
and by more than 0.2s (`--threshold` and `--min-abs`), using the median of
the measured rounds.

## In CI

The **Benchmark** workflow runs this on pull requests that touch `src/` or
`bench/` (the base branch against the PR, on the same runner) and on every
push to `main` that touches them (the new commit against the one before it).
It can also be started by hand from the Actions tab. It runs the dump and
restore to local disk and then through S3 (see above), at 1 and 2 jobs, and
fails the job when `compare.py` finds a regression at 2 jobs in either (a
manual run can turn that off with the `enforce` input). It never runs on a
schedule.
How it works, how stable it has been, and what it cannot detect (the runner
has 2 physical cores with hyper-threads, and the server shares them with the
clients) is documented at the top of
[`.github/workflows/bench.yml`](../.github/workflows/bench.yml).
