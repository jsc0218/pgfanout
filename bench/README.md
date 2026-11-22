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

`compare.py` flags a build that is slower than the base by more than 10%
and by more than 0.2s (`--threshold` and `--min-abs`), using the median of
the measured rounds.

## In CI

The **Benchmark** workflow runs this on pull requests that touch `src/` or
`bench/`, comparing the base branch with the PR on the same runner. It is
advisory unless enforcement is turned on, and it never runs on a schedule.
How it works, how stable it has been, and what it cannot detect (the runner
has 2 physical cores with hyper-threads, and the server shares them with the
clients) is documented at the top of
[`.github/workflows/bench.yml`](../.github/workflows/bench.yml).
