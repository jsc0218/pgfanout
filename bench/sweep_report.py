#!/usr/bin/env python3
"""Summarize a bench.py --jobs-sweep run as Markdown tables.

For every variant and phase (dump, restore) it prints the median time at
each job count, the speedup over the smallest job count that was run, and
the parallel efficiency (speedup divided by the growth in job count).
Efficiency near 100% means the tool still scales; a fall-off shows where the
CPU, the server or the disk becomes the limit.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys

from compare import cpu_text


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("results")
    args = ap.parse_args()

    data = json.load(open(args.results))
    meta, res = data["meta"], data["results"]
    sweep = meta.get("jobs_sweep")
    if not sweep:
        sys.exit("not a sweep result: run bench.py with --jobs-sweep")
    names = sorted({k.rsplit("@", 1)[0] for k in res})
    jobs = sorted(sweep)
    size_mb = meta["source_db_bytes"] / 1e6

    print("### pgfanout scaling sweep\n")
    print(
        f"{size_mb:.0f} MB database, median of {meta['repeats']} interleaved runs, "
        f"{cpu_text(meta)}, PostgreSQL {meta['server_version']}"
        f"{', fsync off' if meta.get('fsync_off') else ''}.\n"
    )
    for phase, key in (("dump", "dump_s"), ("restore", "restore_s")):
        print(f"**{phase}**\n")
        print("| variant | jobs | median | min-max | speedup | efficiency | throughput |")
        print("|---|---|---|---|---|---|---|")
        for name in names:
            base_jobs = jobs[0]
            base = statistics.median(res[f"{name}@{base_jobs}"][key])
            for j in jobs:
                vals = res[f"{name}@{j}"][key]
                med = statistics.median(vals)
                speedup = base / med
                eff = speedup / (j / base_jobs)
                print(
                    f"| {name} | {j} | {med:.1f}s | {min(vals):.1f}-{max(vals):.1f}s "
                    f"| {speedup:.2f}x | {eff:.0%} | {size_mb / med:.0f} MB/s |"
                )
        print()


if __name__ == "__main__":
    main()
