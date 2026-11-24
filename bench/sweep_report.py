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
    # Swept variants are keyed NAME@JOBS; a variant pinned with @JOBS keeps its
    # plain name and is shown as a single reference row.
    groups: dict[str, list[tuple[int, str]]] = {}
    for key, r in res.items():
        name = key.rsplit("@", 1)[0] if "@" in key else key
        jobs = r.get("jobs") or int(key.rsplit("@", 1)[1])  # older files lack "jobs"
        groups.setdefault(name, []).append((jobs, key))
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
        for name, members in sorted(groups.items()):
            members.sort()
            base_jobs, base_key = members[0]
            base = statistics.median(res[base_key][key])
            for j, k in members:
                vals = res[k][key]
                med = statistics.median(vals)
                if len(members) == 1:
                    speedup_text, eff_text = "-", "-"
                else:
                    speedup = base / med
                    speedup_text = f"{speedup:.2f}x"
                    eff_text = f"{speedup / (j / base_jobs):.0%}"
                print(
                    f"| {name} | {j} | {med:.1f}s | {min(vals):.1f}-{max(vals):.1f}s "
                    f"| {speedup_text} | {eff_text} | {size_mb / med:.0f} MB/s |"
                )
        print()


if __name__ == "__main__":
    main()
