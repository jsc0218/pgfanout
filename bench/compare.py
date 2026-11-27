#!/usr/bin/env python3
"""Turn bench.py results into a Markdown report and a regression verdict.

Compares the median dump and restore times of two variants (usually the
base branch and the pull request) and flags the pull request when it is
slower than the base by more than --threshold. A change smaller than
--min-abs seconds is ignored, so tiny absolute differences on a fast
benchmark cannot trip the check. An optional reference variant (e.g.
pg_dump) is reported as a ratio for context and never gates.

The Markdown goes to stdout. The exit status is 1 only when a regression
was found and --fail was given.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys


def cpu_text(meta: dict) -> str:
    info = meta.get("cpu_info") or {}
    text = f"{meta['cpus']} logical CPUs"
    if info.get("physical_cores") and info.get("threads_per_core"):
        text += f" ({info['physical_cores']} cores x {info['threads_per_core']} threads"
        text += f", {info['model']})" if info.get("model") else ")"
    return text


def spread(values: list[float]) -> str:
    if len(values) < 2:
        return "n/a"
    return f"{min(values):.1f}-{max(values):.1f}s"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("results")
    ap.add_argument("--base", required=True, help="variant name of the baseline build")
    ap.add_argument("--pr", required=True, help="variant name of the build under test")
    ap.add_argument("--reference", default=None, help="variant name shown as a ratio only")
    ap.add_argument("--threshold", type=float, default=0.05, help="allowed slowdown (0.05 = 5%%)")
    ap.add_argument("--min-abs", type=float, default=0.2, help="ignore slowdowns under N seconds")
    ap.add_argument("--fail", action="store_true", help="exit 1 on regression")
    ap.add_argument("--title", default="pgfanout benchmark", help="heading of the report")
    ap.add_argument(
        "--single",
        default=None,
        help="variant name of the same build run with one job; shown as the speedup of "
        "the --pr build over it, never gates",
    )
    args = ap.parse_args()

    data = json.load(open(args.results))
    meta, res = data["meta"], data["results"]
    for name in (args.base, args.pr, *([args.single] if args.single else [])):
        if name not in res:
            sys.exit(f"variant {name!r} not in results (have: {', '.join(res)})")

    size_mb = meta["source_db_bytes"] / 1e6
    regressions: list[str] = []
    rows = []
    for phase, key in (("dump", "dump_s"), ("restore", "restore_s")):
        base = statistics.median(res[args.base][key])
        pr = statistics.median(res[args.pr][key])
        delta = (pr - base) / base if base else 0.0
        bad = delta > args.threshold and (pr - base) > args.min_abs
        if bad:
            regressions.append(f"{phase} {delta:+.0%}")
        ref = ""
        if args.reference and args.reference in res:
            ref_med = statistics.median(res[args.reference][key])
            ref = f"{ref_med / pr:.2f}x faster" if pr < ref_med else f"{pr / ref_med:.2f}x slower"
        rows.append(
            f"| {phase} | {base:.1f}s ({spread(res[args.base][key])}) "
            f"| {pr:.1f}s ({spread(res[args.pr][key])}) | {delta:+.1%} "
            f"| {size_mb / pr:.0f} MB/s | {ref or '-'} | {'regression' if bad else 'ok'} |"
        )

    ref_hdr = f"vs {args.reference}" if args.reference else "vs reference"
    print(f"### {args.title}\n")
    print(
        f"{size_mb:.0f} MB database, {meta['jobs']} jobs, median of {meta['repeats']} "
        f"interleaved runs, {cpu_text(meta)}, PostgreSQL {meta['server_version']}"
        f"{', fsync off' if meta.get('fsync_off') else ''}. "
        f"Threshold: {args.threshold:.0%} (and over {args.min_abs:g}s).\n"
    )
    print(f"| phase | {args.base} | {args.pr} | change | pr throughput | {ref_hdr} | verdict |")
    print("|---|---|---|---|---|---|---|")
    print("\n".join(rows))
    print()
    if regressions:
        print(f"**Possible regression:** {', '.join(regressions)}. Re-run to rule out noise.")
    else:
        print("No regression beyond the threshold.")

    if args.single:
        one, many = res[args.single], res[args.pr]
        n1, n = one.get("jobs", 1), many.get("jobs", meta["jobs"])
        print(f"\n**Speedup over one job** (the PR build, {n1} job and {n} jobs; context only)\n")
        print(f"| phase | {n1} job | {n} jobs | speedup | efficiency |")
        print("|---|---|---|---|---|")
        for phase, key in (("dump", "dump_s"), ("restore", "restore_s")):
            t1, tn = statistics.median(one[key]), statistics.median(many[key])
            sp = t1 / tn
            print(f"| {phase} | {t1:.1f}s | {tn:.1f}s | {sp:.2f}x | {sp / (n / n1):.0%} |")

    if regressions and args.fail:
        sys.exit(1)


if __name__ == "__main__":
    main()
