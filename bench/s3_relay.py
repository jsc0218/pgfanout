#!/usr/bin/env python3
"""Client-side S3 upload benchmark: the cost of getting a dump into S3.

N workers each stream a file through pgfanout's real relay into its real S3
multipart writer and boto3 client. The network is replaced by a hook that
answers every request in-process, after reading each part body the way
urllib3 would when sending it. There is no PostgreSQL, no S3, no sockets and
no TLS, so what is measured is the work the tool and boto3 do per byte
(hashing, copying, wrapping the body) and nothing a store or the network adds.
That is where a regression such as boto3's ``aws-chunked`` trailer checksum
shows up (it made dumps with 8+ workers take 35-43% longer).

Server-side and network behaviour are out of scope by design: they are what
made an earlier version with a local HTTPS server too noisy to be useful (the
transport cost buried the signal).

It is a regression test, not a source of absolute numbers. The metric is CPU
seconds of the client process per GiB, which moves far less on a shared runner
than wall time. Wall-clock MB/s is shown for context only.

Usage (the orchestrator needs only the standard library; each variant's own
Python needs boto3, i.e. a pgfanout install):

    python3 bench/s3_relay.py \\
        --variant base=/tmp/venv-base/bin --variant pr=/tmp/venv-pr/bin \\
        --out s3-relay.json --report s3-relay.md --fail

Rounds of all variants are interleaved with the order rotated, and a warm-up
round is dropped. The exit status is 1 only when the PR is slower than the base
by more than --threshold and by more than --min-abs CPU seconds per GiB, and
--fail was given.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import resource
import shutil
import statistics
import subprocess
import sys
import tempfile
import threading
import time

GIB = 1 << 30
MIB = 1 << 20
BUCKET = "bench"
READ_SIZE = 16384  # what urllib3 reads from a request body per send


# --------------------------------------------------------------------------
# Client: one timed run, executed with a variant's own Python
# --------------------------------------------------------------------------


def client(args) -> None:
    import boto3
    from botocore.awsrequest import AWSResponse

    from pgfanout import common
    from pgfanout.storage import S3Blob

    class _Body:
        def __init__(self, content: bytes = b""):
            self.content = content

        def stream(self, *a, **k):
            return iter([self.content])

    def answer(request, **_):
        """Stand in for the network: read the body the way urllib3 would, then reply."""
        body = request.body
        if hasattr(body, "read"):
            while body.read(READ_SIZE):
                pass
        query = request.url.partition("?")[2]
        if request.method == "POST" and "uploads" in query:
            xml = (
                "<InitiateMultipartUploadResult><Bucket>b</Bucket><Key>k</Key>"
                "<UploadId>bench</UploadId></InitiateMultipartUploadResult>"
            )
        elif request.method == "POST":
            xml = (
                "<CompleteMultipartUploadResult><Bucket>b</Bucket><Key>k</Key>"
                "<ETag>&quot;done&quot;</ETag></CompleteMultipartUploadResult>"
            )
        else:
            xml = ""
        return AWSResponse(request.url, 200, {"ETag": '"part"'}, _Body(xml.encode()))

    # https, because that is where boto3 wraps each part for its trailing checksum.
    s3 = boto3.client(
        "s3",
        region_name="us-east-1",
        endpoint_url="https://s3.bench.invalid",
        aws_access_key_id="bench",
        aws_secret_access_key="bench",
    )
    s3.meta.events.register("before-send.s3", answer)
    errors: list[BaseException] = []

    def work(i: int) -> None:
        try:
            blob = S3Blob(BUCKET, f"o{i}", client=s3, part_size=16 * MIB, upload_threads=2)
            with blob.open_write() as sink:
                common.stream_subprocess_to_sink(["cat", args.source], sink, chunk_size=64 * 1024)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=work, args=(i,)) for i in range(args.workers)]
    cpu0, t0 = resource.getrusage(resource.RUSAGE_SELF), time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    wall = time.perf_counter() - t0
    cpu1 = resource.getrusage(resource.RUSAGE_SELF)
    if errors:
        raise errors[0]
    gib = args.workers * os.path.getsize(args.source) / GIB
    cpu = (cpu1.ru_utime - cpu0.ru_utime) + (cpu1.ru_stime - cpu0.ru_stime)
    print(json.dumps({"cpu_s_per_gib": cpu / gib, "mb_per_s": gib * GIB / 1e6 / wall}))


# --------------------------------------------------------------------------
# Orchestrator
# --------------------------------------------------------------------------


def parse_variant(text: str) -> tuple[str, str]:
    name, sep, path = text.partition("=")
    if not sep or not name or not path:
        raise argparse.ArgumentTypeError("--variant must look like NAME=BIN_DIR")
    python = os.path.join(path, "python")
    if not os.access(python, os.X_OK):
        raise argparse.ArgumentTypeError(f"no python in {path}")
    return name, python


def pgfanout_version(python: str) -> str:
    out = subprocess.run(
        [python, "-c", "import importlib.metadata as m; print(m.version('pgfanout'))"],
        capture_output=True,
        text=True,
    )
    return out.stdout.strip() or "unknown"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--client", action="store_true", help=argparse.SUPPRESS)
    ap.add_argument("--source", help=argparse.SUPPRESS)
    ap.add_argument("--variant", action="append", type=parse_variant, default=[])
    ap.add_argument("--base", default="base", help="variant name of the baseline build")
    ap.add_argument("--pr", default="pr", help="variant name of the build under test")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--mb-per-worker", type=int, default=256)
    ap.add_argument("--repeats", type=int, default=5, help="measured rounds per variant")
    ap.add_argument("--threshold", type=float, default=0.05, help="allowed increase (0.05 = 5%%)")
    ap.add_argument("--min-abs", type=float, default=0.1, help="ignore changes under N CPU-s/GiB")
    ap.add_argument("--out", default="s3-relay.json")
    ap.add_argument("--report", default=None, help="also write the Markdown here")
    ap.add_argument("--fail", action="store_true", help="exit 1 on regression")
    args = ap.parse_args()

    if args.client:
        return client(args)

    variants = dict(args.variant)
    for name in (args.base, args.pr):
        if name not in variants:
            sys.exit(f"variant {name!r} not given (have: {', '.join(variants) or 'none'})")

    workdir = tempfile.mkdtemp(prefix="s3relay-run-")
    source = os.path.join(workdir, "source.bin")
    block = os.urandom(MIB)
    with open(source, "wb") as f:
        for _ in range(args.mb_per_worker):
            f.write(block)

    try:
        env = {**os.environ, "AWS_EC2_METADATA_DISABLED": "true"}

        def run_one(name: str) -> dict:
            out = subprocess.run(
                [
                    variants[name], os.path.abspath(__file__), "--client",
                    "--source", source, "--workers", str(args.workers),
                ],
                capture_output=True,
                text=True,
                env=env,
            )  # fmt: skip
            if out.returncode:
                sys.exit(f"{name}: client failed\n{out.stderr}")
            return json.loads(out.stdout.strip().splitlines()[-1])

        names = list(variants)
        results: dict[str, dict[str, list[float]]] = {
            n: {"cpu_s_per_gib": [], "mb_per_s": []} for n in names
        }
        for rnd in range(args.repeats + 1):  # round 0 is the warm-up
            order = names[rnd % len(names) :] + names[: rnd % len(names)]
            for name in order:
                r = run_one(name)
                if rnd:
                    for k, v in r.items():
                        results[name][k].append(v)
                print(f"round {rnd} {name}: {r['cpu_s_per_gib']:.2f} CPU-s/GiB, "
                      f"{r['mb_per_s']:.0f} MB/s" + ("  (warm-up)" if not rnd else ""),
                      file=sys.stderr, flush=True)  # fmt: skip
    finally:
        shutil.rmtree(workdir, ignore_errors=True)

    meta = {
        "workers": args.workers,
        "mb_per_worker": args.mb_per_worker,
        "repeats": args.repeats,
        "cpus": os.cpu_count(),
        "platform": platform.platform(),
        "versions": {n: pgfanout_version(p) for n, p in variants.items()},
    }
    with open(args.out, "w") as f:
        json.dump({"meta": meta, "results": results}, f, indent=2)

    base = statistics.median(results[args.base]["cpu_s_per_gib"])
    pr = statistics.median(results[args.pr]["cpu_s_per_gib"])
    delta = (pr - base) / base if base else 0.0
    bad = delta > args.threshold and (pr - base) > args.min_abs
    base_mbs = statistics.median(results[args.base]["mb_per_s"])
    pr_mbs = statistics.median(results[args.pr]["mb_per_s"])
    spread = lambda xs: f"{min(xs):.2f}-{max(xs):.2f}"  # noqa: E731
    lines = [
        "### pgfanout S3 upload (client side)\n",
        f"{args.workers} workers x {args.mb_per_worker} MiB through the real relay and S3 writer "
        f"with the network replaced by an in-process stub, median of {args.repeats} interleaved "
        f"rounds, {meta['cpus']} logical CPUs. Lower CPU per GiB is better.\n",
        "| | base | PR | change | verdict |",
        "|---|---|---|---|---|",
        f"| client CPU, s per GiB | {base:.2f} ({spread(results[args.base]['cpu_s_per_gib'])}) "
        f"| {pr:.2f} ({spread(results[args.pr]['cpu_s_per_gib'])}) | {delta:+.1%} "
        f"| {'regression' if bad else 'ok'} |",
        f"| throughput, MB/s (context) | {base_mbs:.0f} | {pr_mbs:.0f} "
        f"| {(pr_mbs - base_mbs) / base_mbs:+.1%} | - |",
        "",
    ]
    if bad:
        lines.append(
            f"**Possible regression: client CPU per GiB {delta:+.0%} "
            f"(over {args.threshold:.0%} and {args.min_abs} CPU-s/GiB).** "
            "Re-run before trusting a single result."
        )
    else:
        lines.append(f"No regression over {args.threshold:.0%} (and {args.min_abs} CPU-s/GiB).")
    text = "\n".join(lines) + "\n"
    print(text)
    if args.report:
        with open(args.report, "w") as f:
            f.write(text)
    if bad and args.fail:
        sys.exit(1)


if __name__ == "__main__":
    main()
