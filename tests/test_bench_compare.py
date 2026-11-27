"""bench/compare.py: the verdict that fails the Benchmark job, and its reports."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

COMPARE = Path(__file__).resolve().parent.parent / "bench" / "compare.py"


def _run(tmp_path, results, *args):
    path = tmp_path / "results.json"
    path.write_text(json.dumps(results))
    return subprocess.run(
        [sys.executable, str(COMPARE), str(path), *args], capture_output=True, text=True
    )


def _variant(dump, restore, jobs):
    return {"dump_s": dump, "restore_s": restore, "jobs": jobs}


@pytest.fixture
def results():
    meta = {
        "source_db_bytes": 800_000_000,
        "jobs": 2,
        "repeats": 3,
        "cpus": 4,
        "server_version": "17",
        "fsync_off": True,
        "cpu_info": {},
    }
    return {
        "meta": meta,
        "results": {
            "base@2": _variant([3.3, 3.4, 3.3], [2.4, 2.4, 2.5], 2),
            "pr@2": _variant([3.3, 3.4, 3.3], [2.4, 2.5, 2.4], 2),
            "pr@1": _variant([6.6, 6.5, 6.7], [4.9, 5.0, 5.1], 1),
            "pg_dump-j1": _variant([9.0, 9.0, 9.0], [8.0, 8.0, 8.0], 1),
        },
    }


def test_no_regression_exits_zero_even_with_fail(tmp_path, results):
    out = _run(tmp_path, results, "--base", "base@2", "--pr", "pr@2", "--fail")
    assert out.returncode == 0, out.stderr
    assert "No regression beyond the threshold." in out.stdout


def test_a_regression_fails_only_with_fail_and_names_the_phase(tmp_path, results):
    results["results"]["pr@2"]["dump_s"] = [3.9, 4.0, 3.9]  # +18%, and 0.6s over the base
    args = ("--base", "base@2", "--pr", "pr@2")
    flagged = _run(tmp_path, results, *args, "--fail")
    assert flagged.returncode == 1
    assert "Possible regression:** dump +18%" in flagged.stdout
    assert _run(tmp_path, results, *args).returncode == 0  # reported, not enforced


def test_a_small_absolute_slowdown_is_ignored(tmp_path, results):
    # +6% but only 0.15s: under the 0.2s floor, so it must not fail the job
    results["results"]["pr@2"]["restore_s"] = [2.55, 2.55, 2.55]
    results["results"]["base@2"]["restore_s"] = [2.4, 2.4, 2.4]
    out = _run(tmp_path, results, "--base", "base@2", "--pr", "pr@2", "--fail")
    assert out.returncode == 0, out.stdout


def test_single_adds_a_speedup_table_and_never_gates(tmp_path, results):
    out = _run(tmp_path, results, "--base", "base@2", "--pr", "pr@2", "--single", "pr@1", "--fail")
    assert out.returncode == 0
    assert "Speedup over one job" in out.stdout
    assert "| dump | 6.6s | 3.3s | 2.00x | 100% |" in out.stdout


def _table_header(stdout):
    return next(line for line in stdout.splitlines() if line.startswith("| phase | base"))


def test_reference_adds_a_column_only_when_given(tmp_path, results):
    base = ("--base", "base@2", "--pr", "pr@2")
    with_ref = _run(tmp_path, results, *base, "--reference", "pg_dump-j1")
    assert "vs pg_dump-j1" in _table_header(with_ref.stdout)
    assert "faster" in with_ref.stdout
    without = _run(tmp_path, results, *base)
    assert "vs " not in _table_header(without.stdout)


def test_title_names_the_report(tmp_path, results):
    out = _run(tmp_path, results, "--base", "base@2", "--pr", "pr@2", "--title", "S3 report")
    assert out.stdout.startswith("### S3 report")


@pytest.mark.parametrize("flag", ["--reference", "--single", "--base"])
def test_an_unknown_variant_is_an_error(tmp_path, results, flag):
    args = ["--base", "base@2", "--pr", "pr@2"]
    if flag == "--base":
        args[1] = "nope"
    else:
        args += [flag, "nope"]
    out = _run(tmp_path, results, *args)
    assert out.returncode != 0
    assert "not in results" in out.stderr
