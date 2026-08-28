"""Unit tests for src/common.py helpers.

Pure-Python tests — no live Postgres required.
"""

import argparse
import io
import subprocess
import tempfile

import pytest

from common import (
    ConnInfo,
    _active_procs,
    _format_psql_row,
    _format_psql_value,
    _real_fd,
    apply_config_defaults,
    stream_source_to_subprocess,
    stream_subprocess_to_sink,
)


def test_conninfo_contains_all_fields():
    conn = ConnInfo(
        host="db.example.com",
        port="5433",
        user="backup_user",
        password="s3cr3t",
        database="mydb",
    )
    s = conn.conninfo()
    assert "host=db.example.com" in s
    assert "port=5433" in s
    assert "user=backup_user" in s
    assert "password=s3cr3t" in s
    assert "dbname=mydb" in s


def test_conninfo_is_space_separated_keyvalue():
    conn = ConnInfo(
        host="h",
        port="5432",
        user="u",
        password="p",
        database="d",
    )
    parts = conn.conninfo().split()
    pairs = dict(p.split("=", 1) for p in parts)
    assert pairs == {
        "host": "h",
        "port": "5432",
        "user": "u",
        "password": "p",
        "dbname": "d",
    }


def test_conninfo_quotes_password_with_space():
    """Auto-generated passwords can contain spaces, quotes, or backslashes.

    Naive f-string interpolation would produce ``password=a b`` which the
    libpq parser reads as ``password=a`` followed by an unparsable ``b``.
    ``make_conninfo`` must wrap such values in single quotes — any
    regression here silently breaks connections for a non-trivial fraction
    of real-world credentials.
    """
    import psycopg.conninfo

    conn = ConnInfo(
        host="h",
        port="5432",
        user="u",
        password="p ass'w\\rd",
        database="d",
    )
    s = conn.conninfo()
    # psycopg's own parser must round-trip our output.
    parsed = psycopg.conninfo.conninfo_to_dict(s)
    assert parsed["password"] == "p ass'w\\rd"
    assert parsed["host"] == "h"
    assert parsed["dbname"] == "d"


def test_env_injects_pgpassword():
    conn = ConnInfo(
        host="h",
        port="5432",
        user="u",
        password="pw",
        database="d",
    )
    env = conn.env()
    assert env["PGPASSWORD"] == "pw"


def test_psql_args_unchanged():
    """psql_args() must keep emitting the exact flags every site relies on."""
    conn = ConnInfo(
        host="h",
        port="5432",
        user="u",
        password="pw",
        database="d",
    )
    assert conn.psql_args() == [
        "psql",
        "-h",
        "h",
        "-p",
        "5432",
        "-U",
        "u",
        "-d",
        "d",
        "-X",
    ]


# ---------------------------------------------------------------------------
# psql -t -A output-format compatibility
#
# psql_query/psql_exec were migrated off subprocess-wrapped ``psql -t -A -c``
# onto psycopg.  The helpers below preserve the exact ``value|value`` wire
# format dozens of callers parse with string splits, so a regression here
# would break catalog discovery across the whole codebase.
# ---------------------------------------------------------------------------


def test_format_value_none_becomes_empty():
    assert _format_psql_value(None) == ""


def test_format_value_bool_true_becomes_t():
    assert _format_psql_value(True) == "t"


def test_format_value_bool_false_becomes_f():
    assert _format_psql_value(False) == "f"


def test_format_value_int_stringified():
    assert _format_psql_value(42) == "42"


def test_format_value_string_stringified():
    assert _format_psql_value("public") == "public"


def test_format_row_uses_pipe_separator():
    """psql -A uses | as the unaligned field separator."""
    assert _format_psql_row(("public", "my_table")) == "public|my_table"


def test_format_row_mixes_types():
    """Typical dump_sequences row: last_value (bigint), is_called (bool)."""
    assert _format_psql_row((100, True)) == "100|t"
    assert _format_psql_row((0, False)) == "0|f"


def test_format_row_handles_nulls_and_bools():
    """Exercises the full -t -A contract for a standalone-sequence DDL row."""
    row = ("public", "seq1", "bigint", 1, 1, 9223372036854775807, 1, 1, False)
    assert _format_psql_row(row) == ("public|seq1|bigint|1|1|9223372036854775807|1|1|f")


# ---------------------------------------------------------------------------
# stream_subprocess_to_sink
#
# Pipes a subprocess's stdout into a binary sink chunk-by-chunk.  All tests
# use small shell commands so they stay fully hermetic (no Postgres, no
# network) yet still cover the pipe-buffer, stderr-drain, and cancellation
# corners that a real pg_dump/psql invocation exercises in production.
# ---------------------------------------------------------------------------


def test_stream_subprocess_writes_stdout_to_sink():
    """Happy path: printf's bytes land verbatim in the sink."""
    sink = io.BytesIO()
    stream_subprocess_to_sink(["printf", "hello world"], sink, label="printf-test")
    assert sink.getvalue() == b"hello world"


def test_stream_subprocess_preserves_binary_bytes():
    """Binary payload (pg_dump -Fc simulation) round-trips unchanged.

    Uses ``dd`` to emit 100 KiB from /dev/urandom-style entropy via
    ``printf`` + ``yes`` so no text-mode decoding can mangle bytes.
    """
    sink = io.BytesIO()
    # Generate a deterministic 70-KiB payload containing all byte values
    # (0x00..0xFF) so any encoding round-trip corruption is detectable.
    payload_script = "import sys; sys.stdout.buffer.write(bytes(range(256)) * 280)"
    stream_subprocess_to_sink(["python3", "-c", payload_script], sink, chunk_size=4096)
    expected = bytes(range(256)) * 280
    assert sink.getvalue() == expected
    assert len(sink.getvalue()) == 256 * 280  # 71_680 bytes


def test_stream_subprocess_chunks_larger_than_default():
    """Stress test: 1 MiB stdout exercises the multi-chunk read loop."""
    sink = io.BytesIO()
    payload_script = "import sys; sys.stdout.buffer.write(b'A' * (1024 * 1024))"
    stream_subprocess_to_sink(["python3", "-c", payload_script], sink)
    assert sink.getvalue() == b"A" * (1024 * 1024)


def test_stream_subprocess_raises_on_nonzero_exit():
    """Non-zero exit codes surface as CalledProcessError with stderr."""
    sink = io.BytesIO()
    with pytest.raises(subprocess.CalledProcessError) as exc_info:
        stream_subprocess_to_sink(
            ["sh", "-c", "echo boom >&2; exit 3"],
            sink,
            label="failing-command",
        )
    err = exc_info.value
    assert err.returncode == 3
    # stderr was captured and attached so callers can log it.
    assert "boom" in (err.stderr or "")


def test_stream_subprocess_captures_large_stderr_without_deadlock():
    """Regression test: stderr >64 KiB (pipe-buffer size) must not deadlock.

    pg_dump can spew many warnings on a large schema; if we forgot to
    drain stderr on its own thread, the subprocess would block writing
    to stderr while we block reading from stdout — classic deadlock.
    """
    sink = io.BytesIO()
    script = (
        "import sys; "
        "sys.stderr.write('x' * 200_000); "
        "sys.stderr.flush(); "
        "sys.stdout.buffer.write(b'ok')"
    )
    stream_subprocess_to_sink(["python3", "-c", script], sink)
    assert sink.getvalue() == b"ok"


def test_stream_subprocess_unregisters_on_success():
    """The subprocess is removed from the cancel registry once it exits."""
    before = len(_active_procs)
    sink = io.BytesIO()
    stream_subprocess_to_sink(["true"], sink)
    assert len(_active_procs) == before


def test_stream_subprocess_unregisters_on_failure():
    """Even when the command fails, the registry is cleaned up."""
    before = len(_active_procs)
    sink = io.BytesIO()
    with pytest.raises(subprocess.CalledProcessError):
        stream_subprocess_to_sink(["false"], sink)
    assert len(_active_procs) == before


def test_stream_subprocess_propagates_sink_exceptions():
    """If the sink raises mid-stream, we kill the subprocess and re-raise.

    Simulates an S3 multipart upload failing partway through.  The
    helper's contract is: the exception bubbles up unchanged and the
    subprocess does not leak.
    """

    class ExplodingSink:
        def __init__(self) -> None:
            self.calls = 0

        def write(self, data: bytes) -> int:
            self.calls += 1
            if self.calls >= 2:
                raise RuntimeError("sink exploded")
            return len(data)

    before = len(_active_procs)
    sink = ExplodingSink()
    # Produce enough output (>chunk_size bytes) that at least two writes
    # are attempted — the second raises.
    script = (
        "import sys, time; "
        "sys.stdout.buffer.write(b'A' * 10000); "
        "sys.stdout.flush(); "
        "time.sleep(0.5); "
        "sys.stdout.buffer.write(b'B' * 10000)"
    )
    with pytest.raises(RuntimeError, match="sink exploded"):
        stream_subprocess_to_sink(["python3", "-c", script], sink, chunk_size=1024)
    assert len(_active_procs) == before


def test_stream_subprocess_bytes_callback_receives_cumulative_totals():
    """bytes_callback fires after every sink write with a monotonic total.

    Parallel-dump workers rely on this to feed
    ``progress_monitor``'s shared counter.  The test drives a 64 KiB
    payload through a tiny chunk size so several callbacks fire, and
    asserts the stream is monotonic non-decreasing and lands exactly
    on the total payload size.
    """
    sink = io.BytesIO()
    totals: list[int] = []
    payload_size = 64 * 1024
    script = f"import sys; sys.stdout.buffer.write(b'A' * {payload_size})"
    stream_subprocess_to_sink(
        ["python3", "-c", script],
        sink,
        chunk_size=4096,
        bytes_callback=totals.append,
    )
    assert sink.getvalue() == b"A" * payload_size
    # Multiple chunks expected for 64 KiB payload at 4 KiB chunk size.
    assert len(totals) >= 2
    # Each step must be >= the previous (cumulative, never resets).
    for prev, nxt in zip(totals, totals[1:], strict=False):
        assert nxt >= prev
    # Final value equals the total bytes streamed.
    assert totals[-1] == payload_size


def test_stream_subprocess_without_callback_does_not_crash():
    """bytes_callback is optional — the fast path must not touch it."""
    sink = io.BytesIO()
    stream_subprocess_to_sink(["printf", "no-callback"], sink)
    assert sink.getvalue() == b"no-callback"


# ---------------------------------------------------------------------------
# stream_source_to_subprocess (restore-side helper, reverse direction)
# ---------------------------------------------------------------------------


def test_stream_source_pumps_bytes_into_stdin_and_returns_on_eof():
    """Happy path: bytes from source land verbatim in the subprocess stdin.

    Uses ``cat`` (bit-exact echo of stdin to stdout) with its stdout
    redirected to a temp file so we can verify what the subprocess
    actually received.
    """
    import tempfile

    payload = b"hello stdin pump"
    source = io.BytesIO(payload)
    with tempfile.NamedTemporaryFile() as out:
        stream_source_to_subprocess(
            ["sh", "-c", f"cat > {out.name}"],
            source,
            label="cat-test",
        )
        out.seek(0)
        assert out.read() == payload


def test_stream_source_preserves_binary_bytes():
    """All 256 byte values round-trip unchanged — no text-mode corruption.

    Critical for binary-format COPY: a single byte flipped anywhere in
    a 100 GiB part would make the whole load fail mid-stream.
    """
    import tempfile

    payload = bytes(range(256)) * 512  # 128 KiB
    source = io.BytesIO(payload)
    with tempfile.NamedTemporaryFile() as out:
        stream_source_to_subprocess(
            ["sh", "-c", f"cat > {out.name}"],
            source,
            chunk_size=8192,
            label="binary-roundtrip",
        )
        out.seek(0)
        assert out.read() == payload


def test_stream_source_raises_on_nonzero_exit_with_stderr():
    """Non-zero exits become CalledProcessError carrying stderr.

    Simulates ``psql`` aborting under ``ON_ERROR_STOP=1`` after a bad
    row — the caller must see the server-side error to log it.
    """
    source = io.BytesIO(b"ignored")
    with pytest.raises(subprocess.CalledProcessError) as exc_info:
        stream_source_to_subprocess(
            ["sh", "-c", "cat >/dev/null; echo boom >&2; exit 5"],
            source,
            label="failing-cat",
        )
    assert exc_info.value.returncode == 5
    assert "boom" in (exc_info.value.stderr or "")


def test_stream_source_broken_pipe_surfaces_as_called_process_error():
    """When the subprocess dies before reading all input, we don't raise
    ``BrokenPipeError`` at the caller; we translate that into the
    normal non-zero-exit ``CalledProcessError`` so the restore worker's
    single exception handler covers this case too.
    """
    # The subprocess exits immediately (status 9) without reading its
    # stdin.  A large payload forces us to hit the write buffer limit
    # before stdin is closed, producing a SIGPIPE -> BrokenPipeError.
    source = io.BytesIO(b"X" * (4 * 1024 * 1024))  # 4 MiB
    with pytest.raises(subprocess.CalledProcessError) as exc_info:
        stream_source_to_subprocess(
            ["sh", "-c", "exit 9"],
            source,
            chunk_size=64 * 1024,
            label="early-exit",
        )
    assert exc_info.value.returncode == 9


def test_stream_source_unregisters_subprocess_on_success():
    """Cancel registry must not leak across successful runs."""
    before = len(_active_procs)
    source = io.BytesIO(b"payload")
    stream_source_to_subprocess(["cat"], source, label="ok")
    assert len(_active_procs) == before


def test_stream_source_unregisters_subprocess_on_failure():
    """Cancel registry must not leak even when the command fails."""
    before = len(_active_procs)
    source = io.BytesIO(b"ignored")
    with pytest.raises(subprocess.CalledProcessError):
        stream_source_to_subprocess(["sh", "-c", "exit 1"], source, label="fail")
    assert len(_active_procs) == before


def test_stream_source_bytes_callback_receives_cumulative_totals():
    """Symmetric to the sink-direction callback — same monotonicity
    guarantees.  Exposed for symmetry / future instrumentation; the
    restore progress monitor itself queries pg_stat_progress_copy.
    """
    import tempfile

    payload_size = 64 * 1024
    source = io.BytesIO(b"B" * payload_size)
    totals: list[int] = []
    with tempfile.NamedTemporaryFile() as out:
        stream_source_to_subprocess(
            ["sh", "-c", f"cat > {out.name}"],
            source,
            chunk_size=4096,
            bytes_callback=totals.append,
            label="cb-test",
        )
    assert len(totals) >= 2
    for prev, nxt in zip(totals, totals[1:], strict=False):
        assert nxt >= prev
    assert totals[-1] == payload_size


def test_stream_source_drains_stderr_without_deadlock():
    """If the subprocess spams stderr beyond the pipe buffer, the drain
    thread must keep pulling so the stdin writer doesn't block on a
    full stderr pipe deadlock (classic two-pipe symmetric hang).
    """
    source = io.BytesIO(b"input")
    # Write >64 KiB to stderr while also consuming stdin; exit 0.
    script = (
        "import sys; sys.stderr.write('E' * 200_000); sys.stderr.flush(); sys.stdin.buffer.read()"
    )
    stream_source_to_subprocess(
        ["python3", "-c", script],
        source,
        label="noisy-stderr",
    )


# ---------------------------------------------------------------------------
# OS-direct fast-path coverage
#
# When the sink (or source) exposes a real OS fd, the streaming helpers
# hand it directly to subprocess.Popen as ``stdout=`` / ``stdin=``.  The
# subprocess writes (or reads) via dup2'd fd 1 / fd 0 — no Python in the
# data path at all.  These tests pin down both halves of the dispatch
# so a future refactor can't silently regress into the slower Python
# read/write loop without anyone noticing, and exercise the surrounding
# protections (cancel registry, error surface, progress watcher).
# ---------------------------------------------------------------------------


def test_real_fd_returns_none_for_python_streams():
    """``io.BytesIO`` has fileno() but it raises UnsupportedOperation."""
    assert _real_fd(io.BytesIO()) is None
    assert _real_fd(io.BytesIO(b"x")) is None


def test_real_fd_returns_none_for_objects_without_fileno():
    """Plain objects without fileno fall through to the Python fallback.

    Mirrors what _S3MultipartWriter / _S3StreamReader look like to the
    streaming helpers — Python-implemented, no OS fd.
    """

    class NoFd:
        def write(self, data: bytes) -> int:
            return len(data)

    assert _real_fd(NoFd()) is None


def test_real_fd_returns_real_fd_for_open_file(tmp_path):
    """A regular open() handle exposes its OS fd for the OS-direct path."""
    p = tmp_path / "scratch"
    with open(p, "wb") as f:
        fd = _real_fd(f)
        assert isinstance(fd, int)
        assert fd >= 0
        assert fd == f.fileno()


def test_stream_subprocess_os_direct_writes_full_payload(tmp_path):
    """Real-file sink → OS-direct path → bytes land verbatim.

    Drives a 1 MiB binary payload through python3 -c into a regular
    file.  The Python fallback never runs (sink fileno() yields a real
    fd) so this exercises the dup2'd-stdout fast path end-to-end.
    """
    out_path = tmp_path / "os_direct_out.bin"
    expected = bytes(range(256)) * 4096  # 1 MiB, every byte value present
    script = "import sys; sys.stdout.buffer.write(bytes(range(256)) * 4096)"
    with open(out_path, "wb") as sink:
        stream_subprocess_to_sink(["python3", "-c", script], sink, label="os-direct-sink")
    assert out_path.read_bytes() == expected


def test_stream_subprocess_os_direct_callback_final_total_matches(tmp_path):
    """OS-direct path: live samples lag by the watcher interval but the
    *final* callback carries the exact byte count.

    Parallel-dump's _log_dump_summary depends on this so the per-part
    "ok / N bytes" record reflects the real on-disk size.  The watcher
    fires once per second during streaming; the helper additionally
    triggers one trailing callback after proc.wait() so the final value
    is correct regardless of when the last watcher tick happened.
    """
    out_path = tmp_path / "os_direct_cb.bin"
    payload_size = 1 << 20  # 1 MiB
    script = f"import sys; sys.stdout.buffer.write(b'A' * {payload_size})"
    totals: list[int] = []
    with open(out_path, "wb") as sink:
        stream_subprocess_to_sink(
            ["python3", "-c", script],
            sink,
            label="os-direct-cb",
            bytes_callback=totals.append,
        )
    assert totals, "expected at least one progress callback"
    assert totals[-1] == payload_size
    # Monotonic non-decreasing — the streaming counter must never
    # regress even when several watcher samples land within one sec.
    for prev, nxt in zip(totals, totals[1:], strict=False):
        assert nxt >= prev


def test_stream_subprocess_os_direct_unregisters_subprocess(tmp_path):
    """Cancellation registry stays clean on the OS-direct path.

    Regression guard: the OS-direct branch has its own Popen call, so
    register/unregister logic has to cover it too — a refactor that
    forgets to remove the proc on success/failure would leak entries.
    """
    out_path = tmp_path / "os_direct_reg.bin"
    before = len(_active_procs)
    with open(out_path, "wb") as sink:
        stream_subprocess_to_sink(["printf", "hello-os-direct"], sink)
    assert len(_active_procs) == before
    assert out_path.read_bytes() == b"hello-os-direct"


def test_stream_subprocess_os_direct_propagates_nonzero_exit(tmp_path):
    """OS-direct path still surfaces subprocess errors with stderr."""
    out_path = tmp_path / "os_direct_fail.bin"
    with open(out_path, "wb") as sink:
        with pytest.raises(subprocess.CalledProcessError) as exc_info:
            stream_subprocess_to_sink(
                ["sh", "-c", "echo os-direct-boom >&2; exit 7"],
                sink,
                label="os-direct-fail",
            )
    assert exc_info.value.returncode == 7
    assert "os-direct-boom" in (exc_info.value.stderr or "")


def test_stream_source_os_direct_pumps_real_file_into_stdin(tmp_path):
    """Real-file source → OS-direct path → subprocess receives every byte."""
    src_path = tmp_path / "os_direct_src.bin"
    out_path = tmp_path / "os_direct_sink.bin"
    expected = bytes(range(256)) * 4096  # 1 MiB
    src_path.write_bytes(expected)
    with open(src_path, "rb") as source:
        stream_source_to_subprocess(
            ["sh", "-c", f"cat > {out_path}"],
            source,
            label="os-direct-source",
        )
    assert out_path.read_bytes() == expected


def test_stream_source_os_direct_callback_reports_consumed_bytes(tmp_path):
    """OS-direct restore reports bytes the subprocess actually consumed.

    The parent's source fd shares the open file description with the
    child's dup'd stdin, so as the subprocess reads the parent's offset
    advances too.  ``lseek(SEEK_CUR)`` after proc.wait() returns the
    cumulative consumed count — exact even when the subprocess aborts
    early without reading the whole source.
    """
    src_path = tmp_path / "os_direct_src_cb.bin"
    payload_size = 1 << 20
    src_path.write_bytes(b"S" * payload_size)
    totals: list[int] = []
    with open(src_path, "rb") as source:
        with tempfile.NamedTemporaryFile() as out:
            stream_source_to_subprocess(
                ["sh", "-c", f"cat > {out.name}"],
                source,
                bytes_callback=totals.append,
                label="os-direct-src-cb",
            )
    assert totals, "expected at least one progress callback"
    assert totals[-1] == payload_size


def test_stream_source_os_direct_handles_consumer_dying_early(tmp_path):
    """OS-direct path: subprocess dying mid-stream still surfaces as
    CalledProcessError, no BrokenPipeError leaks to the caller.

    Subprocess exits 9 without reading anything; the source file is
    8 MiB so it would overflow the pipe buffer if writes were happening
    in our process — but with OS-direct redirection the kernel handles
    the entire read pipeline, so an early child exit just leaves the
    file partially read.
    """
    src_path = tmp_path / "os_direct_src_brk.bin"
    src_path.write_bytes(b"X" * (8 << 20))  # 8 MiB — bigger than pipe buffer
    with open(src_path, "rb") as source:
        with pytest.raises(subprocess.CalledProcessError) as exc_info:
            stream_source_to_subprocess(
                ["sh", "-c", "exit 9"],
                source,
                label="os-direct-brokenpipe",
            )
    assert exc_info.value.returncode == 9


# ---------------------------------------------------------------------------
# apply_config_defaults: precedence, password handling, typo guard
# ---------------------------------------------------------------------------


def _dump_args(**overrides):
    """Build a Namespace shaped like dump CLI flags (argparse defaults None)."""
    base = dict(
        host=None,
        port=None,
        user=None,
        password=None,
        database=None,
        num_parts=None,
        output_dir=None,
        retries=None,
    )
    base.update(overrides)
    return argparse.Namespace(**base)


def test_apply_config_defaults_fills_none_args():
    """Config values fill in args left as ``None`` by argparse."""
    args = _dump_args()
    cfg = {
        "connection": {"host": "db.example.com", "user": "u", "database": "mydb"},
        "dump": {"num_parts": 16, "output_dir": "/data/out"},
    }
    apply_config_defaults(args, cfg, "dump")

    assert args.host == "db.example.com"
    assert args.user == "u"
    assert args.database == "mydb"
    assert args.num_parts == 16
    assert args.output_dir == "/data/out"


def test_apply_config_defaults_cli_wins_over_config():
    """CLI args (already-set, non-None) are never overwritten by config."""
    args = _dump_args(host="cli.host", num_parts=128)
    cfg = {
        "connection": {"host": "config.host"},
        "dump": {"num_parts": 8},
    }
    apply_config_defaults(args, cfg, "dump")

    assert args.host == "cli.host"
    assert args.num_parts == 128


def test_apply_config_defaults_password_env_takes_precedence(monkeypatch):
    """``password_env`` reads from $ENV; literal ``password`` is fallback."""
    monkeypatch.setenv("MY_BACKUP_PW", "from-env")
    args = _dump_args()
    cfg = {"connection": {"password_env": "MY_BACKUP_PW", "password": "from-toml"}}
    apply_config_defaults(args, cfg, "dump")

    assert args.password == "from-env"


def test_apply_config_defaults_password_env_falls_back_to_literal(monkeypatch):
    """If $ENV is unset, fall back to the literal ``password`` key."""
    monkeypatch.delenv("UNSET_PW", raising=False)
    args = _dump_args()
    cfg = {"connection": {"password_env": "UNSET_PW", "password": "from-toml"}}
    apply_config_defaults(args, cfg, "dump")

    assert args.password == "from-toml"


def test_apply_config_defaults_warns_on_unknown_section_key(caplog, monkeypatch):
    """Regression guard: typoed config keys must not silently create new attrs.

    Without the ``hasattr`` guard, ``num_part = 128`` (missing 's') would
    silently set ``args.num_part = 128`` while ``args.num_parts`` kept its
    ``None`` default, leaving the user with a default-parallelism dump
    despite their config saying otherwise.  We require a warning to fire
    and the typoed key to be ignored.

    ``setup_logging`` sets ``LOG.propagate = False`` so the production
    binary's structured-log handler is the sole sink — but pytest's
    ``caplog`` listens on the *root* logger, so without re-enabling
    propagation here the record never reaches the capture buffer (even
    though it does fire and reach stderr — visible in pytest's
    "Captured stderr call" section).  Flip propagation on for the
    duration of this test only; ``monkeypatch`` restores the original
    value at teardown.
    """
    import logging as _logging

    monkeypatch.setattr(_logging.getLogger("backup"), "propagate", True)

    args = _dump_args()
    cfg = {"dump": {"num_part": 128}}  # typo!
    with caplog.at_level("WARNING", logger="backup"):
        apply_config_defaults(args, cfg, "dump")

    assert args.num_parts is None  # original default untouched
    assert not hasattr(args, "num_part") or args.num_part is None
    # Warning surface must mention the offending key + section
    msgs = "\n".join(r.message for r in caplog.records)
    assert "num_part" in msgs
    assert "dump" in msgs


def test_apply_config_defaults_section_keys_do_not_leak_across_sections():
    """``[restore]`` keys must not bleed into a dump invocation."""
    args = _dump_args()
    cfg = {
        "dump": {"num_parts": 8},
        "restore": {"index_jobs": 4},  # shouldn't reach a dump Namespace
    }
    apply_config_defaults(args, cfg, "dump")

    assert args.num_parts == 8
    assert not hasattr(args, "index_jobs")
