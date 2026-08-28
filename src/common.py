"""Shared utilities for parallel_dump.py and parallel_restore.py.

Provides connection management, SQL helpers, CLI argument parsing,
structured logging, and optional TOML config file support used by
both scripts.
"""

import argparse
import json
import logging
import os
import signal
import subprocess
import sys
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass

import psycopg
from psycopg.conninfo import make_conninfo

try:
    import tomllib
except ModuleNotFoundError:  # Python < 3.11
    try:
        import tomli as tomllib  # type: ignore[no-redef]
    except ModuleNotFoundError:
        tomllib = None  # type: ignore[assignment]

# ---------------------------------------------------------------------------
# Structured logging
# ---------------------------------------------------------------------------

LOG = logging.getLogger("backup")

_STRUCTURED_FIELDS = (
    "phase",
    "schema",
    "table",
    "part",
    "elapsed_s",
    "bytes",
    "throughput_mbps",
    "status",
    "rows",
    "returncode",
    "snapshot_id",
    "num_tables",
    "num_parts",
)


class _JsonFormatter(logging.Formatter):
    """Emit one JSON object per log line for machine consumption."""

    def format(self, record: logging.LogRecord) -> str:
        entry: dict = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname.lower(),
            "msg": record.getMessage().strip(),
        }
        for key in _STRUCTURED_FIELDS:
            val = getattr(record, key, None)
            if val is not None:
                entry[key] = val
        return json.dumps(entry)


class _TextFormatter(logging.Formatter):
    """Pass-through formatter that reproduces bare ``print()`` output."""

    def format(self, record: logging.LogRecord) -> str:
        return record.getMessage()


class _LevelRouter(logging.Handler):
    """Route WARNING+ to stderr, INFO and below to stdout.

    Preserves the existing behaviour where ``print()`` goes to stdout
    and ``print(..., file=sys.stderr)`` goes to stderr.
    """

    def __init__(self) -> None:
        super().__init__()
        self._out = logging.StreamHandler(sys.stdout)
        self._err = logging.StreamHandler(sys.stderr)

    def setFormatter(self, fmt: logging.Formatter | None) -> None:  # type: ignore[override]
        super().setFormatter(fmt)
        self._out.setFormatter(fmt)
        self._err.setFormatter(fmt)

    def emit(self, record: logging.LogRecord) -> None:
        if record.levelno >= logging.WARNING:
            self._err.emit(record)
        else:
            self._out.emit(record)


def setup_logging(log_format: str = "text", log_level: str = "INFO") -> None:
    """Configure the ``backup`` logger.

    In *text* mode WARNING+ goes to stderr, INFO/DEBUG to stdout
    (matching prior ``print()`` behaviour).  In *json* mode all
    levels go to stdout as JSON lines.
    """
    handler: logging.Handler
    if log_format == "json":
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_JsonFormatter())
    else:
        handler = _LevelRouter()
        handler.setFormatter(_TextFormatter())
    LOG.handlers.clear()
    LOG.addHandler(handler)
    LOG.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    LOG.propagate = False


setup_logging()

# ---------------------------------------------------------------------------
# Connection helper
# ---------------------------------------------------------------------------


@dataclass
class ConnInfo:
    """Encapsulates PostgreSQL connection parameters.

    Avoids passing host/port/user/password/database to every function.
    """

    host: str
    port: str
    user: str
    password: str
    database: str

    def env(self) -> dict[str, str]:
        """Return an environment dict with PGPASSWORD injected."""
        return {**os.environ, "PGPASSWORD": self.password}

    def conninfo(self) -> str:
        """psycopg-style connection string (key=value, space-separated).

        Used by :func:`psycopg.connect` for the *control plane* only:
        catalog queries (``psql_query``/``psql_exec``), the snapshot
        holder in ``parallel_dump``, and SIGINT cancellation.  Bulk
        ``COPY`` does **not** go through psycopg — it runs in ``psql``
        / ``pg_dump`` / ``pg_restore`` subprocesses which consume
        :meth:`psql_args` + :meth:`env` instead.

        Uses :func:`psycopg.conninfo.make_conninfo` so values with spaces,
        quotes, or backslashes — common in auto-generated passwords — are
        properly escaped.  Naive f-string interpolation here would silently
        corrupt any password containing a space (the parser would treat
        everything past the first space as the start of the next key).
        """
        return make_conninfo(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            dbname=self.database,
        )

    def psql_args(self) -> list[str]:
        """Base psql command-line args shared by all invocations.

        -X  skips ~/.psqlrc so user config doesn't alter formatting.
        """
        return [
            "psql",
            "-h",
            self.host,
            "-p",
            self.port,
            "-U",
            self.user,
            "-d",
            self.database,
            "-X",
        ]

    def pg_dump_args(self) -> list[str]:
        """Base pg_dump command-line args (host, port, user, database)."""
        return [
            "pg_dump",
            "-h",
            self.host,
            "-p",
            self.port,
            "-U",
            self.user,
            "-d",
            self.database,
        ]

    def pg_restore_args(self) -> list[str]:
        """Base pg_restore command-line args (host, port, user, database)."""
        return [
            "pg_restore",
            "-h",
            self.host,
            "-p",
            self.port,
            "-U",
            self.user,
            "-d",
            self.database,
        ]


# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------


def quote_ident(name: str) -> str:
    """Double-quote a SQL identifier, escaping embedded double quotes.

    Prevents SQL injection and handles reserved words or special chars
    in table/column names (e.g. "user", "Order", "my-table").
    """
    return '"' + name.replace('"', '""') + '"'


def qualified_name(schema: str, table: str) -> str:
    """Build a schema-qualified, double-quoted SQL identifier.

    Example: ``qualified_name("public", "my-table")`` → ``"public"."my-table"``
    """
    return f"{quote_ident(schema)}.{quote_ident(table)}"


def quote_literal(value: str) -> str:
    """Single-quote a SQL string literal, escaping embedded single quotes."""
    return "'" + value.replace("'", "''") + "'"


def regclass_literal(schema: str, table: str) -> str:
    """Build a single-quoted ``"schema"."table"`` string for ``::regclass`` casts.

    Uses double-quoted identifiers inside the string literal so names
    containing dots, spaces, or reserved words are unambiguous.

    Example: ``regclass_literal("public", "my-table")`` → ``'"public"."my-table"'``
    """
    inner = f"{quote_ident(schema)}.{quote_ident(table)}"
    return quote_literal(inner)


def check_subprocess_result(
    result: subprocess.CompletedProcess[str],
    label: str,
) -> None:
    """Log and raise if a subprocess exited with a non-zero code.

    Centralises the error-check pattern used by psql, pg_dump, and
    pg_restore wrappers so the same logging logic isn't duplicated.
    """
    if result.returncode != 0:
        LOG.error("ERROR: %s failed (exit code %d)", label, result.returncode)
        if result.stderr:
            LOG.error("%s", result.stderr.strip())
        result.check_returncode()


def _format_psql_value(val: object) -> str:
    """Format a single psycopg value to match psql ``-t -A`` output.

    NULL → empty string, booleans → ``t``/``f``, everything else → ``str()``.
    This preserves the output contract callers relied on when ``psql_query``
    was a subprocess wrapper around ``psql -t -A -c``.
    """
    if val is None:
        return ""
    if val is True:
        return "t"
    if val is False:
        return "f"
    return str(val)


def _format_psql_row(row: tuple) -> str:
    """Join a row's values with ``|`` to mirror psql ``-A`` unaligned output."""
    return "|".join(_format_psql_value(v) for v in row)


def psql_query(conn: ConnInfo, sql: str) -> str:
    """Execute a query and return rows in psql ``-t -A`` format.

    Output contract (unchanged from the previous subprocess-based impl):

    * one row per line, trimmed of the trailing newline
    * columns joined with ``|``
    * NULL rendered as empty string, booleans as ``t``/``f``
    * empty string for queries that return no rows
    """
    with tracked_connection(conn.conninfo(), autocommit=True) as pg:
        with pg.cursor() as cur:
            cur.execute(sql)
            if cur.description is None:
                return ""
            rows = cur.fetchall()
    return "\n".join(_format_psql_row(r) for r in rows)


def discover_schemas(conn: ConnInfo) -> list[str]:
    """Return all user-created schema names, sorted alphabetically.

    Excludes system schemas (``pg_catalog``, ``information_schema``,
    ``pg_toast``, and temporary schemas).
    """
    sql = (
        "SELECT nspname FROM pg_namespace "
        "WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast') "
        "AND nspname NOT LIKE 'pg_temp_%' "
        "AND nspname NOT LIKE 'pg_toast_temp_%' "
        "ORDER BY nspname;"
    )
    raw = psql_query(conn, sql)
    return [line.strip() for line in raw.splitlines() if line.strip()]


def discover_tables(conn: ConnInfo, schema: str) -> list[str]:
    """Return all ordinary table names in *schema*, sorted largest first.

    Queries ``pg_tables`` and orders by ``pg_relation_size`` descending so
    the biggest tables are processed first — front-loading work keeps the
    snapshot holder open for the shortest possible time.
    """
    sql = (
        "SELECT tablename FROM pg_tables "
        f"WHERE schemaname = {quote_literal(schema)} "
        "ORDER BY pg_relation_size("
        "  (quote_ident(schemaname) || '.' || quote_ident(tablename))::regclass"
        ") DESC;"
    )
    raw = psql_query(conn, sql)
    return [line.strip() for line in raw.splitlines() if line.strip()]


MIN_PG_VERSION_TID_SCAN = 14


def pg_server_version_num(conn: ConnInfo) -> int:
    """Return the server's major version number (e.g. 14, 15, 17)."""
    raw = psql_query(conn, "SHOW server_version_num;")
    return int(raw) // 10000


def require_pg_version(conn: ConnInfo, min_major: int, feature: str) -> None:
    """Abort if the PostgreSQL server is older than *min_major*."""
    major = pg_server_version_num(conn)
    if major < min_major:
        LOG.error(
            "ERROR: %s requires PostgreSQL %d+, but server is PostgreSQL %d.",
            feature,
            min_major,
            major,
        )
        sys.exit(1)


def psql_exec(conn: ConnInfo, sql: str) -> None:
    """Execute one or more SQL statements, raising on failure.

    Uses autocommit mode (matching ``psql -c``) so statements that cannot
    run inside a transaction block — ``CREATE DATABASE``, ``VACUUM``,
    ``ALTER TABLE … SET UNLOGGED`` on some PG versions — still work.

    Supports multi-statement input (``"DROP ...; CREATE ...;"``) the same
    way ``psql -c`` did, because psycopg forwards the whole string to the
    server when no parameters are bound.
    """
    with tracked_connection(conn.conninfo(), autocommit=True) as pg:
        with pg.cursor() as cur:
            cur.execute(sql)


# ---------------------------------------------------------------------------
# Signal handling / graceful cancellation
# ---------------------------------------------------------------------------

_cancel_event = threading.Event()
_active_procs: list[subprocess.Popen] = []
# RLock, not Lock: cancel_workers() runs from the SIGINT / SIGTERM
# handler which Python delivers on the main thread between bytecode
# instructions.  If the main thread was already inside register_proc /
# register_conn / unregister_* when the signal arrived, a non-reentrant
# Lock would deadlock the moment cancel_workers() tried to re-acquire
# the same lock on the same thread.  Making both registry locks
# reentrant keeps the window closed at the cost of a few extra
# thread-id bookkeeping ops per critical section (negligible — these
# sections are single list operations).
_proc_lock = threading.RLock()
# Tracked psycopg connections — kept separate from _active_procs so the
# cancel-signal handler can abort in-flight control-plane queries (the
# snapshot holder, catalog discovery, psql_query / psql_exec) via the
# server-side cancel protocol.  Bulk COPY runs in psql subprocesses
# tracked in _active_procs; see cancel_workers() for how both lists are
# flushed on SIGINT.
_active_conns: list[psycopg.Connection] = []
_conn_lock = threading.RLock()


def is_cancelled() -> bool:
    """True after a SIGINT/SIGTERM has been received."""
    return _cancel_event.is_set()


def _terminate_if_cancelled(proc: subprocess.Popen) -> None:
    """Terminate *proc* if a cancel request arrived during its registration.

    Closes the ``Popen → _active_procs.append`` race: if
    :func:`cancel_workers` ran between the two and snapshotted the
    list *before* the append, the freshly-launched subprocess would
    otherwise run to completion uncancelled.  Cheap (Event.is_set is
    atomic, no lock) and safe to call multiple times.
    """
    if _cancel_event.is_set():
        try:
            proc.terminate()
        except OSError:
            pass


def register_conn(pg: psycopg.Connection) -> None:
    """Record *pg* so ``cancel_workers()`` can abort its current query.

    Every site that opens a psycopg connection carrying *control-plane*
    work (the snapshot holder, catalog queries via
    ``psql_query``/``psql_exec``) should register its connection and
    unregister it again when done.  The :func:`tracked_connection`
    context manager wraps the register / unregister pair for the
    common short-lived case.

    Data-plane ``COPY`` workers do not use psycopg — they drive ``psql``
    subprocesses which are tracked separately in ``_active_procs``; the
    two registries are both flushed by :func:`cancel_workers`.
    """
    with _conn_lock:
        _active_conns.append(pg)


def unregister_conn(pg: psycopg.Connection) -> None:
    """Remove *pg* from the cancel registry.  Safe to call twice."""
    with _conn_lock:
        try:
            _active_conns.remove(pg)
        except ValueError:
            pass


@contextmanager
def tracked_connection(conninfo: str, **kwargs) -> Iterator[psycopg.Connection]:
    """Open a psycopg connection that is visible to ``cancel_workers()``.

    Behaves like ``with psycopg.connect(conninfo) as pg`` — commits on
    clean exit, rolls back on exception, and closes the connection — but
    additionally (un)registers the connection for signal-driven
    cancellation.

    Long-lived connections whose lifetime spans multiple scopes (e.g.
    the snapshot holder) should call :func:`register_conn` /
    :func:`unregister_conn` directly instead.

    Note on the ``connect → register_conn`` race: unlike subprocesses
    (where :func:`_terminate_if_cancelled` actually kills the child),
    the symmetric move here — ``pg.cancel_safe()`` on a freshly-opened
    idle conn — is a server-side no-op (``CancelRequest`` is dropped
    when there is no in-flight query) and would not cancel the
    caller's first query.  Cooperative ``is_cancelled()`` checks in
    the caller already cover this gap; control-plane queries are
    short, and a follow-up ``cancel_workers()`` (e.g. from the
    snapshot watchdog after probe failure) will pick up this conn
    from ``_active_conns`` because it is now registered.
    """
    pg = psycopg.connect(conninfo, **kwargs)
    register_conn(pg)
    try:
        with pg:
            yield pg
    finally:
        unregister_conn(pg)


def cancel_workers() -> None:
    """Set the cancellation flag, terminate subprocesses, cancel conns.

    Three things happen:

    1. ``_cancel_event`` is set so cooperative workers can check
       :func:`is_cancelled` between chunks and bail out.
    2. Every tracked subprocess (``pg_dump``, ``pg_restore``, and the
       ``psql`` children driving ``COPY TO STDOUT`` / ``COPY FROM
       STDIN``) gets SIGTERM — this is what actually stops the bulk
       data path, since the payload bytes never travel through Python.
    3. Every tracked psycopg connection has its current query cancelled
       via the server-side cancel protocol (``PQcancel``-equivalent).
       In this codebase those are control-plane connections only
       (snapshot holder, catalog queries, ``psql_query`` /
       ``psql_exec``); cancelling them unsticks any long catalog scan
       and lets the holder's watchdog thread exit promptly.

    Called by signal handlers and by the snapshot-holder watchdog.
    Best-effort: errors from ``cancel_safe()`` are swallowed because the
    connection may already have closed.
    """
    _cancel_event.set()
    # Snapshot both registries under their locks, then operate outside
    # the critical section.  pg.cancel_safe() opens a short-lived TCP
    # connection to the server to issue PQcancel; holding _conn_lock
    # across that network round-trip would needlessly block any worker
    # thread trying to register / unregister a connection during teardown.
    with _proc_lock:
        procs = list(_active_procs)
    for proc in procs:
        try:
            proc.terminate()
        except OSError:
            pass
    with _conn_lock:
        conns = list(_active_conns)
    for pg in conns:
        try:
            # cancel_safe is thread-safe and designed for exactly
            # this "cancel from a different thread / signal handler"
            # use case.  Available since psycopg 3.2.
            pg.cancel_safe()
        except Exception:  # noqa: BLE001 — best-effort cancel
            pass


def install_signal_handlers() -> None:
    """Install SIGINT/SIGTERM handlers that gracefully cancel workers.

    First signal: terminates all tracked subprocesses so threads can join.
    Second signal: restores the default handler (hard kill).
    """

    def _handler(signum: int, _frame: object) -> None:
        name = signal.Signals(signum).name
        LOG.warning("\n%s received — cancelling workers...", name)
        cancel_workers()
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)


def run_cancellable(
    cmd: list[str],
    *,
    input: str | None = None,
    env: dict[str, str] | None = None,
    check: bool = False,
) -> subprocess.CompletedProcess[str]:
    """Like ``subprocess.run`` but trackable for signal-based cancellation.

    Registered processes are terminated when ``install_signal_handlers``
    has been called and a SIGINT/SIGTERM arrives.
    """
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE if input is not None else subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        text=True,
    )
    with _proc_lock:
        _active_procs.append(proc)
    _terminate_if_cancelled(proc)
    try:
        stdout, stderr = proc.communicate(input=input)
    except BaseException:
        proc.kill()
        proc.wait()
        raise
    finally:
        with _proc_lock:
            if proc in _active_procs:
                _active_procs.remove(proc)
    if check and proc.returncode != 0:
        raise subprocess.CalledProcessError(proc.returncode, cmd, stdout, stderr)
    return subprocess.CompletedProcess(cmd, proc.returncode, stdout, stderr)


# ---------------------------------------------------------------------------
# Streaming subprocess → binary sink
# ---------------------------------------------------------------------------


# How many bytes to pull off the subprocess stdout at a time.  Small
# enough that SIGINT cancellation feels responsive, large enough that
# the per-chunk syscall + S3 multipart-buffer append overhead doesn't
# dominate.  64 KiB matches boto3's default TransferConfig io_chunksize.
_STREAM_CHUNK_SIZE = 64 * 1024


# ---------------------------------------------------------------------------
# Zero-Python fast-path: pass the destination file's OS fd directly to
# subprocess.Popen as ``stdout=`` (or ``stdin=`` on the restore side).
# Popen does ``dup2(fd, 1)`` in the child, so the subprocess writes to
# the file via its own libc ``write()`` calls — bypassing every Python
# byte of our process.  Equivalent to the pre-refactor pattern where
# ``psql … > part.bin`` shell-redirected output straight to disk.
#
# Why it matters: even with ``os.splice`` (Linux's kernel-side zero-
# copy), our process still ran a Python loop with one iteration per
# pipe-buffer worth of bytes (~64 KiB by default).  At 128-way / 6 GB/s
# that's ~100 K loop iterations/s aggregate — all serialising on the
# GIL for ``total += n`` + threshold check + the optional callback.
# OS-direct redirection has *zero* per-chunk Python in our process:
# we Popen, register, drain stderr, wait, surface errors, and that's
# the entire pipeline.  N workers scale with CPU cores the way the
# pre-refactor shell-redirected pipeline did.
#
# Falls back to the original Python read/write loop when the sink is
# Python-implemented (``_S3MultipartWriter`` for S3 multipart upload,
# ``io.BytesIO`` in tests) — those sinks must look at every byte to
# chunk them into S3 parts, so there's nothing OS redirection could do.
# ---------------------------------------------------------------------------

# Live progress for the OS-direct path: poll ``os.fstat(sink_fd).st_size``
# every N seconds and feed it to bytes_callback.  Cheap (one fstat per
# worker per interval) and decoupled from the data path, so the kernel
# write rate is unaffected.  Half the dump-side progress_monitor poll
# interval (3 s) so each progress report sees a fresh sample.
_OS_DIRECT_PROGRESS_INTERVAL = 1.0  # seconds


def _real_fd(obj) -> int | None:
    """Return *obj*'s underlying OS fd, or None if it isn't a real file.

    A regular-file fd can be passed straight to :class:`subprocess.Popen`
    as ``stdin=`` / ``stdout=``, letting the child write to (or read
    from) it via its own libc syscalls.  Python-implemented streams
    (``io.BytesIO``, ``_S3MultipartWriter``, ``_S3StreamReader``) don't
    qualify — their ``fileno()`` either doesn't exist or raises
    :class:`io.UnsupportedOperation` (which inherits from ``OSError``
    so the ``except OSError`` branch catches it).

    Returning None pushes the caller onto the Python read/write
    fallback, which is correct for those buffered sinks: S3 multipart
    upload has to look at every byte anyway to chunk it into parts,
    so there's nothing OS redirection could do for it.
    """
    fileno = getattr(obj, "fileno", None)
    if fileno is None:
        return None
    try:
        fd = fileno()
    except (OSError, ValueError):
        return None
    return fd if isinstance(fd, int) and fd >= 0 else None


def _start_stderr_drain(proc: subprocess.Popen, stderr_buf: bytearray) -> threading.Thread:
    """Spawn a daemon thread that drains *proc*'s stderr into *stderr_buf*.

    Without this, a chatty subprocess (pg_dump warnings, pg_restore
    "errors ignored on restore", psql ON_ERROR_STOP traces) can fill
    the stderr pipe (typically 64 KiB on Linux) and block writing,
    deadlocking us waiting for stdout / wait().  The drain thread reads
    in 4 KiB increments and is daemon=True so it doesn't keep the
    process alive on its own.
    """

    def _drain() -> None:
        assert proc.stderr is not None
        while True:
            try:
                chunk = proc.stderr.read(4096)
            except ValueError:
                # pipe already closed (proc.kill / fd reuse path)
                return
            if not chunk:
                return
            stderr_buf.extend(chunk)

    t = threading.Thread(target=_drain, daemon=True)
    t.start()
    return t


def _start_fstat_progress(
    fd: int,
    bytes_callback: Callable[[int], None],
    stop_event: threading.Event,
    interval: float = _OS_DIRECT_PROGRESS_INTERVAL,
) -> threading.Thread:
    """Spawn a watcher that reports *fd*'s current size to *bytes_callback*.

    Used by the OS-direct sink path so the live progress display
    updates while the subprocess is writing — without the watcher,
    bytes_callback would only fire once at end-of-stream and the
    progress monitor would show a flat zero for the duration.

    The watcher only acquires the GIL once per ``interval`` (default
    1 s), so even 128 of these threads add at most 128 fstat()s/s to
    the system — invisible next to the data-path syscalls.
    """

    def _watch() -> None:
        while not stop_event.is_set():
            stop_event.wait(interval)
            if stop_event.is_set():
                break
            try:
                bytes_callback(os.fstat(fd).st_size)
            except OSError:
                # fd was closed under us (proc.kill / shutdown).
                return

    t = threading.Thread(target=_watch, daemon=True)
    t.start()
    return t


def stream_subprocess_to_sink(
    cmd: list[str],
    sink,
    *,
    env: dict[str, str] | None = None,
    label: str = "subprocess",
    chunk_size: int = _STREAM_CHUNK_SIZE,
    bytes_callback: Callable[[int], None] | None = None,
) -> None:
    """Run *cmd* and stream its binary stdout into *sink* chunk by chunk.

    Sink-direction counterpart of :func:`stream_source_to_subprocess`:
    use this one when the subprocess *produces* bytes (``pg_dump``,
    ``psql COPY TO STDOUT``) and use the other when the subprocess
    *consumes* bytes (``psql COPY FROM STDIN``).

    *sink* must support ``write(bytes)``.  Typical use is a
    :meth:`storage.Blob.open_write` context manager so pg_dump's output
    streams straight into an S3 multipart upload or a local file
    without ever being fully buffered in Python memory.

    Unlike :func:`run_cancellable`, which uses ``text=True`` and
    buffers all of stdout before returning, this helper:

    * keeps the subprocess registered in ``_active_procs`` so SIGINT /
      :func:`cancel_workers` terminates it;
    * drains stderr on a background thread so the pipe buffer can't
      fill up and block the producer;
    * passes stdout through as raw bytes — critical for ``pg_dump -Fc``
      custom-format output, which is binary and must never be decoded.

    If *bytes_callback* is given it's invoked with the *cumulative*
    byte count streamed so far.  Used by parallel COPY workers to
    publish live progress into the shared ``bytes_written`` dict that
    :func:`progress_monitor` polls.  The callback must be cheap
    (typical use: a single dict assignment) and non-raising —
    exceptions escape through the normal ``try/except BaseException``
    path and terminate the subprocess, which is usually *not* what you
    want for a progress counter.

    On the OS-direct fast-path the callback is driven by an
    ``os.fstat`` watcher firing once per :data:`_OS_DIRECT_PROGRESS_INTERVAL`
    (1 s by default) plus once at end of stream; on the Python fallback
    it fires after every ``sink.write``.  Both paths finish with one
    final callback carrying the true total, so callers that read the
    counter after the function returns see an exact value.

    Implementation: when *sink* exposes a real OS fd (``LocalBlob``
    backs onto an open file) we hand the fd directly to ``Popen`` as
    ``stdout=`` so the subprocess writes to the file via its own libc
    syscalls — *zero* Python in the data path, no GIL traffic from the
    streaming step, scales across cores.  When *sink* is a Python-
    implemented stream (``_S3MultipartWriter``, ``io.BytesIO``) we fall
    back to a read/write loop because the bytes have to traverse Python
    anyway to be chunked into multipart parts.

    Raises :class:`subprocess.CalledProcessError` with the captured
    stderr as the error payload if the process exits non-zero.
    """
    sink_fd = _real_fd(sink)
    stderr_buf = bytearray()

    if sink_fd is not None:
        # OS-direct fast-path: dup2(sink_fd, 1) in the child via Popen's
        # ``stdout=`` argument.  The subprocess writes straight into the
        # destination file; we never see the bytes.  Same scaling
        # characteristics as the pre-refactor `psql > part.bin` shell
        # redirection.
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.DEVNULL,
            stdout=sink_fd,
            stderr=subprocess.PIPE,
            env=env,
        )
        with _proc_lock:
            _active_procs.append(proc)
        _terminate_if_cancelled(proc)

        stderr_thread = _start_stderr_drain(proc, stderr_buf)

        # Live progress watcher (only when the caller actually wants
        # updates).  Without it, bytes_callback only fires at end of
        # stream and progress_monitor would display a flat zero for the
        # duration of a multi-minute table.
        progress_stop = threading.Event()
        progress_thread: threading.Thread | None = None
        if bytes_callback is not None:
            progress_thread = _start_fstat_progress(sink_fd, bytes_callback, progress_stop)

        try:
            proc.wait()
        except BaseException:
            proc.kill()
            proc.wait()
            raise
        finally:
            progress_stop.set()
            if progress_thread is not None:
                progress_thread.join(timeout=2)
            stderr_thread.join(timeout=5)
            with _proc_lock:
                if proc in _active_procs:
                    _active_procs.remove(proc)

        # Final exact byte count after the watcher's last sample.
        # Catches whatever was written between the last fstat and the
        # subprocess closing fd 1.
        if bytes_callback is not None:
            try:
                bytes_callback(os.fstat(sink_fd).st_size)
            except OSError:
                pass

        if proc.returncode != 0:
            stderr_text = stderr_buf.decode("utf-8", errors="replace")
            LOG.error("ERROR: %s failed (exit code %d)", label, proc.returncode)
            if stderr_text.strip():
                LOG.error("%s", stderr_text.strip())
            raise subprocess.CalledProcessError(proc.returncode, cmd, None, stderr_text)
        return

    # Python fallback: sink is a Python-implemented stream (S3 multipart
    # writer, in-memory buffer for tests).  Bytes have to traverse Python
    # for chunking / multipart accounting, so we keep the read/write
    # loop with per-chunk callbacks.
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )
    with _proc_lock:
        _active_procs.append(proc)
    _terminate_if_cancelled(proc)

    stderr_thread = _start_stderr_drain(proc, stderr_buf)

    assert proc.stdout is not None
    try:
        total = 0
        while True:
            chunk = proc.stdout.read(chunk_size)
            if not chunk:
                break
            sink.write(chunk)
            if bytes_callback is not None:
                total += len(chunk)
                bytes_callback(total)
        proc.stdout.close()
        proc.wait()
    except BaseException:
        proc.kill()
        proc.wait()
        raise
    finally:
        stderr_thread.join(timeout=5)
        with _proc_lock:
            if proc in _active_procs:
                _active_procs.remove(proc)

    if proc.returncode != 0:
        stderr_text = stderr_buf.decode("utf-8", errors="replace")
        LOG.error("ERROR: %s failed (exit code %d)", label, proc.returncode)
        if stderr_text.strip():
            LOG.error("%s", stderr_text.strip())
        raise subprocess.CalledProcessError(proc.returncode, cmd, None, stderr_text)


def stream_source_to_subprocess(
    cmd: list[str],
    source,
    *,
    env: dict[str, str] | None = None,
    label: str = "subprocess",
    chunk_size: int = _STREAM_CHUNK_SIZE,
    bytes_callback: Callable[[int], None] | None = None,
) -> None:
    """Run *cmd* and pump *source*'s bytes into its stdin chunk by chunk.

    Source-direction counterpart of :func:`stream_subprocess_to_sink`:
    use this one when the subprocess *consumes* bytes (``psql COPY
    FROM STDIN``) and use the other when the subprocess *produces*
    bytes (``pg_dump``, ``psql COPY TO STDOUT``).

    Typical use is restore-side ``psql`` running ``COPY … FROM STDIN``
    with its stdin fed by a :meth:`storage.Blob.open_read` stream so a
    100 GiB part can be loaded from S3 without ever being fully
    buffered in Python memory.

    *source* must support ``read(n) -> bytes`` — anything
    :meth:`Blob.open_read` returns qualifies, as does a plain file
    object.  EOF is signalled by an empty ``bytes``.

    Mechanics:

    * ``stdout`` is redirected to ``DEVNULL`` (callers don't need psql's
      command tags; a chatty COPY would otherwise fill the pipe and
      block);
    * ``stderr`` is drained on a dedicated thread, same as
      :func:`stream_subprocess_to_sink`, so ``ON_ERROR_STOP`` failure
      messages can't deadlock the pipeline;
    * the subprocess is registered in ``_active_procs`` so SIGINT /
      :func:`cancel_workers` terminates it immediately.

    If *bytes_callback* is given it fires with the *cumulative* byte
    count fed into the pipe.  On the OS-direct fast-path it fires once
    at end of stream with the source's final read offset; on the Python
    fallback it fires after every chunk write.  Not used by the restore
    progress monitor today (which queries ``pg_stat_progress_copy``
    server-side) but exposed for symmetry with the dump path and for
    future per-worker instrumentation.

    Implementation: when *source* exposes a real OS fd (``LocalBlob``
    backs onto an open file) we hand the fd directly to ``Popen`` as
    ``stdin=`` so the subprocess reads from the file via its own libc
    syscalls — *zero* Python in the data path, no GIL traffic during
    the load.  S3-backed sources (``_S3StreamReader``) and in-memory
    test sources fall back to a read/write loop because their bytes
    have to pass through Python anyway.

    Raises :class:`subprocess.CalledProcessError` with captured stderr
    when the subprocess exits non-zero.  A ``BrokenPipeError`` while
    writing is treated as early subprocess death — we stop sending,
    wait for it, and let the resulting non-zero exit surface the real
    server-side error.
    """
    source_fd = _real_fd(source)
    stderr_buf = bytearray()

    if source_fd is not None:
        # OS-direct fast-path: dup2(source_fd, 0) in the child via
        # Popen's ``stdin=`` argument.  The subprocess reads from the
        # file via its own syscalls; we never touch the bytes.  Same
        # scaling characteristics as the pre-refactor pipeline.
        proc = subprocess.Popen(
            cmd,
            stdin=source_fd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            env=env,
        )
        with _proc_lock:
            _active_procs.append(proc)
        _terminate_if_cancelled(proc)

        stderr_thread = _start_stderr_drain(proc, stderr_buf)

        try:
            proc.wait()
        except BaseException:
            proc.kill()
            proc.wait()
            raise
        finally:
            stderr_thread.join(timeout=5)
            with _proc_lock:
                if proc in _active_procs:
                    _active_procs.remove(proc)

        # Final byte count: subprocess and parent share the open file
        # description (Popen dup2's the fd), so as the child reads, the
        # parent's offset advances too.  lseek(SEEK_CUR) returns how far
        # the subprocess actually got — accurate even when ON_ERROR_STOP
        # aborts mid-stream and the subprocess didn't consume the full
        # source.
        if bytes_callback is not None:
            try:
                consumed = os.lseek(source_fd, 0, os.SEEK_CUR)
                bytes_callback(consumed)
            except OSError:
                pass

        if proc.returncode != 0:
            stderr_text = stderr_buf.decode("utf-8", errors="replace")
            LOG.error("ERROR: %s failed (exit code %d)", label, proc.returncode)
            if stderr_text.strip():
                LOG.error("%s", stderr_text.strip())
            raise subprocess.CalledProcessError(proc.returncode, cmd, None, stderr_text)
        return

    # Python fallback: source is a Python-implemented stream
    # (_S3StreamReader, BytesIO).  Keep the read/write loop with
    # per-chunk callbacks and BrokenPipeError handling.
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        env=env,
    )
    with _proc_lock:
        _active_procs.append(proc)
    _terminate_if_cancelled(proc)

    stderr_thread = _start_stderr_drain(proc, stderr_buf)

    assert proc.stdin is not None
    try:
        total = 0
        while True:
            chunk = source.read(chunk_size)
            if not chunk:
                break
            try:
                proc.stdin.write(chunk)
            except BrokenPipeError:
                # psql died before consuming everything — e.g. bad-data
                # abort under ON_ERROR_STOP.  Stop writing; the non-zero
                # returncode below turns this into a CalledProcessError
                # carrying the real server-side error in stderr.
                break
            if bytes_callback is not None:
                total += len(chunk)
                bytes_callback(total)
        try:
            proc.stdin.close()
        except BrokenPipeError:
            pass
        proc.wait()
    except BaseException:
        proc.kill()
        proc.wait()
        raise
    finally:
        stderr_thread.join(timeout=5)
        with _proc_lock:
            if proc in _active_procs:
                _active_procs.remove(proc)

    if proc.returncode != 0:
        stderr_text = stderr_buf.decode("utf-8", errors="replace")
        LOG.error("ERROR: %s failed (exit code %d)", label, proc.returncode)
        if stderr_text.strip():
            LOG.error("%s", stderr_text.strip())
        raise subprocess.CalledProcessError(proc.returncode, cmd, None, stderr_text)


# ---------------------------------------------------------------------------
# CLI helpers
# ---------------------------------------------------------------------------

PROGRESS_INTERVAL = 3  # seconds between progress reports
PRE_DATA_FILENAME = "pre_data.sql"  # CREATE TABLE (bare structure, no indexes/constraints)
POST_DATA_FILENAME = (
    "post_data.dump"  # CREATE INDEX, PRIMARY KEY, UNIQUE, FK, triggers (pg_dump custom format)
)
SEQUENCES_FILENAME = "sequences.sql"  # setval() calls to restore sequence positions
GLOBAL_PRE_DATA_FILENAME = (
    "global_pre_data.dump"  # extensions, types, enums, domains (pg_dump custom format)
)
GLOBAL_POST_DATA_FILENAME = (
    "global_post_data.dump"  # standalone functions, event triggers (pg_dump custom format)
)
MATVIEW_MARKER_FILENAME = "matview"  # empty marker file: directory is a materialized view


def discover_matviews(conn: ConnInfo, schema: str) -> list[str]:
    """Return materialized view names in *schema*, sorted alphabetically.

    Used by database-level dumps to capture mat-view DDL separately from
    ordinary tables (mat-views can't be loaded via COPY FROM — they must
    be populated via REFRESH MATERIALIZED VIEW).
    """
    sql = (
        "SELECT matviewname FROM pg_matviews "
        f"WHERE schemaname = {quote_literal(schema)} "
        "ORDER BY matviewname;"
    )
    raw = psql_query(conn, sql)
    return [line.strip() for line in raw.splitlines() if line.strip()]


def add_connection_args(parser: argparse.ArgumentParser) -> None:
    """Add ``--config`` and connection flags to the argument parser.

    All connection flags default to ``None`` so that values from a
    config file can fill in the gaps.  Required fields (host, user,
    password, database) are validated by ``validate_connection_args``
    after the config merge.
    """
    parser.add_argument(
        "--config",
        default=None,
        help="Optional. Path to backup.toml config file.  "
        "Auto-detected from ./backup.toml or ~/.config/pg-parallel-backup/backup.toml.",
    )
    parser.add_argument("--host", default=None, help="PostgreSQL hostname")
    parser.add_argument("--port", default=None, help="PostgreSQL port (default: 5432)")
    parser.add_argument("--user", default=None, help="PostgreSQL user")
    parser.add_argument("--password", default=None, help="PostgreSQL password")
    parser.add_argument("--database", default=None, help="Database name")
    parser.add_argument(
        "--log-format",
        choices=["text", "json"],
        default=None,
        help="Log output format (default: text)",
    )
    parser.add_argument(
        "--log-level",
        choices=["debug", "info", "warning", "error"],
        default=None,
        help="Minimum log level (default: info)",
    )


# ---------------------------------------------------------------------------
# Configuration file support
# ---------------------------------------------------------------------------

_CONFIG_SEARCH_PATHS = [
    "./backup.toml",
    os.path.expanduser("~/.config/pg-parallel-backup/backup.toml"),
]


def load_config(config_path: str | None) -> dict:
    """Load a TOML config file and return it as a dict.

    Search order (first match wins):

    1. Explicit ``--config <path>`` (error if not found)
    2. ``./backup.toml`` in the current working directory
    3. ``~/.config/pg-parallel-backup/backup.toml``

    Returns an empty dict when no config file is found or ``tomllib``
    is not available (Python < 3.11).

    Expected TOML layout::

        [connection]
        host = "db.example.com"
        port = "5432"
        user = "backup_user"
        password = "secret"        # or use password_env
        password_env = "PGPASSWORD" # read password from env var
        database = "mydb"

        [dump]
        num_parts = 8
        output_dir = "/data/backups"
        retries = 2

        [restore]
        input_dir = "/data/backups/latest"
        index_jobs = 4
        unlogged = false
        retries = 1
    """
    if config_path:
        if tomllib is None:
            LOG.error(
                "ERROR: --config requires Python 3.11+ (tomllib). "
                "Install the 'tomli' backport for older versions.",
            )
            sys.exit(1)
        if not os.path.isfile(config_path):
            LOG.error("ERROR: Config file not found: %s", config_path)
            sys.exit(1)
        with open(config_path, "rb") as f:
            cfg = tomllib.load(f)
        LOG.info("Config: %s", config_path)
        return cfg

    if tomllib is None:
        return {}

    for path in _CONFIG_SEARCH_PATHS:
        if os.path.isfile(path):
            with open(path, "rb") as f:
                cfg = tomllib.load(f)
            LOG.info("Config: %s", path)
            return cfg

    return {}


def apply_config_defaults(
    args: argparse.Namespace,
    config: dict,
    section: str,
) -> None:
    """Fill ``None``-valued CLI args from config file values.

    Merges keys from ``[connection]`` and ``[<section>]`` (e.g.
    ``[dump]`` or ``[restore]``) into *args*.  CLI arguments always
    take precedence — only ``None`` values are overwritten.

    For passwords, ``password_env`` (the name of an environment variable)
    takes precedence over the literal ``password`` key, avoiding secrets
    in config files.
    """
    conn_cfg = config.get("connection", {})
    sect_cfg = config.get(section, {})

    for key in ("host", "port", "user", "database"):
        if getattr(args, key, None) is None:
            val = conn_cfg.get(key)
            if val is not None:
                setattr(args, key, str(val))

    if getattr(args, "password", None) is None:
        pw_env_name = conn_cfg.get("password_env")
        if pw_env_name:
            pw = os.environ.get(pw_env_name)
            if pw:
                args.password = pw
            else:
                LOG.warning(
                    "WARNING: connection.password_env = %r but $%s is empty or not set.",
                    pw_env_name,
                    pw_env_name,
                )
        if getattr(args, "password", None) is None:
            pw = conn_cfg.get("password")
            if pw is not None:
                args.password = str(pw)

    # Section keys (e.g. [dump].num_parts, [restore].index_jobs) are
    # only honoured when they correspond to a real CLI flag — i.e. an
    # attribute that argparse already attached to *args*.  Without the
    # ``hasattr`` guard a typo like ``num_part = 128`` would silently
    # create a new ``args.num_part`` attribute that nothing reads,
    # leaving the actual ``args.num_parts`` at its ``None`` default
    # (which then collapses to the parser's hardcoded fallback,
    # typically 1).  Loud warning beats silent misconfiguration over
    # multi-hour backups.
    for key, val in sect_cfg.items():
        if not hasattr(args, key):
            LOG.warning(
                "Config key [%s].%s is not a recognized argument; ignored.",
                section,
                key,
            )
            continue
        if getattr(args, key) is None:
            setattr(args, key, val)


def validate_connection_args(args: argparse.Namespace) -> None:
    """Abort with a clear message if required connection fields are missing."""
    missing = [
        f"--{k}" for k in ("host", "user", "password", "database") if not getattr(args, k, None)
    ]
    if missing:
        LOG.error(
            "ERROR: Missing required connection parameters: %s\n"
            "Provide via CLI flags or a backup.toml config file (see --config).",
            ", ".join(missing),
        )
        sys.exit(1)


def apply_logging_config(args: argparse.Namespace, config: dict) -> None:
    """Configure logging from CLI flags and/or ``[logging]`` config section.

    CLI ``--log-format`` / ``--log-level`` take precedence over the
    config file's ``[logging]`` section.
    """
    log_cfg = config.get("logging", {})
    setup_logging(
        args.log_format or log_cfg.get("format", "text"),
        args.log_level or log_cfg.get("level", "info"),
    )


def conn_from_args(args: argparse.Namespace) -> ConnInfo:
    """Build a ConnInfo from parsed (and config-merged) CLI arguments.

    Call ``validate_connection_args`` first to ensure required fields
    are present.  Port defaults to ``"5432"`` if not provided.
    """
    return ConnInfo(
        host=args.host,
        port=args.port or "5432",
        user=args.user,
        password=args.password,
        database=args.database,
    )
