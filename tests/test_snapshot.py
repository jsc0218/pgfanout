"""Unit tests for parallel_dump's snapshot-management functions.

Uses fakes for psycopg.Connection / cursor so we don't need a live
Postgres — the integration suite (test_parallel_backup.py) covers the
real end-to-end path.  These tests catch regressions in the control flow
(watchdog stop-event, probe-raises → cancel_workers(), graceful close
of a dead connection).
"""

import threading
from unittest import mock

import psycopg
import pytest

import parallel_dump as pd


class _FakeCursor:
    """Minimal context-manager cursor.  ``behave`` controls what execute does."""

    def __init__(self, behave):
        self._behave = behave

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        return self._behave(sql)

    def fetchone(self):
        return (1,)


class _FakeConn:
    def __init__(self, cursor_behave=lambda sql: None):
        self._cursor_behave = cursor_behave
        self.closed = False

    def cursor(self):
        return _FakeCursor(self._cursor_behave)

    def execute(self, sql, params=None):
        return self._cursor_behave(sql)

    def close(self):
        self.closed = True


# ---------------------------------------------------------------------------
# snapshot_watchdog
# ---------------------------------------------------------------------------


def test_watchdog_exits_when_stop_event_set():
    """Watchdog must not hang after stop_event is set."""
    holder = _FakeConn()
    stop_event = threading.Event()

    # Tiny interval so the test is fast.
    with mock.patch.object(pd, "WATCHDOG_INTERVAL", 0.05):
        t = threading.Thread(target=pd.snapshot_watchdog, args=(holder, stop_event))
        t.start()
        stop_event.set()
        t.join(timeout=2)
    assert not t.is_alive(), "watchdog did not honour stop_event"


def test_watchdog_cancels_workers_when_probe_raises():
    """If SELECT 1 raises on the holder, workers must be cancelled."""

    def probe_raises(_sql):
        raise psycopg.OperationalError("server closed the connection")

    holder = _FakeConn(cursor_behave=probe_raises)
    stop_event = threading.Event()

    with (
        mock.patch.object(pd, "WATCHDOG_INTERVAL", 0.05),
        mock.patch.object(pd, "cancel_workers") as cancel,
    ):
        t = threading.Thread(target=pd.snapshot_watchdog, args=(holder, stop_event))
        t.start()
        t.join(timeout=3)

    assert not t.is_alive(), "watchdog did not exit after probe failure"
    cancel.assert_called_once()


# ---------------------------------------------------------------------------
# close_snapshot_holder
# ---------------------------------------------------------------------------


def test_close_holder_commits_and_closes():
    calls = []

    def track(sql):
        calls.append(sql)

    holder = _FakeConn(cursor_behave=track)
    pd.close_snapshot_holder(holder)

    assert "COMMIT" in calls
    assert holder.closed is True


def test_close_holder_swallows_commit_error():
    """Already-dead connection must not raise from close_snapshot_holder."""

    def raise_err(_sql):
        raise psycopg.OperationalError("connection already closed")

    holder = _FakeConn(cursor_behave=raise_err)
    # Must not raise.
    pd.close_snapshot_holder(holder)
    assert holder.closed is True


def test_close_holder_unregisters_even_when_commit_fails():
    """close_snapshot_holder must always unregister from the cancel list,
    otherwise a stale reference leaks and cancel_workers() would try to
    cancel a dead connection forever.
    """
    import common

    def raise_err(_sql):
        raise psycopg.OperationalError("connection already closed")

    holder = _FakeConn(cursor_behave=raise_err)
    common.register_conn(holder)
    assert holder in common._active_conns

    pd.close_snapshot_holder(holder)

    assert holder not in common._active_conns


# ---------------------------------------------------------------------------
# export_snapshot error paths
# ---------------------------------------------------------------------------


def test_export_snapshot_rejects_bad_id_format():
    """Guards against an unexpected server response shape."""

    class _Conn(_FakeConn):
        def execute(self, sql, params=None):
            if "pg_export_snapshot" in sql:
                return _ExecResult(("not a valid id",))
            return None

    class _ExecResult:
        def __init__(self, row):
            self._row = row

        def fetchone(self):
            return self._row

    fake = _Conn()
    with mock.patch.object(psycopg, "connect", return_value=fake):
        with pytest.raises(SystemExit):
            pd.export_snapshot(mock.Mock(conninfo=lambda: ""))
    assert fake.closed is True


def test_export_snapshot_connect_failure_exits():
    with mock.patch.object(
        psycopg,
        "connect",
        side_effect=psycopg.OperationalError("refused"),
    ):
        with pytest.raises(SystemExit):
            pd.export_snapshot(mock.Mock(conninfo=lambda: ""))


# ---------------------------------------------------------------------------
# progress_monitor (dump side, shared-counter rewrite)
# ---------------------------------------------------------------------------


def test_progress_monitor_sums_shared_counter():
    """Monitor must read live bytes from the in-memory dict, not the fs."""
    bytes_written = {1: 0, 2: 0, 3: 0}
    stop_event = threading.Event()

    # Capture all LOG.info calls so we can inspect the reported totals.
    captured = []

    def fake_info(msg, *args, **kwargs):
        captured.append((msg, args, kwargs))

    with (
        mock.patch.object(pd, "PROGRESS_INTERVAL", 0.05),
        mock.patch.object(pd.LOG, "info", side_effect=fake_info),
    ):
        t = threading.Thread(
            target=pd.progress_monitor,
            args=(bytes_written, 3, stop_event),
            daemon=True,
        )
        t.start()

        # Simulate workers writing bytes concurrently.
        import time

        bytes_written[1] = 500_000
        bytes_written[2] = 200_000
        time.sleep(0.15)
        bytes_written[3] = 100_000
        time.sleep(0.15)

        stop_event.set()
        t.join(timeout=2)

    assert not t.is_alive(), "monitor did not honour stop_event"
    # At least one progress line must have been emitted with total > 0.
    totals = [
        kwargs.get("extra", {}).get("bytes")
        for _, _, kwargs in captured
        if kwargs.get("extra", {}).get("bytes") is not None
    ]
    assert totals, "expected at least one progress log line with bytes extra"
    assert max(totals) == 800_000
