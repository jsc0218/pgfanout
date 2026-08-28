"""Unit tests for the cancellation machinery in common.py.

Covers:
  * register_conn / unregister_conn bookkeeping
  * tracked_connection context manager (happy path, exception path,
    explicit-close semantics)
  * cancel_workers() fires cancel_safe() on every tracked connection
    even if one raises
  * cancel_workers() SIGTERMs tracked subprocesses (existing behavior —
    just asserting we didn't regress)

No live Postgres required — we use fakes for psycopg.Connection.
"""

import threading
from unittest import mock

import pytest

import common


class _FakeConn:
    """Minimal psycopg.Connection stand-in for cancellation tests."""

    def __init__(self, cancel_raises: Exception | None = None):
        self.cancel_calls = 0
        self.closed = False
        self._cancel_raises = cancel_raises
        self.committed = False
        self.rolled_back = False

    # context-manager protocol mirrors psycopg.Connection's
    # "commit on clean exit, rollback on exception" behavior
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.committed = True
        else:
            self.rolled_back = True
        self.closed = True
        return False

    def cancel_safe(self):
        self.cancel_calls += 1
        if self._cancel_raises is not None:
            raise self._cancel_raises


@pytest.fixture(autouse=True)
def _reset_registries():
    """Every test starts with empty cancel registries and cleared flag."""
    common._active_conns.clear()
    common._active_procs.clear()
    common._cancel_event.clear()
    yield
    common._active_conns.clear()
    common._active_procs.clear()
    common._cancel_event.clear()


def test_register_and_unregister_conn():
    pg = _FakeConn()
    common.register_conn(pg)
    assert pg in common._active_conns

    common.unregister_conn(pg)
    assert pg not in common._active_conns


def test_unregister_unknown_conn_is_noop():
    pg = _FakeConn()
    common.unregister_conn(pg)
    assert common._active_conns == []


def test_tracked_connection_happy_path(monkeypatch):
    fake = _FakeConn()
    monkeypatch.setattr(common.psycopg, "connect", lambda *a, **kw: fake)

    with common.tracked_connection("dummy=1") as pg:
        assert pg is fake
        assert fake in common._active_conns

    assert common._active_conns == []
    assert fake.committed
    assert fake.closed


def test_tracked_connection_unregisters_on_exception(monkeypatch):
    fake = _FakeConn()
    monkeypatch.setattr(common.psycopg, "connect", lambda *a, **kw: fake)

    with pytest.raises(RuntimeError, match="boom"):
        with common.tracked_connection("dummy=1"):
            raise RuntimeError("boom")

    assert common._active_conns == []
    assert fake.rolled_back
    assert fake.closed


def test_cancel_workers_calls_cancel_safe_on_all_tracked():
    a = _FakeConn()
    b = _FakeConn()
    common.register_conn(a)
    common.register_conn(b)

    common.cancel_workers()

    assert common.is_cancelled()
    assert a.cancel_calls == 1
    assert b.cancel_calls == 1


def test_cancel_workers_is_best_effort(monkeypatch):
    """If one connection's cancel_safe() raises, the others still run."""
    a = _FakeConn(cancel_raises=RuntimeError("already gone"))
    b = _FakeConn()
    c = _FakeConn()
    common.register_conn(a)
    common.register_conn(b)
    common.register_conn(c)

    common.cancel_workers()

    assert a.cancel_calls == 1
    assert b.cancel_calls == 1
    assert c.cancel_calls == 1


def test_cancel_workers_terminates_tracked_procs():
    proc = mock.MagicMock()
    common._active_procs.append(proc)

    common.cancel_workers()

    proc.terminate.assert_called_once()


def test_terminate_if_cancelled_is_noop_when_not_cancelled():
    """No spurious terminate() when no cancel is pending."""
    proc = mock.MagicMock()
    common._terminate_if_cancelled(proc)
    proc.terminate.assert_not_called()


def test_terminate_if_cancelled_terminates_when_cancel_set():
    """The Popen → append race: cancel arrived in the gap, helper rescues."""
    proc = mock.MagicMock()
    common._cancel_event.set()
    common._terminate_if_cancelled(proc)
    proc.terminate.assert_called_once()


def test_terminate_if_cancelled_swallows_oserror():
    """Already-dead subprocess raises OSError on terminate(); ignored."""
    proc = mock.MagicMock()
    proc.terminate.side_effect = OSError("no such process")
    common._cancel_event.set()
    # Must not raise.
    common._terminate_if_cancelled(proc)
    proc.terminate.assert_called_once()


def test_run_cancellable_terminates_on_already_cancelled():
    """Regression guard for the Popen → _active_procs.append race.

    If ``cancel_workers`` ran *before* the worker registered its
    fresh subprocess, the proc would otherwise run to completion
    uncancelled.  ``_terminate_if_cancelled`` after the append
    closes that window — verify by setting the cancel flag *before*
    calling ``run_cancellable`` and confirming the child exits
    via SIGTERM (returncode -15) rather than running its full sleep.
    """
    common._cancel_event.set()
    import time as _t

    t0 = _t.time()
    res = common.run_cancellable(["sleep", "30"])
    elapsed = _t.time() - t0
    # SIGTERM is signal 15; Popen reports it as returncode -15.
    assert res.returncode == -15, f"expected -15 (SIGTERM), got {res.returncode}"
    assert elapsed < 5.0, f"subprocess ran for {elapsed:.1f}s — race fix regressed"


def test_tracked_connection_is_thread_safe():
    """register/unregister from many threads must not corrupt the list."""

    def _fake_connect(*_a, **_kw):
        return _FakeConn()

    with mock.patch.object(common.psycopg, "connect", side_effect=_fake_connect):

        def worker():
            for _ in range(100):
                with common.tracked_connection("dummy=1"):
                    pass

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    assert common._active_conns == []
