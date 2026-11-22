"""Unit tests for parallel_restore phase orchestration.

Verifies that the database-level phase functions forward their
parameters correctly to ``restore_table_post_data``.  Specifically
locks in that ``--index-jobs`` reaches *both* Phase 2a (PKs / indexes
/ unique constraints) and Phase 2b (FK constraints) so a regression
cannot silently make FK validation serial again — that was a real
performance bug on multi-FK schemas, since ``pg_restore -j N`` is the
only way to validate FKs in parallel.

These tests stub :func:`parallel_restore.restore_table_post_data` so
they don't require a live Postgres or filesystem layout.
"""

import parallel_restore
from common import ConnInfo


def _stub_conn() -> ConnInfo:
    return ConnInfo(host="h", port="5432", user="u", password="p", database="d")


def _capture_post_data_calls(monkeypatch) -> list[dict]:
    """Replace ``restore_table_post_data`` with a recorder.

    The fake mirrors the real signature (positional + keyword) so the
    callers are exercised exactly as in production.  Each invocation
    appends a dict of normalised kwargs to *calls*, which the test
    body then asserts against — abstracting away whether the caller
    used positional or keyword form.
    """
    calls: list[dict] = []

    def fake(
        conn,
        input_dir,
        index_jobs: int = 1,
        skip_fk: bool = False,
        only_fk: bool = False,
    ) -> None:
        calls.append(
            {
                "input_dir": input_dir,
                "index_jobs": index_jobs,
                "skip_fk": skip_fk,
                "only_fk": only_fk,
            }
        )

    monkeypatch.setattr(parallel_restore, "restore_table_post_data", fake)
    return calls


def test_phase_2a_forwards_index_jobs(monkeypatch):
    """Phase 2a: every table receives the user's ``--index-jobs`` value.

    Confirms the existing wiring (index_jobs is the third positional
    arg of ``restore_table_post_data``) so a future refactor that
    introduces a new parameter ahead of it can't silently shift the
    value into a different slot.
    """
    calls = _capture_post_data_calls(monkeypatch)
    work = [("public", "t1", "/d1"), ("public", "t2", "/d2")]

    parallel_restore._phase_2a_indexes(_stub_conn(), work, index_jobs=8)

    assert len(calls) == 2
    for c in calls:
        assert c["index_jobs"] == 8
        assert c["skip_fk"] is True
        assert c["only_fk"] is False
    assert [c["input_dir"] for c in calls] == ["/d1", "/d2"]


def test_phase_2b_forwards_index_jobs(monkeypatch):
    """Phase 2b: ``--index-jobs`` parallelises FK validation, too.

    Regression guard.  Previously ``_phase_2b_foreign_keys`` called
    ``restore_table_post_data(conn, table_dir, only_fk=True)`` and let
    ``index_jobs`` default to 1, so FK constraints validated serially
    even when the user passed ``--index-jobs N`` and Phase 2a was
    happily fanning out across N workers.  Multi-FK schemas paid an
    O(num_fks * validate_time) tail that the rest of the restore had
    already worked hard to eliminate.

    Lock in that the value reaches every table.
    """
    calls = _capture_post_data_calls(monkeypatch)
    work = [("public", "orders", "/dump/public/orders")]

    parallel_restore._phase_2b_foreign_keys(_stub_conn(), work, index_jobs=8)

    assert len(calls) == 1
    assert calls[0]["index_jobs"] == 8
    assert calls[0]["only_fk"] is True
    assert calls[0]["skip_fk"] is False


def test_phase_2b_iterates_all_tables(monkeypatch):
    """Phase 2b touches every table in *work*, not just the first.

    Belt-and-braces: a malformed early-exit (e.g. an accidental
    ``return`` inside the loop) would leave half the FK graph
    unvalidated, with no row-level test in the integration suite
    catching it on small datasets.
    """
    calls = _capture_post_data_calls(monkeypatch)
    work = [
        ("public", "orders", "/d/public/orders"),
        ("public", "shipments", "/d/public/shipments"),
        ("billing", "invoices", "/d/billing/invoices"),
    ]

    parallel_restore._phase_2b_foreign_keys(_stub_conn(), work, index_jobs=4)

    assert [c["input_dir"] for c in calls] == [
        "/d/public/orders",
        "/d/public/shipments",
        "/d/billing/invoices",
    ]
    for c in calls:
        assert c["index_jobs"] == 4
        assert c["only_fk"] is True


def test_phase_2a_and_2b_are_symmetric(monkeypatch):
    """Both phases forward the same ``index_jobs`` for the same work list.

    Without this guard, a refactor that updated only one phase would
    quietly skew parallelism between PK creation and FK validation —
    masked by integration tests that always pass ``index_jobs=1`` or
    ``index_jobs=2`` and don't measure phase-level timings.
    """
    work = [("public", "t1", "/d1"), ("public", "t2", "/d2")]

    a_calls = _capture_post_data_calls(monkeypatch)
    parallel_restore._phase_2a_indexes(_stub_conn(), work, index_jobs=16)
    a_jobs = [c["index_jobs"] for c in a_calls]

    b_calls = _capture_post_data_calls(monkeypatch)
    parallel_restore._phase_2b_foreign_keys(_stub_conn(), work, index_jobs=16)
    b_jobs = [c["index_jobs"] for c in b_calls]

    assert a_jobs == b_jobs == [16, 16]
