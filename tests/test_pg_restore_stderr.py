"""Unit tests for ``_classify_pg_restore_stderr``.

Pure-Python tests — no live Postgres required.  Exercise the stderr
classifier used by :func:`parallel_restore._restore_global_dump` to
decide whether a non-zero pg_restore exit was due to benign "already
exists" triplets (re-applying globals against a DB that already has
them) or a genuine failure that must abort the restore.
"""

from parallel_restore import _classify_pg_restore_stderr


def test_empty_stderr_returns_empty():
    assert _classify_pg_restore_stderr("") == []


def test_blank_lines_ignored():
    assert _classify_pg_restore_stderr("\n\n   \n") == []


def test_only_summary_line_is_benign():
    """The summary line alone carries no signal — drop it."""
    stderr = "pg_restore: warning: errors ignored on restore: 0\n"
    assert _classify_pg_restore_stderr(stderr) == []


def test_single_already_exists_triplet_suppressed():
    """The canonical three-line block must be fully suppressed.

    This is the exact regression that motivated the classifier: the
    old per-line filter surfaced the TOC-entry and Command-was lines
    as "unexpected", producing a spurious sys.exit(1) on any restore
    into a DB with pre-existing globals.
    """
    stderr = (
        "pg_restore: from TOC entry 4; 3079 16580 EXTENSION citext \n"
        'pg_restore: error: could not execute query: ERROR:  extension "citext" already exists\n'
        "Command was: CREATE EXTENSION IF NOT EXISTS citext WITH SCHEMA public;\n"
        "pg_restore: warning: errors ignored on restore: 1\n"
    )
    assert _classify_pg_restore_stderr(stderr) == []


def test_multiple_already_exists_triplets_all_suppressed():
    stderr = (
        "pg_restore: from TOC entry 4; 3079 16580 EXTENSION citext \n"
        'pg_restore: error: ERROR:  extension "citext" already exists\n'
        "Command was: CREATE EXTENSION citext;\n"
        "pg_restore: from TOC entry 12; 1247 1 TYPE status_type\n"
        'pg_restore: error: ERROR:  type "status_type" already exists\n'
        "Command was: CREATE TYPE status_type AS ENUM ('a', 'b');\n"
        "pg_restore: warning: errors ignored on restore: 2\n"
    )
    assert _classify_pg_restore_stderr(stderr) == []


def test_real_error_surfaces_full_block():
    """A non-already-exists error must preserve its entire context."""
    stderr = (
        "pg_restore: from TOC entry 99; 1255 99999 FUNCTION secret_fn\n"
        "pg_restore: error: could not execute query: ERROR:  permission denied for schema private\n"
        "Command was: CREATE FUNCTION private.secret_fn() RETURNS void ...\n"
        "pg_restore: warning: errors ignored on restore: 1\n"
    )
    groups = _classify_pg_restore_stderr(stderr)
    assert len(groups) == 1
    assert groups[0] == [
        "pg_restore: from TOC entry 99; 1255 99999 FUNCTION secret_fn",
        "pg_restore: error: could not execute query: ERROR:  permission denied for schema private",
        "Command was: CREATE FUNCTION private.secret_fn() RETURNS void ...",
    ]


def test_mixed_benign_and_real_keeps_only_real():
    """Benign triplets are dropped; real errors retain their context."""
    stderr = (
        "pg_restore: from TOC entry 4; 3079 16580 EXTENSION citext \n"
        'pg_restore: error: ERROR:  extension "citext" already exists\n'
        "Command was: CREATE EXTENSION citext;\n"
        "pg_restore: from TOC entry 50; 1247 50000 ROLE admin\n"
        'pg_restore: error: ERROR:  role "admin" does not exist\n'
        "Command was: ALTER TABLE foo OWNER TO admin;\n"
        "pg_restore: warning: errors ignored on restore: 2\n"
    )
    groups = _classify_pg_restore_stderr(stderr)
    assert len(groups) == 1
    assert groups[0][0].endswith("ROLE admin")
    assert 'role "admin" does not exist' in groups[0][1]
    assert groups[0][2] == "Command was: ALTER TABLE foo OWNER TO admin;"


def test_pre_toc_error_surfaces():
    """Errors without a ``from TOC entry`` header still surface.

    Covers pre-TOC failures like a missing input file or a bad
    archive format — there is no TOC context in that case, but the
    error itself is still meaningful.
    """
    stderr = 'pg_restore: error: could not open input file "foo.dump": No such file or directory\n'
    groups = _classify_pg_restore_stderr(stderr)
    assert len(groups) == 1
    assert groups[0] == [
        'pg_restore: error: could not open input file "foo.dump": No such file or directory'
    ]


def test_summary_alone_not_attributed_to_preceding_group():
    """The summary line must close the current group, not attach to it.

    If the summary got swept into a preceding real-error group, the
    subsequent "already exists" search in the group would still drop
    it correctly — but a later maintenance change might break this
    invariant.  Lock in the boundary behaviour explicitly.
    """
    stderr = (
        "pg_restore: from TOC entry 1; 1 1 FUNCTION f\n"
        "pg_restore: error: ERROR:  syntax error\n"
        "Command was: CREATE FUNCTION f() RETURNS ...;\n"
        "pg_restore: warning: errors ignored on restore: 1\n"
    )
    groups = _classify_pg_restore_stderr(stderr)
    assert len(groups) == 1
    assert not any("errors ignored on restore" in line for line in groups[0])
