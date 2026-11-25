"""Unit tests for src/pgfanout/storage.py.

No live Postgres or S3 needed — the local backend is exercised end-to-end
against a tmp_path fixture, and the S3 backend is driven by a botocore
Stubber (pure in-process client stub that records expected requests and
returns canned responses).
"""

import io
import os
import signal
import threading
import time

import boto3
import pytest
from botocore.exceptions import ClientError
from botocore.stub import Stubber

from pgfanout.storage import (
    Blob,
    LocalBlob,
    S3Blob,
    _is_not_found,
    is_dir,
    is_file,
    is_s3_uri,
    list_dir,
    localize,
    make_blob,
    make_s3_client,
    makedirs,
    parse_s3_uri,
)
from pgfanout.storage import (
    join as storage_join,
)


def test_local_blob_write_read_roundtrip(tmp_path):
    path = tmp_path / "part1.bin"
    blob = LocalBlob(str(path))

    payload = b"\x01\x02\x03" + b"Z" * 1024 + b"\xff\xff"
    with blob.open_write() as f:
        f.write(payload)

    assert blob.exists()
    assert blob.size() == len(payload)

    with blob.open_read() as f:
        assert f.read() == payload


def test_local_blob_creates_parent_dirs(tmp_path):
    """open_write must mkdir-p intermediate directories."""
    nested = tmp_path / "a" / "b" / "c" / "part.bin"
    blob = LocalBlob(str(nested))
    with blob.open_write() as f:
        f.write(b"hello")
    assert os.path.isfile(str(nested))
    assert blob.size() == 5


def test_size_on_missing_blob_is_zero(tmp_path):
    blob = LocalBlob(str(tmp_path / "missing.bin"))
    assert blob.size() == 0
    assert not blob.exists()


def test_delete_is_idempotent(tmp_path):
    path = tmp_path / "gone.bin"
    path.write_bytes(b"x")
    blob = LocalBlob(str(path))
    blob.delete()
    assert not blob.exists()
    # Deleting again must not raise.
    blob.delete()


def test_uri_returns_path_for_local(tmp_path):
    blob = LocalBlob(str(tmp_path / "p.bin"))
    assert blob.uri == str(tmp_path / "p.bin")


def test_make_blob_returns_local_for_plain_path(tmp_path):
    blob = make_blob(str(tmp_path / "x.bin"))
    assert isinstance(blob, LocalBlob)
    assert isinstance(blob, Blob)


def test_local_blob_overwrite(tmp_path):
    """open_write must truncate an existing blob (matches previous behaviour)."""
    path = tmp_path / "part.bin"
    blob = LocalBlob(str(path))

    with blob.open_write() as f:
        f.write(b"old_content_is_longer")
    with blob.open_write() as f:
        f.write(b"new")

    assert blob.size() == 3
    with blob.open_read() as f:
        assert f.read() == b"new"


# ---------------------------------------------------------------------------
# parse_s3_uri
# ---------------------------------------------------------------------------


def test_parse_s3_uri_basic():
    assert parse_s3_uri("s3://mybucket/some/key.bin") == (
        "mybucket",
        "some/key.bin",
    )


def test_parse_s3_uri_deep_key():
    assert parse_s3_uri("s3://b/a/b/c/d/file") == ("b", "a/b/c/d/file")


@pytest.mark.parametrize(
    "bad_uri",
    [
        "s3://bucket",  # no key
        "s3://bucket/",  # empty key
        "s3:///key",  # no bucket
        "http://bucket/key",  # wrong scheme
        "bucket/key",  # no scheme
    ],
)
def test_parse_s3_uri_rejects_malformed(bad_uri):
    with pytest.raises(ValueError):
        parse_s3_uri(bad_uri)


# ---------------------------------------------------------------------------
# S3Blob: helpers
# ---------------------------------------------------------------------------


def _stub_client():
    """Return (stubbed_client, stubber) both activated and ready to use.

    Uses botocore's Stubber instead of moto so we don't add another test
    dependency.  The returned client looks exactly like a real boto3
    client to code under test, but every API call is intercepted and
    compared against a pre-programmed list of expected requests.
    """
    client = boto3.client(
        "s3",
        region_name="us-east-1",
        aws_access_key_id="AKIATEST",
        aws_secret_access_key="secret",
    )
    stubber = Stubber(client)
    stubber.activate()
    return client, stubber


# ---------------------------------------------------------------------------
# S3Blob: read / HEAD / delete paths
# ---------------------------------------------------------------------------


def test_s3_blob_size_returns_content_length():
    client, stub = _stub_client()
    stub.add_response(
        "head_object",
        {"ContentLength": 12345, "ETag": '"abc"'},
        expected_params={"Bucket": "b", "Key": "k"},
    )
    blob = S3Blob("b", "k", client=client)
    assert blob.size() == 12345
    stub.assert_no_pending_responses()


def test_s3_blob_size_returns_zero_on_404():
    client, stub = _stub_client()
    stub.add_client_error(
        "head_object",
        service_error_code="404",
        http_status_code=404,
        expected_params={"Bucket": "b", "Key": "missing"},
    )
    blob = S3Blob("b", "missing", client=client)
    assert blob.size() == 0


def test_s3_blob_exists_true_and_false():
    client, stub = _stub_client()
    stub.add_response(
        "head_object",
        {"ContentLength": 1, "ETag": '"x"'},
        expected_params={"Bucket": "b", "Key": "present"},
    )
    stub.add_client_error(
        "head_object",
        service_error_code="404",
        http_status_code=404,
        expected_params={"Bucket": "b", "Key": "missing"},
    )
    assert S3Blob("b", "present", client=client).exists() is True
    assert S3Blob("b", "missing", client=client).exists() is False


def test_s3_blob_size_propagates_non_404_errors():
    """A permission error must NOT be silently swallowed as size==0."""
    client, stub = _stub_client()
    stub.add_client_error(
        "head_object",
        service_error_code="AccessDenied",
        http_status_code=403,
        expected_params={"Bucket": "b", "Key": "k"},
    )
    blob = S3Blob("b", "k", client=client)
    with pytest.raises(ClientError):
        blob.size()


def test_s3_blob_delete_swallows_not_found():
    client, stub = _stub_client()
    stub.add_client_error(
        "delete_object",
        service_error_code="NoSuchKey",
        http_status_code=404,
        expected_params={"Bucket": "b", "Key": "gone"},
    )
    # Must not raise.
    S3Blob("b", "gone", client=client).delete()


def test_s3_blob_delete_success():
    client, stub = _stub_client()
    stub.add_response(
        "delete_object",
        {},
        expected_params={"Bucket": "b", "Key": "k"},
    )
    S3Blob("b", "k", client=client).delete()
    stub.assert_no_pending_responses()


def test_s3_blob_uri_format():
    blob = S3Blob("my-bucket", "some/deep/key")
    assert blob.uri == "s3://my-bucket/some/deep/key"


def test_s3_blob_read_returns_streamed_body():
    client, stub = _stub_client()
    payload = b"\x01\x02\x03DATA\xff"
    stub.add_response(
        "get_object",
        {"Body": io.BytesIO(payload), "ContentLength": len(payload)},
        expected_params={"Bucket": "b", "Key": "k"},
    )
    blob = S3Blob("b", "k", client=client)
    with blob.open_read() as src:
        assert src.read() == payload
    stub.assert_no_pending_responses()


def test_s3_blob_read_in_chunks():
    """copy_part reads in RESTORE_CHUNK_SIZE chunks — make sure .read(n) works."""
    client, stub = _stub_client()
    payload = b"A" * 100 + b"B" * 100 + b"C" * 50
    stub.add_response(
        "get_object",
        {"Body": io.BytesIO(payload), "ContentLength": len(payload)},
        expected_params={"Bucket": "b", "Key": "k"},
    )
    blob = S3Blob("b", "k", client=client)
    collected = bytearray()
    with blob.open_read() as src:
        while True:
            chunk = src.read(64)
            if not chunk:
                break
            collected.extend(chunk)
    assert bytes(collected) == payload


# ---------------------------------------------------------------------------
# S3Blob: write / multipart paths
# ---------------------------------------------------------------------------


# Small part size so our tests exercise the multi-part path without
# allocating megabytes.  S3Blob enforces the 5 MiB real-world minimum;
# we monkeypatch it down to 16 bytes per test via a direct constructor.


def _stub_multipart_upload(stub, bucket, key, parts: list[bytes], upload_id="UP1"):
    """Queue stub responses for a full multipart upload sequence."""
    stub.add_response(
        "create_multipart_upload",
        {"UploadId": upload_id, "Bucket": bucket, "Key": key},
        expected_params={"Bucket": bucket, "Key": key},
    )
    for i, body in enumerate(parts, 1):
        stub.add_response(
            "upload_part",
            {"ETag": f'"etag-{i}"'},
            expected_params={
                "Bucket": bucket,
                "Key": key,
                "PartNumber": i,
                "UploadId": upload_id,
                "Body": body,
            },
        )
    stub.add_response(
        "complete_multipart_upload",
        {"Bucket": bucket, "Key": key, "ETag": '"final"'},
        expected_params={
            "Bucket": bucket,
            "Key": key,
            "UploadId": upload_id,
            "MultipartUpload": {
                "Parts": [
                    {"PartNumber": i, "ETag": f'"etag-{i}"'} for i in range(1, len(parts) + 1)
                ]
            },
        },
    )


@pytest.fixture(autouse=True)
def _serial_s3_uploads(monkeypatch):
    """The Stubber-based tests queue responses in a fixed order, so upload in the
    calling thread there. The background-upload tests below pass upload_threads
    explicitly, and test_s3_upload_threads_env sets the variable itself."""
    from pgfanout import storage

    monkeypatch.setenv(storage.S3_UPLOAD_THREADS_ENV, "0")


@pytest.fixture
def tiny_part_size(monkeypatch):
    """Lower the S3 minimum part size so streaming tests can use 16 B parts.

    S3's real 5 MiB minimum would mean every streaming test allocates
    tens of MiB of memory.  The production check still exists — see
    :func:`test_s3_blob_rejects_too_small_part_size`, which doesn't use
    this fixture.
    """
    from pgfanout import storage

    monkeypatch.setattr(storage, "S3_MIN_PART_SIZE", 1)
    yield 16


def test_s3_blob_write_single_part(tiny_part_size):
    """Write smaller than part_size → one part + complete."""
    client, stub = _stub_client()
    payload = b"X" * 10
    # part_size 16 > payload 10, so one partial part flushed at close.
    _stub_multipart_upload(stub, "b", "k", [payload])

    blob = S3Blob("b", "k", client=client, part_size=tiny_part_size)
    with blob.open_write() as sink:
        sink.write(payload)
    stub.assert_no_pending_responses()


def test_s3_blob_write_multipart_splits_correctly(tiny_part_size):
    """Stream 40 bytes through 16-byte parts → 16 + 16 + 8."""
    client, stub = _stub_client()
    full = bytes(range(40))
    _stub_multipart_upload(stub, "b", "k", [full[0:16], full[16:32], full[32:40]])

    blob = S3Blob("b", "k", client=client, part_size=tiny_part_size)
    with blob.open_write() as sink:
        # Feed the buffer in small irregular writes to exercise the
        # accumulator path.
        sink.write(full[0:5])
        sink.write(full[5:25])
        sink.write(full[25:28])
        sink.write(full[28:40])
    stub.assert_no_pending_responses()


def test_s3_blob_write_exception_aborts_upload(tiny_part_size):
    """Exception inside `with` must abort the multipart, not complete it."""
    client, stub = _stub_client()
    stub.add_response(
        "create_multipart_upload",
        {"UploadId": "UP1", "Bucket": "b", "Key": "k"},
        expected_params={"Bucket": "b", "Key": "k"},
    )
    stub.add_response(
        "abort_multipart_upload",
        {},
        expected_params={"Bucket": "b", "Key": "k", "UploadId": "UP1"},
    )

    blob = S3Blob("b", "k", client=client, part_size=tiny_part_size)
    with pytest.raises(RuntimeError, match="simulated failure"):
        with blob.open_write() as sink:
            sink.write(b"abc")
            raise RuntimeError("simulated failure")

    stub.assert_no_pending_responses()


def test_s3_blob_write_empty_falls_back_to_put_object(tiny_part_size):
    """Zero writes must still produce an object (via abort + PutObject)."""
    client, stub = _stub_client()
    stub.add_response(
        "create_multipart_upload",
        {"UploadId": "UP1", "Bucket": "b", "Key": "empty"},
        expected_params={"Bucket": "b", "Key": "empty"},
    )
    stub.add_response(
        "abort_multipart_upload",
        {},
        expected_params={"Bucket": "b", "Key": "empty", "UploadId": "UP1"},
    )
    stub.add_response(
        "put_object",
        {"ETag": '"empty"'},
        expected_params={"Bucket": "b", "Key": "empty", "Body": b""},
    )
    blob = S3Blob("b", "empty", client=client, part_size=tiny_part_size)
    with blob.open_write():
        pass
    stub.assert_no_pending_responses()


def test_s3_blob_rejects_too_small_part_size():
    """S3 spec: parts must be >= 5 MiB.  Guard against misconfig."""
    with pytest.raises(ValueError, match=">="):
        # Construct a writer directly to bypass the _part_size override
        # trick our other tests use.
        from pgfanout.storage import _S3MultipartWriter

        _S3MultipartWriter(client=None, bucket="b", key="k", part_size=1024)


def test_s3_blob_write_abort_on_complete_failure(tiny_part_size):
    """CompleteMultipartUpload failure must abort so parts don't linger.

    Without the abort, the server retains the uploaded parts indefinitely
    (until a bucket lifecycle policy reaps them), quietly running up the
    storage bill for every failed completion.
    """
    client, stub = _stub_client()
    stub.add_response(
        "create_multipart_upload",
        {"UploadId": "UP1", "Bucket": "b", "Key": "k"},
        expected_params={"Bucket": "b", "Key": "k"},
    )
    stub.add_response(
        "upload_part",
        {"ETag": '"etag-1"'},
        expected_params={
            "Bucket": "b",
            "Key": "k",
            "PartNumber": 1,
            "UploadId": "UP1",
            "Body": b"hello",
        },
    )
    stub.add_client_error(
        "complete_multipart_upload",
        service_error_code="InternalError",
        service_message="boom",
    )
    stub.add_response(
        "abort_multipart_upload",
        {},
        expected_params={"Bucket": "b", "Key": "k", "UploadId": "UP1"},
    )

    blob = S3Blob("b", "k", client=client, part_size=tiny_part_size)
    with pytest.raises(ClientError):
        with blob.open_write() as sink:
            sink.write(b"hello")

    stub.assert_no_pending_responses()


def test_s3_blob_write_abort_on_keyboard_interrupt(tiny_part_size):
    """SIGINT during ``complete_multipart_upload`` must still abort.

    Regression guard for the ``except Exception`` → ``except
    BaseException`` fix.  Both ``upload_part`` and
    ``complete_multipart_upload`` are network round-trips that can
    take many seconds against a real S3 endpoint, which is exactly
    when an operator's Ctrl+C is most likely to land.  The earlier
    ``except Exception`` would skip the abort because
    ``KeyboardInterrupt`` is not a subclass of ``Exception`` — the
    multipart upload would dangle on the server until lifecycle
    policy reaped it (chargeable storage in the meantime).  Verify
    that with ``except BaseException`` the abort fires *and* the
    interrupt propagates.
    """
    client, stub = _stub_client()
    stub.add_response(
        "create_multipart_upload",
        {"UploadId": "UP1", "Bucket": "b", "Key": "k"},
        expected_params={"Bucket": "b", "Key": "k"},
    )
    stub.add_response(
        "upload_part",
        {"ETag": '"etag-1"'},
        expected_params={
            "Bucket": "b",
            "Key": "k",
            "PartNumber": 1,
            "UploadId": "UP1",
            "Body": b"hello",
        },
    )

    # Patch complete_multipart_upload to raise KeyboardInterrupt — the
    # canonical BaseException-but-not-Exception case.
    real_complete = client.complete_multipart_upload

    def _interrupt_complete(*_a, **_kw):
        raise KeyboardInterrupt("simulated SIGINT mid-upload")

    client.complete_multipart_upload = _interrupt_complete

    stub.add_response(
        "abort_multipart_upload",
        {},
        expected_params={"Bucket": "b", "Key": "k", "UploadId": "UP1"},
    )

    blob = S3Blob("b", "k", client=client, part_size=tiny_part_size)
    with pytest.raises(KeyboardInterrupt, match="simulated SIGINT"):
        with blob.open_write() as sink:
            sink.write(b"hello")

    # abort_multipart_upload was registered as the next stubbed response;
    # if it didn't fire (i.e. the cleanup was skipped) this assertion
    # would fail with "pending responses".
    stub.assert_no_pending_responses()
    # Restore the patched method just in case the stub's __exit__ inspects it.
    client.complete_multipart_upload = real_complete


def test_s3_blob_write_abort_on_trailing_flush_failure(tiny_part_size):
    """UploadPart failure during the final flush must also abort.

    The trailing-buffer ``_flush_part`` call runs inside ``__exit__``
    on the happy path (no exception from the ``with`` body), so a
    failure there does not hit the ``exc_type is not None`` branch.
    Must still clean up.
    """
    client, stub = _stub_client()
    stub.add_response(
        "create_multipart_upload",
        {"UploadId": "UP1", "Bucket": "b", "Key": "k"},
        expected_params={"Bucket": "b", "Key": "k"},
    )
    stub.add_client_error(
        "upload_part",
        service_error_code="InternalError",
        service_message="boom",
    )
    stub.add_response(
        "abort_multipart_upload",
        {},
        expected_params={"Bucket": "b", "Key": "k", "UploadId": "UP1"},
    )

    blob = S3Blob("b", "k", client=client, part_size=tiny_part_size)
    with pytest.raises(ClientError):
        with blob.open_write() as sink:
            sink.write(b"short")  # smaller than part_size → flushed at close

    stub.assert_no_pending_responses()


# ---------------------------------------------------------------------------
# make_blob / make_s3_client
# ---------------------------------------------------------------------------


def test_make_blob_returns_s3_for_s3_uri():
    # Passing a fake client prevents make_s3_client from being invoked
    # (which would try to hit env-configured endpoints).
    client, _stub = _stub_client()
    blob = make_blob("s3://bucket/key", s3_client=client)
    assert isinstance(blob, S3Blob)
    assert blob.uri == "s3://bucket/key"


def test_make_blob_rejects_malformed_s3_uri():
    with pytest.raises(ValueError):
        make_blob("s3://bucket-no-key")


def test_make_s3_client_honours_env_endpoint(monkeypatch):
    monkeypatch.setenv("AWS_ENDPOINT_URL_S3", "https://ceph.example:7483")
    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAFAKE")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
    client = make_s3_client()
    assert client.meta.endpoint_url == "https://ceph.example:7483"
    assert client.meta.region_name == "eu-west-1"


def test_make_s3_client_verify_false(monkeypatch):
    monkeypatch.setenv("AWS_S3_VERIFY_SSL", "false")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAFAKE")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
    client = make_s3_client(endpoint_url="https://x")
    # botocore stores verify on the client's internal _endpoint config;
    # the public surface is client.meta.config, but verify isn't
    # exposed there.  Instead assert via a request-level test: the
    # request args dict includes verify=False.
    assert client._endpoint.http_session._verify is False


# ---------------------------------------------------------------------------
# storage.join — URI-aware path joiner
# ---------------------------------------------------------------------------


def test_join_local_matches_os_path_join(tmp_path):
    assert storage_join(str(tmp_path), "a", "b", "c") == os.path.join(str(tmp_path), "a", "b", "c")


def test_join_s3_simple():
    assert storage_join("s3://bucket/prefix", "part1.bin") == ("s3://bucket/prefix/part1.bin")


def test_join_s3_trailing_slash_on_base():
    """Trailing / on base must not produce a // in the result."""
    assert storage_join("s3://bucket/prefix/", "part1.bin") == ("s3://bucket/prefix/part1.bin")


def test_join_s3_leading_slash_on_part():
    """Leading / on a part must not override the base (unlike os.path.join)."""
    assert storage_join("s3://bucket/prefix", "/part1.bin") == ("s3://bucket/prefix/part1.bin")


def test_join_s3_multiple_parts():
    assert storage_join("s3://bucket/prefix", "schema", "table", "part1.bin") == (
        "s3://bucket/prefix/schema/table/part1.bin"
    )


def test_join_s3_no_parts_returns_normalized_base():
    assert storage_join("s3://bucket/prefix/") == "s3://bucket/prefix"


def test_join_s3_skips_empty_parts():
    assert storage_join("s3://bucket/prefix", "", "part1.bin", "") == (
        "s3://bucket/prefix/part1.bin"
    )


def test_is_not_found_recognises_shapes():
    """Tiny unit test for the error-discriminator helper."""

    class _Err(Exception):
        def __init__(self, code=None, status=None):
            self.response = {
                "Error": {"Code": code} if code else {},
                "ResponseMetadata": {"HTTPStatusCode": status} if status else {},
            }

    assert _is_not_found(_Err(code="404"))
    assert _is_not_found(_Err(code="NoSuchKey"))
    assert _is_not_found(_Err(code="NotFound"))
    assert _is_not_found(_Err(status=404))
    assert not _is_not_found(_Err(code="AccessDenied"))
    assert not _is_not_found(RuntimeError("unrelated"))


# ---------------------------------------------------------------------------
# list_dir / is_dir / is_file / makedirs — local paths
# ---------------------------------------------------------------------------


def test_list_dir_local(tmp_path):
    (tmp_path / "a").mkdir()
    (tmp_path / "b.txt").write_text("x")
    (tmp_path / "c").mkdir()
    assert list_dir(str(tmp_path)) == ["a", "b.txt", "c"]


def test_list_dir_local_missing_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        list_dir(str(tmp_path / "nope"))


def test_list_dir_local_sorted(tmp_path):
    """Contract: results must come back alphabetically sorted."""
    for name in ["zzz", "aaa", "mmm", "bbb"]:
        (tmp_path / name).touch()
    assert list_dir(str(tmp_path)) == ["aaa", "bbb", "mmm", "zzz"]


def test_is_dir_local(tmp_path):
    sub = tmp_path / "sub"
    sub.mkdir()
    assert is_dir(str(sub))
    assert not is_dir(str(tmp_path / "nope"))
    (tmp_path / "f.txt").touch()
    assert not is_dir(str(tmp_path / "f.txt"))


def test_is_file_local(tmp_path):
    f = tmp_path / "a.bin"
    f.write_bytes(b"1")
    assert is_file(str(f))
    assert not is_file(str(tmp_path / "missing.bin"))
    (tmp_path / "d").mkdir()
    assert not is_file(str(tmp_path / "d"))


def test_makedirs_local_creates_nested(tmp_path):
    target = tmp_path / "a" / "b" / "c"
    makedirs(str(target))
    assert target.is_dir()


def test_makedirs_local_is_idempotent(tmp_path):
    target = tmp_path / "d"
    makedirs(str(target))
    # Second call must not raise (exist_ok default True).
    makedirs(str(target))


# ---------------------------------------------------------------------------
# list_dir / is_dir / is_file / makedirs — S3
# ---------------------------------------------------------------------------


def test_list_dir_s3_delimited():
    """list_objects_v2 with Delimiter='/' returns both files and subdirs."""
    client, stub = _stub_client()
    stub.add_response(
        "list_objects_v2",
        {
            "IsTruncated": False,
            "KeyCount": 3,
            "Contents": [
                {"Key": "dump1/pre_data.sql"},
                {"Key": "dump1/sequences.sql"},
                # "directory marker" — key equals the request prefix; must
                # be filtered out because it's not a real child.
                {"Key": "dump1/"},
            ],
            "CommonPrefixes": [
                {"Prefix": "dump1/public/"},
                {"Prefix": "dump1/reporting/"},
            ],
        },
        expected_params={
            "Bucket": "b",
            "Prefix": "dump1/",
            "Delimiter": "/",
        },
    )
    assert list_dir("s3://b/dump1", s3_client=client) == [
        "pre_data.sql",
        "public",
        "reporting",
        "sequences.sql",
    ]


def test_list_dir_s3_trailing_slash_equivalent():
    """s3://b/pfx and s3://b/pfx/ both resolve to the same prefix listing."""
    client, stub = _stub_client()
    stub.add_response(
        "list_objects_v2",
        {"IsTruncated": False, "KeyCount": 0, "Contents": [], "CommonPrefixes": []},
        expected_params={"Bucket": "b", "Prefix": "pfx/", "Delimiter": "/"},
    )
    assert list_dir("s3://b/pfx/", s3_client=client) == []


def test_list_dir_s3_missing_returns_empty():
    """An unused S3 prefix isn't an error — listdir returns []."""
    client, stub = _stub_client()
    stub.add_response(
        "list_objects_v2",
        {"IsTruncated": False, "KeyCount": 0},
        expected_params={"Bucket": "b", "Prefix": "nope/", "Delimiter": "/"},
    )
    assert list_dir("s3://b/nope", s3_client=client) == []


def test_list_dir_s3_paginates():
    """>1000 objects must paginate transparently."""
    client, stub = _stub_client()
    # Page 1: truncated with continuation token.
    stub.add_response(
        "list_objects_v2",
        {
            "IsTruncated": True,
            "NextContinuationToken": "TOK",
            "KeyCount": 2,
            "Contents": [{"Key": "p/a"}, {"Key": "p/b"}],
            "CommonPrefixes": [{"Prefix": "p/sub1/"}],
        },
        expected_params={"Bucket": "b", "Prefix": "p/", "Delimiter": "/"},
    )
    # Page 2: final.
    stub.add_response(
        "list_objects_v2",
        {
            "IsTruncated": False,
            "KeyCount": 1,
            "Contents": [{"Key": "p/c"}],
            "CommonPrefixes": [{"Prefix": "p/sub2/"}],
        },
        expected_params={
            "Bucket": "b",
            "Prefix": "p/",
            "Delimiter": "/",
            "ContinuationToken": "TOK",
        },
    )
    assert list_dir("s3://b/p", s3_client=client) == [
        "a",
        "b",
        "c",
        "sub1",
        "sub2",
    ]


def test_is_dir_s3_true_when_content_exists():
    client, stub = _stub_client()
    stub.add_response(
        "list_objects_v2",
        {"IsTruncated": False, "KeyCount": 1, "Contents": [{"Key": "p/x"}]},
        expected_params={"Bucket": "b", "Prefix": "p/", "MaxKeys": 1},
    )
    assert is_dir("s3://b/p", s3_client=client) is True


def test_is_dir_s3_false_when_empty():
    client, stub = _stub_client()
    stub.add_response(
        "list_objects_v2",
        {"IsTruncated": False, "KeyCount": 0},
        expected_params={"Bucket": "b", "Prefix": "empty/", "MaxKeys": 1},
    )
    assert is_dir("s3://b/empty", s3_client=client) is False


def test_is_file_s3_delegates_to_head_object():
    """is_file(s3://...) is a HEAD round-trip, not a list."""
    client, stub = _stub_client()
    stub.add_response(
        "head_object",
        {"ContentLength": 42},
        expected_params={"Bucket": "b", "Key": "a/file.bin"},
    )
    stub.add_client_error(
        "head_object",
        service_error_code="404",
        http_status_code=404,
        expected_params={"Bucket": "b", "Key": "a/missing.bin"},
    )
    assert is_file("s3://b/a/file.bin", s3_client=client) is True
    assert is_file("s3://b/a/missing.bin", s3_client=client) is False


def test_makedirs_s3_is_noop():
    """makedirs on an s3 URI must NOT make an API call."""
    # Passing a None client verifies we never try to construct one —
    # the call should return immediately without touching the network.
    makedirs("s3://bucket/prefix/whatever")
    makedirs("s3://bucket")
    makedirs("s3://bucket/a/b/c/d")


# ---------------------------------------------------------------------------
# is_s3_uri helper — scheme detection
# ---------------------------------------------------------------------------


def test_is_s3_uri_recognises_s3_scheme():
    assert is_s3_uri("s3://bucket/key")
    assert is_s3_uri("s3://bucket")
    assert is_s3_uri("s3://")  # technically malformed but scheme-matches


def test_is_s3_uri_rejects_local_paths():
    assert not is_s3_uri("/tmp/backup")
    assert not is_s3_uri("./relative/path")
    assert not is_s3_uri("C:\\Windows\\tmp")
    assert not is_s3_uri("")


def test_is_s3_uri_rejects_other_schemes():
    """gs://, file://, http:// etc. must not match."""
    assert not is_s3_uri("gs://bucket/key")
    assert not is_s3_uri("file:///tmp/x")
    assert not is_s3_uri("http://example.com/x")
    # s3a and s3n are Hadoop conventions; we deliberately don't match
    # them today — if we add support later, the test documents the
    # regression surface.
    assert not is_s3_uri("s3a://bucket/key")
    assert not is_s3_uri("s3n://bucket/key")


# ---------------------------------------------------------------------------
# localize() — yield a local path for any Blob
#
# Restore-side consumers (``psql -f``, ``pg_restore``) require a real
# file.  ``localize()`` adapts both backends to that contract without
# forcing the caller to branch on scheme.
# ---------------------------------------------------------------------------


def test_localize_local_blob_yields_underlying_path(tmp_path):
    """LocalBlob is zero-copy: the yielded path IS the real file."""
    path = tmp_path / "pre_data.sql"
    path.write_bytes(b"CREATE TABLE x ();")
    blob = LocalBlob(str(path))

    with localize(blob) as p:
        # Same path — not a temp copy.
        assert p == str(path)
        # Original file still present, contents intact.
        assert open(p, "rb").read() == b"CREATE TABLE x ();"

    # Local file is NOT unlinked on exit — it's not ours to delete.
    assert path.exists()


def test_localize_local_blob_preserves_file_on_exception(tmp_path):
    """A raise inside the ``with`` must not delete the caller's file."""
    path = tmp_path / "data.bin"
    path.write_bytes(b"important")
    blob = LocalBlob(str(path))

    with pytest.raises(RuntimeError):
        with localize(blob):
            raise RuntimeError("boom")

    assert path.exists()
    assert path.read_bytes() == b"important"


def test_localize_s3_blob_downloads_to_temp_file():
    """S3Blob is materialized to a real, readable temp file."""
    client, stub = _stub_client()
    payload = b"\x00\x01\x02pg_restore custom format payload\xff"
    stub.add_response(
        "get_object",
        {"Body": io.BytesIO(payload), "ContentLength": len(payload)},
        expected_params={"Bucket": "b", "Key": "post_data.dump"},
    )
    blob = S3Blob("b", "post_data.dump", client=client)

    with localize(blob) as p:
        assert os.path.isfile(p)
        # Must be a different path from blob.uri (blob.uri is s3://).
        assert not p.startswith("s3://")
        with open(p, "rb") as f:
            assert f.read() == payload
        captured_path = p

    # Temp file is cleaned up on exit.
    assert not os.path.exists(captured_path)
    stub.assert_no_pending_responses()


def test_localize_s3_blob_cleans_up_on_exception():
    """Temp file is unlinked even when the ``with`` block raises."""
    client, stub = _stub_client()
    payload = b"partial"
    stub.add_response(
        "get_object",
        {"Body": io.BytesIO(payload), "ContentLength": len(payload)},
        expected_params={"Bucket": "b", "Key": "pre_data.sql"},
    )
    blob = S3Blob("b", "pre_data.sql", client=client)

    captured_path = None
    with pytest.raises(ValueError, match="planned"):
        with localize(blob) as p:
            captured_path = p
            assert os.path.isfile(p)
            raise ValueError("planned")

    assert captured_path is not None
    assert not os.path.exists(captured_path)


def test_localize_s3_blob_streams_large_payload():
    """Large S3 payloads are chunk-read, not slurped into RAM in one go.

    We can't observe RSS in a unit test, but we can verify correctness
    on a payload bigger than the stub's default Body size: 3 MiB here
    straddles the 1 MiB localize chunk boundary multiple times.
    """
    client, stub = _stub_client()
    payload = (b"abcdefgh" * 128) * (3 * 1024)  # ~3 MiB, 8-byte repeats
    stub.add_response(
        "get_object",
        {"Body": io.BytesIO(payload), "ContentLength": len(payload)},
        expected_params={"Bucket": "b", "Key": "big.dump"},
    )
    blob = S3Blob("b", "big.dump", client=client)

    with localize(blob) as p:
        with open(p, "rb") as f:
            assert f.read() == payload
    stub.assert_no_pending_responses()


def test_localize_s3_blob_missing_temp_unlink_is_safe():
    """If something else removed the temp file, cleanup must not raise.

    Simulates a race where an external process cleans ``/tmp`` between
    the ``yield`` and the ``finally`` — our unlink catches
    FileNotFoundError rather than propagating it to the caller.
    """
    client, stub = _stub_client()
    stub.add_response(
        "get_object",
        {"Body": io.BytesIO(b"x"), "ContentLength": 1},
        expected_params={"Bucket": "b", "Key": "k"},
    )
    blob = S3Blob("b", "k", client=client)

    with localize(blob) as p:
        # Preemptively unlink the temp file — the context manager's
        # finally-block must tolerate this.
        os.unlink(p)
        assert not os.path.exists(p)
    # No exception raised on context exit.


# ---------------------------------------------------------------------------
# Background part uploads (upload_threads > 0)
# ---------------------------------------------------------------------------


class _FakeS3:
    """Thread-safe in-memory S3 client that records what happened, and when."""

    def __init__(self, *, delay=0.0, fail_part=None, gate=None):
        import threading

        self.lock = threading.Lock()
        self.delay = delay
        self.fail_part = fail_part
        self.gate = gate  # an Event every upload_part waits for, if given
        self.parts: dict[int, bytes] = {}
        self.events: list[str] = []
        self.in_flight = 0
        self.max_in_flight = 0
        self.in_flight_at_abort: list[int] = []
        self.upload_threads: set[str] = set()
        self.completed = None
        self.objects: dict[str, bytes] = {}

    def create_multipart_upload(self, **kw):
        return {"UploadId": "UP"}

    def upload_part(self, *, Body, PartNumber, **kw):
        import threading
        import time

        with self.lock:
            self.in_flight += 1
            self.max_in_flight = max(self.max_in_flight, self.in_flight)
            self.upload_threads.add(threading.current_thread().name)
        try:
            if self.gate is not None:
                assert self.gate.wait(10), "gate never opened"
            if self.delay:
                # later parts finish first, so ordering bugs would show up
                time.sleep(self.delay / PartNumber)
            if PartNumber == self.fail_part:
                raise RuntimeError(f"boom on part {PartNumber}")
            with self.lock:
                self.parts[PartNumber] = bytes(Body)
                self.events.append(f"part{PartNumber}")
            return {"ETag": f"e{PartNumber}"}
        finally:
            with self.lock:
                self.in_flight -= 1

    def complete_multipart_upload(self, *, MultipartUpload, **kw):
        with self.lock:
            self.events.append("complete")
            self.completed = MultipartUpload["Parts"]
        return {}

    def abort_multipart_upload(self, **kw):
        with self.lock:
            self.events.append("abort")
            self.in_flight_at_abort.append(self.in_flight)
        return {}

    def put_object(self, *, Key, Body, **kw):
        self.objects[Key] = bytes(Body)
        return {}


def _write_all(client, data: bytes, *, threads, chunk=7):
    blob = S3Blob("b", "k", client=client, part_size=16, upload_threads=threads)
    with blob.open_write() as sink:
        for i in range(0, len(data), chunk):
            sink.write(data[i : i + chunk])


@pytest.mark.parametrize("threads", [0, 1, 2, 4])
def test_s3_background_upload_keeps_part_order(tiny_part_size, threads):
    """Whatever finishes first, the parts are reassembled in order."""
    client = _FakeS3(delay=0.02)
    data = bytes(range(256)) * 2  # 512 bytes -> 32 parts of 16 bytes
    _write_all(client, data, threads=threads)
    numbers = [p["PartNumber"] for p in client.completed]
    assert numbers == list(range(1, 33))
    assert b"".join(client.parts[n] for n in numbers) == data
    assert [p["ETag"] for p in client.completed] == [f"e{n}" for n in numbers]


def test_s3_zero_threads_uploads_in_the_calling_thread(tiny_part_size):
    import threading

    client = _FakeS3()
    _write_all(client, b"x" * 40, threads=0)
    assert client.upload_threads == {threading.current_thread().name}


def test_s3_threads_upload_off_the_calling_thread(tiny_part_size):
    import threading

    client = _FakeS3()
    _write_all(client, b"x" * 40, threads=2)
    assert threading.current_thread().name not in client.upload_threads
    assert all(n.startswith("pgfanout-s3-upload") for n in client.upload_threads)


def test_s3_writer_keeps_reading_while_a_part_is_uploading(tiny_part_size):
    """write() must return while an upload is still in flight (the whole point)."""
    import threading

    gate = threading.Event()
    client = _FakeS3(gate=gate)
    blob = S3Blob("b", "k", client=client, part_size=16, upload_threads=1)
    writer = blob.open_write()
    sink = writer.__enter__()
    done = threading.Event()

    def feed():
        sink.write(b"a" * 16)  # a full part -> upload starts and blocks on the gate
        sink.write(b"b" * 10)  # must not wait for that upload
        done.set()

    t = threading.Thread(target=feed)
    t.start()
    try:
        assert done.wait(5), "write() blocked on the in-flight upload"
        assert client.parts == {}  # the first part is still uploading
    finally:
        gate.set()
        t.join(5)
        writer.__exit__(None, None, None)
    assert b"".join(client.parts[n] for n in sorted(client.parts)) == b"a" * 16 + b"b" * 10


@pytest.mark.parametrize("threads", [1, 2, 3])
def test_s3_in_flight_parts_are_bounded(tiny_part_size, threads):
    client = _FakeS3(delay=0.01)
    _write_all(client, b"z" * 400, threads=threads)
    assert 1 <= client.max_in_flight <= threads


def test_s3_failed_background_upload_aborts_after_in_flight_parts_finish(tiny_part_size):
    client = _FakeS3(delay=0.01, fail_part=2)
    with pytest.raises(RuntimeError, match="boom on part 2"):
        _write_all(client, b"q" * 200, threads=2)
    assert client.completed is None
    assert "complete" not in client.events
    # abort runs once, and only after no upload is still running
    assert client.events.count("abort") == 1
    assert client.in_flight == 0
    assert client.events[-1] == "abort"


def test_s3_exception_in_with_block_stops_uploads_then_aborts(tiny_part_size):
    client = _FakeS3(delay=0.01)
    blob = S3Blob("b", "k", client=client, part_size=16, upload_threads=2)
    with pytest.raises(ValueError, match="stop"):
        with blob.open_write() as sink:
            sink.write(b"x" * 100)
            raise ValueError("stop")
    assert client.events.count("abort") == 1
    assert client.events[-1] == "abort"
    assert client.completed is None


def test_s3_empty_object_with_threads_still_uses_put_object(tiny_part_size):
    client = _FakeS3()
    _write_all(client, b"", threads=2)
    assert client.objects == {"k": b""}
    assert client.completed is None


def test_s3_upload_threads_env(monkeypatch):
    from pgfanout import storage

    monkeypatch.delenv(storage.S3_UPLOAD_THREADS_ENV, raising=False)
    assert S3Blob("b", "k", client=object())._upload_threads == storage.S3_DEFAULT_UPLOAD_THREADS
    monkeypatch.setenv(storage.S3_UPLOAD_THREADS_ENV, "0")
    assert S3Blob("b", "k", client=object())._upload_threads == 0
    monkeypatch.setenv(storage.S3_UPLOAD_THREADS_ENV, "4")
    assert S3Blob("b", "k", client=object())._upload_threads == 4
    # an explicit argument wins over the environment
    assert S3Blob("b", "k", client=object(), upload_threads=2)._upload_threads == 2
    for bad in ("x", "-1", str(storage.S3_MAX_UPLOAD_THREADS + 1)):
        monkeypatch.setenv(storage.S3_UPLOAD_THREADS_ENV, bad)
        with pytest.raises(ValueError, match=storage.S3_UPLOAD_THREADS_ENV):
            S3Blob("b", "k", client=object())


def test_s3_memory_is_bounded_when_all_upload_slots_are_busy(monkeypatch):
    """With every slot busy the writer must block *before* copying another part.

    The documented bound is part_size * (threads + 1); an extra slice and copy
    taken before the wait used to push the peak to about five parts.
    """
    import threading
    import tracemalloc

    from pgfanout import storage

    monkeypatch.setattr(storage, "S3_MIN_PART_SIZE", 1)
    part, threads, chunk = 1 << 20, 2, 64 << 10
    gate = threading.Event()  # closed: uploads never finish, so the slots stay full
    client = _FakeS3(gate=gate)
    blob = S3Blob("b", "k", client=client, part_size=part, upload_threads=threads)
    writer = blob.open_write()
    sink = writer.__enter__()
    finished = threading.Event()

    def feed():
        for _ in range(8 * (part // chunk)):  # 8 parts' worth, so it must block
            sink.write(b"\0" * chunk)
        finished.set()

    tracemalloc.start()
    t = threading.Thread(target=feed)
    t.start()
    try:
        assert not finished.wait(1.5), "the writer should be blocked on full upload slots"
        peak = tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()
        gate.set()
        t.join(10)
        writer.__exit__(None, None, None)
    assert peak <= (threads + 1.5) * part, f"peak {peak / part:.1f} parts, expected <= 3.5"


def test_s3_one_huge_write_does_not_buffer_the_whole_write(monkeypatch):
    """A single write much larger than a part is cut into parts as it goes."""
    import threading
    import tracemalloc

    from pgfanout import storage

    monkeypatch.setattr(storage, "S3_MIN_PART_SIZE", 1)
    part = 1 << 20
    gate = threading.Event()
    client = _FakeS3(gate=gate)
    blob = S3Blob("b", "k", client=client, part_size=part, upload_threads=1)
    writer = blob.open_write()
    sink = writer.__enter__()
    big = bytes(16 * part)
    tracemalloc.start()
    t = threading.Thread(target=lambda: sink.write(big))
    t.start()
    try:
        t.join(1.5)
        peak = tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()
        gate.set()
        t.join(10)
        writer.__exit__(None, None, None)
    # one part filling + one in flight, never the 16 parts of the write
    assert peak <= 3.5 * part, f"peak {peak / part:.1f} parts"


_MAIN_THREAD_ID = threading.get_ident()


def _sigint_main_thread():
    """Deliver a real SIGINT to the main thread, so a blocked wait there is interrupted.

    ``_thread.interrupt_main`` only sets a flag and does not wake a thread that is
    blocked waiting, so it would fire after the wait rather than during it.
    """
    signal.pthread_kill(_MAIN_THREAD_ID, signal.SIGINT)


def test_s3_interrupt_while_stopping_uploads_still_aborts_and_keeps_the_error(tiny_part_size):
    """A Ctrl-C during the wait must not skip the abort or hide the original error.

    The interrupt is delivered to the main thread while ``shutdown(wait=True)``
    is genuinely blocked on uploads that are held by a gate, so the wait itself
    is what gets interrupted, and the abort must still come after every part
    has finished.
    """
    import threading

    gate = threading.Event()
    client = _FakeS3(gate=gate)
    blob = S3Blob("b", "k", client=client, part_size=16, upload_threads=2)
    timers = [
        threading.Timer(0.3, _sigint_main_thread),  # lands inside the wait
        threading.Timer(0.8, gate.set),  # uploads finish after the interrupt
    ]
    try:
        with pytest.raises(ValueError, match="original failure"):
            with blob.open_write() as sink:
                sink.write(b"x" * 40)  # parts are in flight, held by the gate
                deadline = time.monotonic() + 5
                while client.in_flight < 2 and time.monotonic() < deadline:
                    time.sleep(0.01)
                assert client.in_flight == 2
                for t in timers:
                    t.start()
                raise ValueError("original failure")
    finally:
        for t in timers:
            t.cancel()
        gate.set()
    assert client.in_flight_at_abort == [0]  # nothing was on the wire at abort
    assert client.events.count("abort") == 1
    assert client.events[-1] == "abort"
    assert client.completed is None


def test_s3_interrupt_while_stopping_uploads_is_reraised_without_an_original_error(tiny_part_size):
    """With nothing else failing, the interrupt itself reaches the caller, after the abort."""
    import threading

    gate = threading.Event()
    client = _FakeS3(gate=gate)
    blob = S3Blob("b", "k", client=client, part_size=16, upload_threads=2)
    timers = [
        threading.Timer(0.3, _sigint_main_thread),
        threading.Timer(0.8, gate.set),
    ]
    sink = blob.open_write().__enter__()
    try:
        sink.write(b"x" * 40)
        deadline = time.monotonic() + 5
        while client.in_flight < 2 and time.monotonic() < deadline:
            time.sleep(0.01)
        for t in timers:
            t.start()
        with pytest.raises(KeyboardInterrupt):
            sink._stop_uploads()
    finally:
        for t in timers:
            t.cancel()
        gate.set()
    assert client.in_flight == 0  # the wait ran to the end before re-raising
