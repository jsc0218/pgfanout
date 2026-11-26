"""Minimal blob-storage abstraction for dump/restore part files.

Isolates the worker code paths from the concrete storage backend so
``copy_range`` (dump) and ``copy_part`` (restore) work identically
whether parts live on a local filesystem or in an object store.

Two backends are provided:

* :class:`LocalBlob` — plain filesystem paths.  Default when *uri* has
  no scheme prefix; used by local dumps and the Docker test suite.
* :class:`S3Blob` — streaming multipart upload / ``GetObject`` download
  against any S3-compatible endpoint (AWS, Ceph RGW, MinIO, ...).
  Does not touch local disk — useful for stateless compute nodes and
  for tables too large to stage.

The client for :class:`S3Blob` is configured via environment variables
(see :func:`make_s3_client`) so the same credentials flow works for
boto3, the AWS CLI, and our tool.
"""

from __future__ import annotations

import base64
import os
import struct
import tempfile
import zlib
from abc import ABC, abstractmethod
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from concurrent.futures import wait as wait_futures
from contextlib import contextmanager
from typing import TYPE_CHECKING, BinaryIO
from urllib.parse import urlparse

if TYPE_CHECKING:
    from collections.abc import Iterator


class Blob(ABC):
    """Handle for a single blob (one part file, one DDL file, ...).

    Implementations MUST be safe to construct from inside a worker thread
    without any shared state, and MUST expose ``open_write``/``open_read``
    returning a binary file-like object that supports ``write()``/``read()``
    (and works as a context manager so the caller can use ``with``).
    """

    @abstractmethod
    def open_write(self) -> BinaryIO: ...

    @abstractmethod
    def open_read(self) -> BinaryIO: ...

    @abstractmethod
    def size(self) -> int:
        """Return the size in bytes, or 0 if the blob does not exist."""

    @abstractmethod
    def exists(self) -> bool: ...

    @abstractmethod
    def delete(self) -> None:
        """Remove the blob.  No-op if it does not exist."""

    @property
    @abstractmethod
    def uri(self) -> str:
        """Human-readable location for logs (path or ``s3://bucket/key``)."""


class LocalBlob(Blob):
    """Backend for plain filesystem paths.  Creates parent dirs on write."""

    def __init__(self, path: str):
        self._path = path

    def open_write(self) -> BinaryIO:
        parent = os.path.dirname(self._path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        return open(self._path, "wb")

    def open_read(self) -> BinaryIO:
        return open(self._path, "rb")

    def size(self) -> int:
        try:
            return os.path.getsize(self._path)
        except FileNotFoundError:
            return 0

    def exists(self) -> bool:
        return os.path.isfile(self._path)

    def delete(self) -> None:
        try:
            os.unlink(self._path)
        except FileNotFoundError:
            pass

    @property
    def uri(self) -> str:
        return self._path

    def __repr__(self) -> str:
        return f"LocalBlob({self._path!r})"


# ---------------------------------------------------------------------------
# S3 backend
# ---------------------------------------------------------------------------

# S3 mandates multipart parts >= 5 MiB (except the last).  We default to a
# larger flush threshold so the network packets stay fat and the API-call
# count stays low on large backups.
S3_MIN_PART_SIZE = 5 * 1024 * 1024
S3_DEFAULT_PART_SIZE = 16 * 1024 * 1024

# Parts are uploaded on background threads so the writer can keep reading the
# next part from ``psql`` while the previous one is on the wire. This is the
# number of such threads per part file; 0 uploads in the writing thread (each
# part blocks the reader until S3 has it). It is also the number of parts that
# can be in flight, so RAM per part file is part_size * (threads + 1).
S3_DEFAULT_UPLOAD_THREADS = 2
S3_MAX_UPLOAD_THREADS = 8  # stays inside botocore's default pool of 10 connections
S3_UPLOAD_THREADS_ENV = "PGFANOUT_S3_UPLOAD_THREADS"

# Integrity check on every uploaded part. "crc32" (the default): pgfanout
# computes a CRC32 of the part with zlib (C code) and sends it as a plain
# header, and S3 rejects the part if the bytes it received do not match.
# "boto3": leave it to boto3, whose default sends the part through a
# Python-level aws-chunked wrapper with a trailing checksum, which made dumps
# with 8 or more workers take 35-43% longer in our measurements. Use "boto3"
# for a store that does not accept a CRC32 on a multipart upload.
S3_PART_CHECKSUM_ENV = "PGFANOUT_S3_PART_CHECKSUM"
S3_PART_CHECKSUMS = ("crc32", "boto3")


def is_s3_uri(uri: str) -> bool:
    """True if *uri* targets an S3 (or S3-compatible) endpoint.

    A single helper so the scattered ``startswith("s3://")`` checks in
    the codebase all read the same way and, if we ever need to widen
    the accepted scheme list (``s3a://``, ``s3n://``), we change it in
    one place.
    """
    return uri.startswith("s3://")


def parse_s3_uri(uri: str) -> tuple[str, str]:
    """Split ``s3://bucket/key/with/slashes`` into ``(bucket, key)``.

    Raises :class:`ValueError` if the URI is malformed or has no key.
    Use :func:`_parse_s3_prefix` for directory-style URIs (bucket-root
    and empty-key are legal).
    """
    parsed = urlparse(uri)
    if parsed.scheme != "s3":
        raise ValueError(f"not an s3:// URI: {uri!r}")
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    if not bucket or not key:
        raise ValueError(f"s3 URI must be s3://bucket/key, got: {uri!r}")
    return bucket, key


def _parse_s3_prefix(uri: str) -> tuple[str, str]:
    """Split ``s3://bucket/some/prefix`` into ``(bucket, prefix_with_slash)``.

    The empty prefix (``s3://bucket`` or ``s3://bucket/``) is valid and
    refers to the bucket root.  The returned prefix always ends with
    ``/`` if non-empty, so ``ListObjectsV2`` won't spuriously match a
    sibling key whose name starts with the prefix but extends beyond
    (e.g. ``s3://b/foo`` must not match ``foobar/baz``).
    """
    parsed = urlparse(uri)
    if parsed.scheme != "s3":
        raise ValueError(f"not an s3:// URI: {uri!r}")
    bucket = parsed.netloc
    if not bucket:
        raise ValueError(f"s3 URI missing bucket: {uri!r}")
    prefix = parsed.path.lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    return bucket, prefix


def _upload_threads_from_env() -> int:
    raw = (os.environ.get(S3_UPLOAD_THREADS_ENV) or "").strip()
    if not raw:
        return S3_DEFAULT_UPLOAD_THREADS
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(f"{S3_UPLOAD_THREADS_ENV} must be an integer, got {raw!r}") from None
    if not 0 <= value <= S3_MAX_UPLOAD_THREADS:
        raise ValueError(f"{S3_UPLOAD_THREADS_ENV} must be 0-{S3_MAX_UPLOAD_THREADS}, got {value}")
    return value


def _part_checksum_from_env() -> bool:
    """True when pgfanout should send its own CRC32 with every part."""
    raw = (os.environ.get(S3_PART_CHECKSUM_ENV) or "").strip().lower()
    if not raw:
        return True
    if raw not in S3_PART_CHECKSUMS:
        raise ValueError(
            f"{S3_PART_CHECKSUM_ENV} must be one of {', '.join(S3_PART_CHECKSUMS)}, got {raw!r}"
        )
    return raw == "crc32"


def _crc32_b64(data) -> str:
    """CRC32 of *data* in the form S3 expects (big-endian, base64)."""
    return base64.b64encode(struct.pack(">I", zlib.crc32(data))).decode("ascii")


def _env_bool(name: str, default: bool | None = None) -> bool | None:
    """Parse a boolean-ish env var.  Empty/unset returns *default*."""
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def make_s3_client(
    *,
    endpoint_url: str | None = None,
    region: str | None = None,
    verify: object | None = None,
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
):
    """Construct a boto3 S3 client with Ceph-friendly defaults.

    Configuration resolution order (first wins):

    1. Explicit keyword arguments to this function.
    2. Environment variables:

       * ``AWS_ENDPOINT_URL_S3`` (boto3-native) or
         ``AWS_S3_ENDPOINT_URL`` (legacy alias used by our tests).
         Needed for Ceph RGW / MinIO; AWS users leave it unset.
       * ``AWS_REGION`` — some Ceph clusters require a non-empty
         region; defaults to ``us-east-1`` which almost everyone
         accepts as a placeholder.
       * ``AWS_S3_VERIFY_SSL`` — ``false`` disables TLS verification
         (useful for Ceph with self-signed certs).  A path to a CA
         bundle is also accepted.  Default: verify.
       * ``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY`` — boto3's
         standard credential chain; we only read them explicitly if
         the caller passes them.

    3. boto3's default credential / config chain (``~/.aws/*``,
       instance metadata, etc.).

    Import of ``boto3`` is deferred so a local-only invocation of the
    tool doesn't pay for the dependency on startup.
    """
    import boto3  # deferred: ~150ms import cost

    endpoint_url = (
        endpoint_url
        or os.environ.get("AWS_ENDPOINT_URL_S3")
        or os.environ.get("AWS_S3_ENDPOINT_URL")
    )
    region = region or os.environ.get("AWS_REGION") or "us-east-1"

    if verify is None:
        raw = os.environ.get("AWS_S3_VERIFY_SSL")
        if raw is None or raw == "":
            verify = None  # leave boto3 default (verify)
        elif raw.strip().lower() in ("0", "false", "no", "off"):
            verify = False
        elif raw.strip().lower() in ("1", "true", "yes", "on"):
            verify = True
        else:
            # Treat anything else as a CA bundle path.
            verify = raw

    kwargs: dict = {"service_name": "s3", "region_name": region}
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
    if verify is not None:
        kwargs["verify"] = verify
    if aws_access_key_id is not None:
        kwargs["aws_access_key_id"] = aws_access_key_id
    if aws_secret_access_key is not None:
        kwargs["aws_secret_access_key"] = aws_secret_access_key

    return boto3.client(**kwargs)


class _S3MultipartWriter:
    """Streaming sink that turns ``.write(bytes)`` calls into S3 parts.

    Behaviour matches the local file handle returned by :meth:`LocalBlob.open_write`:

    * Zero-byte "close with no write" produces an empty object, written
      with a plain ``PutObject`` because S3 rejects a multipart upload
      with zero parts.
    * Exception inside the ``with`` block aborts the multipart upload,
      best effort: if the abort call itself fails the parts stay on the
      server (and accrue charges) until a bucket lifecycle rule removes them.
    * Chunks smaller than the part size accumulate; the part is flushed
      only once the buffer has enough data.  The final buffer (whatever
      remains at close) is uploaded as the last part regardless of size.
    * With ``upload_threads > 0`` full parts are uploaded on background
      threads, so the caller keeps reading the next part while the previous
      one is on the wire. At most ``upload_threads`` parts are in flight; a
      further flush waits for the oldest, which bounds RAM at
      ``part_size * (upload_threads + 1)``. A failed upload surfaces on a
      later ``write`` or at close, and the multipart upload is then aborted
      once the uploads still in flight have finished (aborting earlier could
      let a late part land after the abort). ``upload_threads=0`` uploads in
      the calling thread.
    * With ``checksum`` every part carries a CRC32 computed here (see
      ``PGFANOUT_S3_PART_CHECKSUM``), and the upload is created with the same
      algorithm so S3 accepts and verifies it.
    """

    def __init__(
        self,
        client,
        bucket: str,
        key: str,
        part_size: int,
        upload_threads: int = 0,
        checksum: bool = False,
    ):
        if part_size < S3_MIN_PART_SIZE:
            raise ValueError(f"part_size must be >= {S3_MIN_PART_SIZE} (S3 minimum)")
        if upload_threads < 0:
            raise ValueError("upload_threads must not be negative")
        self._upload_threads = upload_threads
        self._checksum = checksum  # send our own CRC32 with each part
        self._pool: ThreadPoolExecutor | None = None
        self._pending: deque[Future] = deque()  # in-flight parts, oldest first
        self._client = client
        self._bucket = bucket
        self._key = key
        self._part_size = part_size
        self._buf = bytearray()
        self._parts: list[dict] = []
        self._upload_id: str | None = None
        self._next_part_num = 1
        self._closed = False

    def __enter__(self) -> _S3MultipartWriter:
        extra = {"ChecksumAlgorithm": "CRC32"} if self._checksum else {}
        resp = self._client.create_multipart_upload(Bucket=self._bucket, Key=self._key, **extra)
        self._upload_id = resp["UploadId"]
        if self._upload_threads:
            self._pool = ThreadPoolExecutor(
                max_workers=self._upload_threads, thread_name_prefix="pgfanout-s3-upload"
            )
        return self

    def write(self, data: bytes) -> int:
        if self._closed:
            raise ValueError("write() after close")
        if not data:
            return 0
        # Feed the buffer one part at a time instead of appending all of *data*
        # first, so a large write cannot grow it past part_size. Part boundaries
        # are the same as before; a short tail stays in the buffer until the
        # next write or close.
        with memoryview(data) as view:
            pos = 0
            while pos < len(view):
                take = min(self._part_size - len(self._buf), len(view) - pos)
                self._buf.extend(view[pos : pos + take])
                pos += take
                if len(self._buf) >= self._part_size:
                    self._flush_part(self._part_size)
        return len(data)

    def _upload_part(self, part_number: int, body: bytes) -> dict:
        extra = {"ChecksumCRC32": _crc32_b64(body)} if self._checksum else {}
        resp = self._client.upload_part(
            Bucket=self._bucket,
            Key=self._key,
            PartNumber=part_number,
            UploadId=self._upload_id,
            Body=body,
            **extra,
        )
        # CompleteMultipartUpload must repeat each part's checksum.
        return {"PartNumber": part_number, "ETag": resp["ETag"], **extra}

    def _reap_oldest(self) -> None:
        """Wait for the oldest in-flight part; re-raises its upload error."""
        self._parts.append(self._pending.popleft().result())

    def _flush_part(self, size: int) -> None:
        if self._pool is not None:
            # Make room *before* copying the part: collect finished uploads so a
            # failure shows up early, then wait for the oldest if every slot is
            # busy. Copying first would hold one more part than the documented
            # bound (part_size * (threads + 1)) while blocked.
            while self._pending and self._pending[0].done():
                self._reap_oldest()
            while len(self._pending) >= self._upload_threads:
                self._reap_oldest()
        # One copy of the part (the views are released before the buffer shrinks).
        with memoryview(self._buf) as view, view[:size] as part:
            body = bytes(part)
        del self._buf[:size]
        part_number = self._next_part_num
        self._next_part_num += 1
        if self._pool is None:
            self._parts.append(self._upload_part(part_number, body))
        else:
            self._pending.append(self._pool.submit(self._upload_part, part_number, body))

    def _drain(self) -> None:
        """Wait for every in-flight part (in order), raising the first failure."""
        while self._pending:
            self._reap_oldest()

    def _stop_uploads(self) -> None:
        """Let uploads already on the wire finish, and run no further ones.

        An interrupt (Ctrl-C) while waiting does not cut the wait short: the
        wait is finished and the interrupt is re-raised afterwards. That keeps
        the caller's abort from racing a part that is still being sent.
        """
        pool = self._pool
        pending = list(self._pending)
        interrupt: BaseException | None = None
        if pool is not None:
            # Every step is inside the loop, so an interrupt at any point is kept
            # and the step is simply run again; only then is the state cleared.
            #
            # Wait on the futures, not on Thread.join: on Python 3.10 a join that
            # is interrupted by Ctrl-C marks the thread finished while it is still
            # running, so a retried shutdown() would return early.
            while True:
                try:
                    for future in pending:
                        future.cancel()  # parts that have not started; a running one refuses
                    pool.shutdown(wait=False, cancel_futures=True)
                    # A cancelled future never runs, and on 3.10 its waiters are never
                    # woken, so only the uploads that are actually running are waited on.
                    wait_futures([f for f in pending if not f.cancelled()])
                    pool.shutdown(wait=True)  # nothing left to wait for, only the threads exit
                    break
                except BaseException as exc:  # noqa: BLE001
                    interrupt = interrupt or exc
        self._pool = None
        self._pending.clear()
        if interrupt is not None:
            raise interrupt

    def _abort_once_uploads_finish(self) -> None:
        """Failure cleanup: wait for in-flight parts, then abort the upload.

        Never raises (apart from the abort call itself), so the failure being
        handled is the one the caller sees, and the abort always runs, and runs
        only after no part is still on the wire.
        """
        try:
            self._stop_uploads()
        except BaseException:  # noqa: BLE001, S110 - superseded by the failure in progress
            pass
        self._best_effort_abort()

    def _best_effort_abort(self) -> None:
        """Tear down the multipart upload so dangling parts don't accrue charges.

        Best effort: a failure of the abort call is swallowed, so the original
        error is not masked, and the parts then remain until a bucket
        lifecycle rule removes them.

        Idempotent: clears ``_upload_id`` so repeat calls from overlapping
        error paths (e.g. an ``UploadPartCopy`` failure followed by an
        ``__exit__`` trying to clean up) don't double-abort.
        """
        if self._upload_id is None:
            return
        try:
            self._client.abort_multipart_upload(
                Bucket=self._bucket,
                Key=self._key,
                UploadId=self._upload_id,
            )
        except Exception:  # noqa: BLE001 — best-effort cleanup
            pass
        self._upload_id = None

    def __exit__(self, exc_type, exc, tb) -> bool:
        self._closed = True
        if exc_type is not None:
            # Propagate the original exception, but first try to free
            # the dangling parts on the server so the caller isn't
            # charged storage for an aborted upload. Uploads already on the
            # wire finish first: a part that lands after the abort would
            # otherwise be left behind.
            self._abort_once_uploads_finish()
            return False  # let the exception propagate

        # Happy-path finalisation.  Any failure here (trailing flush,
        # CompleteMultipartUpload, or the empty-blob PutObject fallback)
        # must also abort the multipart upload — otherwise dangling
        # parts persist on the server until a bucket lifecycle rule
        # reaps them.  The abort happens first so it runs even if the
        # caller's exception handler re-raises something else.
        #
        # ``except BaseException`` (not ``Exception``) is deliberate:
        # ``_flush_part`` and ``complete_multipart_upload`` are network
        # round-trips that can take many seconds on a real S3 endpoint,
        # which is exactly the window during which an operator's Ctrl+C
        # — or any other ``BaseException`` like ``SystemExit`` /
        # ``GeneratorExit`` — is most likely to land.  ``except
        # Exception`` would skip the abort in that case and leave the
        # upload dangling on the server (charged storage until the
        # bucket lifecycle policy reaps it), defeating the very
        # invariant the comment above promises.  We re-raise so the
        # signal still propagates.
        try:
            # Flush the trailing buffer (may be smaller than part_size —
            # S3 allows the last part to be arbitrary size).
            if self._buf:
                self._flush_part(len(self._buf))
            self._drain()  # every in-flight part, in order; raises a failed upload

            if not self._parts:
                # S3 rejects CompleteMultipartUpload with zero parts, so
                # abort the empty multipart upload and fall back to a
                # plain PutObject.  _best_effort_abort() clears
                # _upload_id so a subsequent PutObject failure does not
                # re-abort an already-aborted upload.
                self._best_effort_abort()
                self._client.put_object(Bucket=self._bucket, Key=self._key, Body=b"")
                return False

            self._client.complete_multipart_upload(
                Bucket=self._bucket,
                Key=self._key,
                UploadId=self._upload_id,
                MultipartUpload={"Parts": self._parts},
            )
        except BaseException:
            self._abort_once_uploads_finish()
            raise
        finally:
            self._stop_uploads()  # releases the upload threads; a no-op if done
        return False


class _S3StreamReader:
    """Thin wrapper so ``GetObject``'s StreamingBody satisfies our Blob contract."""

    def __init__(self, client, bucket: str, key: str):
        self._client = client
        self._bucket = bucket
        self._key = key
        self._body = None

    def __enter__(self):
        resp = self._client.get_object(Bucket=self._bucket, Key=self._key)
        self._body = resp["Body"]
        return self

    def read(self, n: int = -1) -> bytes:
        # StreamingBody.read() uses None for "all", not -1 like stdlib.
        return self._body.read(None if n is None or n < 0 else n)

    def __exit__(self, exc_type, exc, tb) -> bool:
        try:
            if self._body is not None:
                self._body.close()
        except Exception:  # noqa: BLE001
            pass
        return False


class S3Blob(Blob):
    """Backend for ``s3://bucket/key`` URIs.

    Writes use a streaming multipart upload so arbitrarily large blobs
    cost ``part_size * (upload_threads + 1)`` bytes of RAM at a time —
    never the whole object.
    Reads wrap ``GetObject``'s streaming body.

    The client is constructed once via :func:`make_s3_client` (or passed
    in, which tests use).  Construction does one network round-trip at
    most (for ``size()`` / ``exists()`` / ``delete()``); the rest happen
    only when the open_* context manager is entered.
    """

    def __init__(
        self,
        bucket: str,
        key: str,
        *,
        client=None,
        part_size: int = S3_DEFAULT_PART_SIZE,
        upload_threads: int | None = None,
        part_checksum: bool | None = None,
    ):
        self._bucket = bucket
        self._key = key
        self._part_size = part_size
        # None means: PGFANOUT_S3_UPLOAD_THREADS, else the default.
        self._upload_threads = (
            _upload_threads_from_env() if upload_threads is None else upload_threads
        )
        # None means: PGFANOUT_S3_PART_CHECKSUM, else our own CRC32.
        self._part_checksum = _part_checksum_from_env() if part_checksum is None else part_checksum
        self._client = client  # lazily created if None

    @classmethod
    def from_uri(cls, uri: str, *, client=None, **kwargs) -> S3Blob:
        bucket, key = parse_s3_uri(uri)
        return cls(bucket, key, client=client, **kwargs)

    def _get_client(self):
        if self._client is None:
            self._client = make_s3_client()
        return self._client

    def open_write(self) -> BinaryIO:
        return _S3MultipartWriter(  # type: ignore[return-value]
            self._get_client(),
            self._bucket,
            self._key,
            self._part_size,
            self._upload_threads,
            self._part_checksum,
        )

    def open_read(self) -> BinaryIO:
        """Return a chunked streaming reader (a boto3 ``StreamingBody``).

        ``read(n)`` pulls only *n* bytes, so a restore never materialises
        the whole object in memory.
        """
        return _S3StreamReader(self._get_client(), self._bucket, self._key)  # type: ignore[return-value]

    def size(self) -> int:
        try:
            resp = self._get_client().head_object(Bucket=self._bucket, Key=self._key)
        except Exception as exc:  # noqa: BLE001
            if _is_not_found(exc):
                return 0
            raise
        return int(resp.get("ContentLength", 0))

    def exists(self) -> bool:
        try:
            self._get_client().head_object(Bucket=self._bucket, Key=self._key)
        except Exception as exc:  # noqa: BLE001
            if _is_not_found(exc):
                return False
            raise
        return True

    def delete(self) -> None:
        try:
            self._get_client().delete_object(Bucket=self._bucket, Key=self._key)
        except Exception as exc:  # noqa: BLE001
            # S3's DeleteObject is idempotent by spec — both AWS and
            # Ceph return success for absent keys — but swallow
            # NoSuchKey anyway in case a backend surfaces it.
            if _is_not_found(exc):
                return
            raise

    @property
    def uri(self) -> str:
        return f"s3://{self._bucket}/{self._key}"

    def __repr__(self) -> str:
        return f"S3Blob({self.uri!r})"


def _is_not_found(exc: BaseException) -> bool:
    """True for ``404 Not Found`` / ``NoSuchKey`` from botocore/boto3.

    Isolated in one place so the swallow-vs-raise decision is easy to
    audit.  We intentionally match on multiple surfaces because
    different S3 backends raise slightly different shapes:

    * boto3 head_object on AWS → ClientError with code ``404`` and
      HTTP status 404.
    * boto3 get_object / delete_object on AWS → ClientError with code
      ``NoSuchKey``.
    * Ceph RGW tends to match AWS verbatim, but older versions have
      been known to ship ``NoSuchBucket`` for missing buckets even
      when the key is the real problem.
    """
    # Avoid the import-time cost of botocore.exceptions on local-only
    # invocations by doing a lazy isinstance-ish match.
    code = None
    status = None
    resp = getattr(exc, "response", None)
    if isinstance(resp, dict):
        err = resp.get("Error", {})
        code = err.get("Code")
        status = resp.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return code in ("404", "NoSuchKey", "NotFound") or status == 404


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def make_blob(uri: str, *, s3_client=None) -> Blob:
    """Return the appropriate :class:`Blob` for *uri*.

    Dispatches on the URI scheme:

    * ``s3://bucket/key`` → :class:`S3Blob`.  The optional *s3_client*
      override is mostly a test seam; production code leaves it None
      and lets :func:`make_s3_client` read env vars.
    * everything else → :class:`LocalBlob` (plain filesystem path).
    """
    if is_s3_uri(uri):
        return S3Blob.from_uri(uri, client=s3_client)
    return LocalBlob(uri)


def join(base: str, *parts: str) -> str:
    """Scheme-aware path join.  Use this wherever a caller might supply
    either a filesystem path or an ``s3://`` URI as *base*.

    For S3 URIs we always use ``/``, strip redundant slashes on both
    sides of each segment, and avoid producing the ``s3://b//x/y``
    double-slash that some clients tolerate and others reject.

    For local paths we defer to :func:`os.path.join`, which picks the
    platform separator and handles absolute-override semantics
    correctly (e.g. ``os.path.join("/a", "/b") == "/b"``).
    """
    if is_s3_uri(base):
        cleaned = [p.strip("/") for p in parts if p]
        stripped_base = base.rstrip("/")
        if not cleaned:
            return stripped_base
        return stripped_base + "/" + "/".join(cleaned)
    return os.path.join(base, *parts)


# ---------------------------------------------------------------------------
# Scheme-aware directory operations
#
# These helpers let the dump/restore code treat a filesystem directory
# and an ``s3://bucket/prefix/`` identically.  For S3, "directory" is a
# social construct: there are only objects and slashes in their names.
# We emulate the filesystem-ish contract callers expect:
#
#   * ``list_dir``  — immediate child names, no paths, sorted.
#   * ``is_dir``    — something is under this prefix.
#   * ``is_file``   — a concrete object / file exists at this location.
#   * ``makedirs``  — ensure a path exists (no-op for S3).
#
# ---------------------------------------------------------------------------


def list_dir(uri: str, *, s3_client=None) -> list[str]:
    """List immediate children of a directory or S3 prefix.

    Returns child names (not full paths) sorted alphabetically.  Mirrors
    :func:`os.listdir`'s contract as closely as the two storage models
    allow:

    * **Local**  — raises :class:`FileNotFoundError` when *uri* doesn't
      exist (propagated straight through from :func:`os.listdir`).
    * **S3**     — a missing / unused prefix returns ``[]`` without any
      exception.  Prefixes in S3 don't exist independently of the
      objects underneath them, so there's nothing to be "missing".

    Subdirectory detection for S3 uses ``Delimiter='/'``.  An entry in
    ``CommonPrefixes`` becomes a child name with its trailing slash
    stripped.  An entry in ``Contents`` whose name (after stripping the
    request prefix) is empty is skipped — that's the "directory marker"
    object some tools create at the prefix itself.

    Paginates automatically via boto3's paginator so prefixes with >1000
    objects work without any caller-side fiddling.
    """
    if is_s3_uri(uri):
        client = s3_client or make_s3_client()
        bucket, prefix = _parse_s3_prefix(uri)
        names: set[str] = set()
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
            for obj in page.get("Contents", []) or []:
                name = obj["Key"][len(prefix) :]
                # Skip "directory marker" objects (key == prefix).
                if name:
                    names.add(name)
            for cp in page.get("CommonPrefixes", []) or []:
                sub = cp["Prefix"][len(prefix) :].rstrip("/")
                if sub:
                    names.add(sub)
        return sorted(names)
    return sorted(os.listdir(uri))


def is_dir(uri: str, *, s3_client=None) -> bool:
    """True if *uri* is a directory (local) or an S3 prefix with content.

    For S3 we do a single ``ListObjectsV2`` with ``MaxKeys=1`` and no
    delimiter — the cheapest way to answer "does anything live under
    this prefix?".  We don't use :func:`is_file` here because in
    S3-speak a key is both a file and a prefix; the two concepts
    overlap.  A zero-result answer means neither.
    """
    if is_s3_uri(uri):
        client = s3_client or make_s3_client()
        bucket, prefix = _parse_s3_prefix(uri)
        resp = client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        return resp.get("KeyCount", 0) > 0
    return os.path.isdir(uri)


def is_file(uri: str, *, s3_client=None) -> bool:
    """True if *uri* is a regular file (local) or an existing S3 key."""
    if is_s3_uri(uri):
        return S3Blob.from_uri(uri, client=s3_client).exists()
    return os.path.isfile(uri)


def makedirs(uri: str, *, exist_ok: bool = True) -> None:
    """Ensure a directory exists.

    No-op for ``s3://`` URIs: S3 has no real directories — a ``PutObject``
    to ``s3://b/p/k`` implicitly makes ``s3://b/p/`` visible in
    prefix-delimited listings.  Eliding the call here lets callers write
    ``makedirs(output_dir); open_write(...)`` uniformly.

    For local paths, defers to :func:`os.makedirs`.
    """
    if is_s3_uri(uri):
        return
    os.makedirs(uri, exist_ok=exist_ok)


# ---------------------------------------------------------------------------
# Materialisation helpers
# ---------------------------------------------------------------------------


# How many bytes to pull off a blob at a time when downloading to a temp
# file.  Matches RESTORE_CHUNK_SIZE on the restore side — large enough to
# amortise per-read overhead, small enough to keep SIGINT responsive.
_LOCALIZE_CHUNK_SIZE = 1 << 20  # 1 MiB


@contextmanager
def localize(blob: Blob) -> Iterator[str]:
    """Yield a local filesystem path to *blob*'s contents.

    Some consumers (``psql -f``, ``pg_restore``) take a path argument and
    cannot read from a stream or an ``s3://`` URI directly.  This helper
    lets callers stay scheme-agnostic:

    * :class:`LocalBlob` — yield the underlying path verbatim.  Zero-copy,
      no temp file, no extra disk I/O.  The yielded path is the real
      blob, so callers must not ``unlink`` it.
    * any other backend (today: :class:`S3Blob`) — stream the blob into
      a :class:`tempfile.NamedTemporaryFile`, yield its path, and delete
      the temp file on exit.  The download uses the same chunked reader
      as the dump pipeline so gigabyte-sized DDL archives don't balloon
      Python's RSS.

    The context manager always cleans up its own temp file, including
    on exceptions — the caller never needs to handle deletion.
    """
    if isinstance(blob, LocalBlob):
        # Reuse the real file — no need to copy.  ``uri`` is the path.
        yield blob.uri
        return

    # Remote blob: materialise to a temp file.  NamedTemporaryFile is
    # created in the OS-default temp dir so the caller doesn't need
    # write access to any specific location.  ``delete=False`` lets us
    # close the file before handing the path off to a subprocess
    # (Windows and some Linux setups refuse concurrent opens).
    tmp = tempfile.NamedTemporaryFile(
        prefix="backup-localize-",
        suffix=".blob",
        delete=False,
    )
    try:
        # Always close the tempfile before yielding (or before
        # re-raising) so no buffered writes linger and no file handle
        # outlives the download phase — otherwise a failed S3 read
        # would leak an open fd until GC on top of the unlink.
        try:
            with blob.open_read() as src:
                while True:
                    chunk = src.read(_LOCALIZE_CHUNK_SIZE)
                    if not chunk:
                        break
                    tmp.write(chunk)
        finally:
            tmp.close()
        yield tmp.name
    finally:
        try:
            os.unlink(tmp.name)
        except FileNotFoundError:
            pass
