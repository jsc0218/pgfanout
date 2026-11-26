# Architecture: how data gets from Postgres to storage

The [README](../README.md#-architecture-at-a-glance) shows the overall flow
and the split between the control plane (Python, `psycopg`) and the data
plane (`psql`, `pg_dump`, `pg_restore`). This page covers the data plane in
more detail.

For local files, Python never touches the payload bytes in the COPY path.
For S3 it relays them chunk by chunk, because the multipart upload needs the
bytes in Python.

`pgfanout.common.stream_subprocess_to_sink` (dump) and
`pgfanout.common.stream_source_to_subprocess` (restore) connect the `psql`
subprocess to the `Blob` via one of two paths, picked automatically per
call based on the destination type.

## Path 1: OS-direct fast path

Used when the `Blob` exposes a real OS file descriptor, which is every
`LocalBlob`. The helpers hand the fd straight to `subprocess.Popen` as
`stdout=` (dump) or `stdin=` (restore). `Popen` calls `dup2(fd, 1/0)` in the
child, so the subprocess writes to (or reads from) the file through its own
libc `write()` / `read()` calls. There is no Python in the data path of the
pgfanout process, and no GIL traffic from the streaming step, so N parallel
workers do not contend on the GIL.

A small daemon thread polls `os.fstat(fd).st_size` once a second to feed live
progress into the shared `bytes_written` dict that `progress_monitor`
consumes.

## Path 2: Python read/write fallback

Used when the `Blob` is a Python class (`_S3MultipartWriter`,
`_S3StreamReader`). The bytes have to pass through Python so the multipart
upload buffers can chunk them. A `while True: chunk = read(); write(chunk)`
loop pumps the data, with per-chunk `bytes_callback` updates. This is also
the path used by tests that hand in `io.BytesIO`.

For S3 the writer also overlaps reading and uploading: full parts are sent on
background threads (`PGFANOUT_S3_UPLOAD_THREADS`, see the README), so the
reader can keep draining `psql` while a part is on the wire. At most that many
parts are in flight, which bounds RAM at part size x (threads + 1) per file.
A failed upload is raised on a later write or at close, and the multipart
upload is aborted once the parts still in flight have finished.

Each part also carries a CRC32 that pgfanout computes with `zlib` and sends as
a header (the upload is created with `ChecksumAlgorithm=CRC32`, and each part's
checksum is repeated in `CompleteMultipartUpload`), so S3 verifies every part.
boto3's default instead streams each part through a Python wrapper with a
trailing checksum, which costs far more CPU under the GIL.

## Common to both paths

The subprocess stays registered in the cancel registry, a dedicated thread
drains stderr so warnings cannot deadlock the pipeline, and a non-zero exit
surfaces as `CalledProcessError` with the captured stderr attached.

## Where to read the code

* `src/pgfanout/common.py`: the two helpers above, `_real_fd`, cancellation.
* `src/pgfanout/storage.py`: the `Blob` interface, `LocalBlob`, `S3Blob`, the
  multipart writer (part size, abort on failure) and `localize`.
