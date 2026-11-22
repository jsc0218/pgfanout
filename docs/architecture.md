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

## Common to both paths

The subprocess stays registered in the cancel registry, a dedicated thread
drains stderr so warnings cannot deadlock the pipeline, and a non-zero exit
surfaces as `CalledProcessError` with the captured stderr attached.

## Where to read the code

* `src/pgfanout/common.py`: the two helpers above, `_real_fd`, cancellation.
* `src/pgfanout/storage.py`: the `Blob` interface, `LocalBlob`, `S3Blob`, the
  multipart writer (part size, abort on failure) and `localize`.
