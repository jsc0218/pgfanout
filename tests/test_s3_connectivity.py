#!/usr/bin/env python3
"""Standalone smoke test: verify S3 round-trip against an S3-compatible endpoint.

Not part of the pytest suite — collected at module level via
``conftest.py::collect_ignore`` because it talks to a real S3-compatible
endpoint (AWS, Ceph RGW, MinIO, ...) and requires live credentials.

All secrets and target locations come from environment variables so no
credentials land in the source tree.  The variable names intentionally
mirror the ones consumed by :func:`storage.make_s3_client` so one
``export`` block works for both this smoke test and the dump/restore
scripts:

    Required
    --------
    AWS_ACCESS_KEY_ID        — access key for the bucket
    AWS_SECRET_ACCESS_KEY    — secret key for the bucket
    S3_BUCKET                — name of the bucket to round-trip against

    Optional
    --------
    AWS_ENDPOINT_URL_S3      — endpoint URL (unset on real AWS, set on
                               Ceph RGW / MinIO / LocalStack)
    AWS_S3_ENDPOINT_URL      — legacy alias for the above
    AWS_REGION               — region (default: us-east-1)
    AWS_S3_VERIFY_SSL        — ``false`` disables TLS verify
                               (Ceph with self-signed certs), ``true``
                               forces it, any other value is treated as
                               a CA-bundle path.  Default: verify.
    S3_TEST_KEY              — object key used for the round-trip
                               (default: backup-test/connectivity-test.txt).

Example:
    export AWS_ACCESS_KEY_ID=...
    export AWS_SECRET_ACCESS_KEY=...
    export AWS_ENDPOINT_URL_S3=https://ceph.example.com:7483
    export AWS_S3_VERIFY_SSL=false
    export S3_BUCKET=my-bucket
    python3 tests/test_s3_connectivity.py
"""

import hashlib
import os
import sys

import boto3
import urllib3
from botocore.exceptions import ClientError, NoCredentialsError


def _require_env(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        sys.exit(
            f"ERROR: required environment variable {name} is not set.\n"
            "See the module docstring for the full list."
        )
    return val


def _resolve_verify() -> bool | str:
    raw = os.environ.get("AWS_S3_VERIFY_SSL")
    if raw is None or raw == "":
        return True
    lower = raw.strip().lower()
    if lower in ("0", "false", "no", "off"):
        return False
    if lower in ("1", "true", "yes", "on"):
        return True
    return raw


def main() -> None:
    access_key = _require_env("AWS_ACCESS_KEY_ID")
    secret_key = _require_env("AWS_SECRET_ACCESS_KEY")
    bucket = _require_env("S3_BUCKET")

    endpoint = os.environ.get("AWS_ENDPOINT_URL_S3") or os.environ.get("AWS_S3_ENDPOINT_URL")
    region = os.environ.get("AWS_REGION") or "us-east-1"
    verify = _resolve_verify()
    test_key = os.environ.get("S3_TEST_KEY") or "backup-test/connectivity-test.txt"

    # Suppress InsecureRequestWarning only when the user actually opted
    # out of TLS verification — otherwise we'd hide legitimate cert
    # problems on correctly configured endpoints.
    if verify is False:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            region_name=region,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            verify=verify,
        )
    except NoCredentialsError:
        sys.exit("ERROR: boto3 rejected credentials (malformed?).")

    # List bucket
    print(f"Listing s3://{bucket}/ ...")
    try:
        resp = s3.list_objects_v2(Bucket=bucket, MaxKeys=5)
        for obj in resp.get("Contents", []):
            print(f"  {obj['Key']}  ({obj['Size']} bytes)")
        if not resp.get("Contents"):
            print("  (empty bucket)")
        print("  List OK.\n")
    except ClientError as e:
        sys.exit(f"ERROR listing bucket: {e}")

    # Upload
    test_body = b"Hello from backup S3 connectivity test!\n"
    expected_md5 = hashlib.md5(test_body).hexdigest()
    print(f"Uploading to s3://{bucket}/{test_key} ...")
    try:
        s3.put_object(Bucket=bucket, Key=test_key, Body=test_body)
    except ClientError as e:
        sys.exit(f"ERROR uploading: {e}")
    print("  Upload OK.\n")

    # Download
    print(f"Downloading s3://{bucket}/{test_key} ...")
    try:
        resp = s3.get_object(Bucket=bucket, Key=test_key)
        downloaded = resp["Body"].read()
    except ClientError as e:
        sys.exit(f"ERROR downloading: {e}")

    actual_md5 = hashlib.md5(downloaded).hexdigest()
    if actual_md5 != expected_md5:
        sys.exit(f"ERROR: MD5 mismatch! expected={expected_md5}, got={actual_md5}")
    print(f"  Download OK. MD5 matches: {actual_md5}\n")

    # Cleanup
    print(f"Deleting s3://{bucket}/{test_key} ...")
    try:
        s3.delete_object(Bucket=bucket, Key=test_key)
    except ClientError as e:
        print(f"  WARNING: cleanup failed: {e}")
    else:
        print("  Deleted.\n")

    print("S3 connectivity test PASSED.")


if __name__ == "__main__":
    main()
