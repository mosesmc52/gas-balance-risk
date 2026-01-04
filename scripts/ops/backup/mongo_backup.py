#!/usr/bin/env python3
"""
Backup MongoDB to a compressed archive and upload to DigitalOcean Spaces.

Requires:
- mongodump installed and available on PATH
- tar available on PATH
- boto3 configured via SpacesClient env vars:
  SPACES_KEY, SPACES_SECRET, SPACES_BUCKET, SPACES_REGION (default nyc3),
  optional: SPACES_ENDPOINT, SPACES_CDN_BASE

Mongo env vars:
- MONGO_URI (required)
Optional:
- MONGO_DB (if set, dump only this DB; otherwise full instance)
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass

# Import your existing helper
# Ensure spaces.py is on your PYTHONPATH (e.g., project root) or adjust import accordingly.
from spaces import SpacesClient


@dataclass(frozen=True)
class BackupResult:
    key: str
    url: str
    bytes_uploaded: int
    created_utc: str


def _utc_stamp() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _run(cmd: list[str]) -> None:
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if p.returncode != 0:
        raise RuntimeError(
            "Command failed:\n"
            f"  cmd: {' '.join(cmd)}\n"
            f"  code: {p.returncode}\n"
            f"  stdout:\n{p.stdout}\n"
            f"  stderr:\n{p.stderr}\n"
        )


def _ensure_tool(name: str) -> None:
    if shutil.which(name) is None:
        raise RuntimeError(f"Required tool not found on PATH: {name}")


def _build_public_url(spaces: SpacesClient, key: str) -> str:
    """
    DO Spaces object URLs are:
      - If using CDN: <cdn_base>/<key>
      - Otherwise: https://<bucket>.<region>.digitaloceanspaces.com/<key>

    NOTE: Your SpacesClient.public_url() method, as written, appears to double-insert the bucket.
    This function avoids that by constructing a known-correct URL format.
    """
    if spaces.cdn_base:
        return spaces.cdn_base.rstrip("/") + "/" + key.lstrip("/")

    # Default DO Spaces virtual-hosted-style URL:
    # https://{bucket}.{region}.digitaloceanspaces.com/{key}
    region = spaces.region or os.getenv("SPACES_REGION", "nyc3")
    bucket = spaces.bucket
    return f"https://{bucket}.{region}.digitaloceanspaces.com/{key.lstrip('/')}"


def _mongodump(mongo_uri: str, out_dir: str, mongo_db: str | None) -> None:
    cmd = ["mongodump", f"--uri={mongo_uri}", f"--out={out_dir}"]
    if mongo_db:
        cmd.append(f"--db={mongo_db}")
    _run(cmd)


def _tar_gz_dir(src_dir: str, out_path: str) -> None:
    # Create a tar.gz of the directory contents
    # tar -C <src_dir> -czf <out_path> .
    _run(["tar", "-C", src_dir, "-czf", out_path, "."])


def _upload_file(spaces: SpacesClient, file_path: str, key: str, acl: str) -> int:
    size = os.path.getsize(file_path)
    with open(file_path, "rb") as f:
        spaces.client.put_object(
            Bucket=spaces.bucket,
            Key=key,
            Body=f,
            ACL=acl,
            ContentType="application/gzip",
        )
    return size


def _list_keys_with_prefix(spaces: SpacesClient, prefix: str) -> list[str]:
    keys: list[str] = []
    continuation = None

    while True:
        kwargs = {"Bucket": spaces.bucket, "Prefix": prefix}
        if continuation:
            kwargs["ContinuationToken"] = continuation

        resp = spaces.client.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []) or []:
            k = obj.get("Key")
            if k:
                keys.append(k)

        if resp.get("IsTruncated"):
            continuation = resp.get("NextContinuationToken")
        else:
            break

    return keys


def _delete_keys(spaces: SpacesClient, keys: list[str]) -> None:
    # Batch delete up to 1000 at a time
    for i in range(0, len(keys), 1000):
        batch = keys[i : i + 1000]
        spaces.client.delete_objects(
            Bucket=spaces.bucket,
            Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True},
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backup MongoDB to DigitalOcean Spaces."
    )
    parser.add_argument(
        "--mongo-uri",
        default=os.getenv("MONGO_URI"),
        help="MongoDB connection URI (or env MONGO_URI).",
    )
    parser.add_argument(
        "--mongo-db",
        default=os.getenv("MONGO_DB"),
        help="Optional DB name; if omitted dumps all DBs.",
    )
    parser.add_argument(
        "--prefix",
        default="backups/mongo",
        help="Spaces key prefix, e.g. backups/mongo",
    )
    parser.add_argument(
        "--name",
        default=None,
        help="Logical backup name (defaults to mongo-db or 'all').",
    )
    parser.add_argument(
        "--acl",
        default="private",
        choices=["private", "public-read"],
        help="Spaces object ACL.",
    )
    parser.add_argument(
        "--retention",
        type=int,
        default=0,
        help="Keep N most recent backups under prefix/name; 0 disables cleanup.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Create archive but do not upload/delete.",
    )
    args = parser.parse_args()

    if not args.mongo_uri:
        print("ERROR: missing --mongo-uri or env MONGO_URI", file=sys.stderr)
        return 2

    _ensure_tool("mongodump")
    _ensure_tool("tar")

    spaces = SpacesClient()  # uses env vars for Spaces creds/config

    stamp = _utc_stamp()
    logical_name = args.name or (args.mongo_db if args.mongo_db else "all")

    # Key layout:
    # backups/mongo/<logical_name>/<YYYY>/<MM>/<DD>/mongo_<logical_name>_<stamp>.tar.gz
    now = dt.datetime.now(dt.timezone.utc)
    key = (
        f"{args.prefix.rstrip('/')}/"
        f"{logical_name}/"
        f"{now:%Y}/{now:%m}/{now:%d}/"
        f"mongo_{logical_name}_{stamp}.tar.gz"
    )

    with tempfile.TemporaryDirectory(prefix="mongo_backup_") as td:
        dump_dir = os.path.join(td, "dump")
        os.makedirs(dump_dir, exist_ok=True)

        archive_path = os.path.join(td, f"mongo_{logical_name}_{stamp}.tar.gz")

        # 1) Dump
        _mongodump(args.mongo_uri, dump_dir, args.mongo_db)

        # 2) Archive
        _tar_gz_dir(dump_dir, archive_path)

        size = os.path.getsize(archive_path)
        print(f"[ok] created archive: {archive_path} ({size:,} bytes)")
        print(f"[plan] upload to: s3://{spaces.bucket}/{key}")

        if args.dry_run:
            print("[dry-run] skipping upload and retention cleanup")
            return 0

        # 3) Upload
        uploaded_size = _upload_file(spaces, archive_path, key=key, acl=args.acl)
        url = _build_public_url(spaces, key)
        print(f"[ok] uploaded: {uploaded_size:,} bytes")
        print(f"[ok] url: {url}")

    # 4) Optional retention cleanup (by lexicographic key order works due to YYYY/MM/DD + timestamp naming)
    if args.retention and args.retention > 0:
        base_prefix = f"{args.prefix.rstrip('/')}/{logical_name}/"
        keys = sorted(_list_keys_with_prefix(spaces, base_prefix))
        if len(keys) > args.retention:
            to_delete = keys[: len(keys) - args.retention]
            print(
                f"[cleanup] deleting {len(to_delete)} old backups under {base_prefix} (retention={args.retention})"
            )
            _delete_keys(spaces, to_delete)
            print("[cleanup] done")
        else:
            print(
                f"[cleanup] nothing to delete (found {len(keys)} <= retention {args.retention})"
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
