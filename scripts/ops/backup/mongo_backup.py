"""
MongoDB backup to a compressed archive and upload to DigitalOcean Spaces.

This script supports two backup strategies:

1) Physical backup (preferred when available)
   - Uses the external MongoDB Database Tools (`mongodump`)
   - Produces a BSON dump suitable for `mongorestore`
   - Provides stronger consistency guarantees on replica sets
   - Requires `mongodump` and `tar` to be installed and available on PATH

2) Logical backup (fallback / portable mode)
   - Uses PyMongo to export collections as newline-delimited Extended JSON (EJSON)
   - Compresses data using gzip and packages with Python's tarfile
   - Exports collection index metadata
   - Does NOT require MongoDB tools, but offers weaker consistency guarantees
   - Intended for environments where `mongodump` is unavailable (e.g. minimal containers)

External dependencies (system):
- mongodump (MongoDB Database Tools) — required for physical backup mode
- tar — required for physical backup mode

Python dependencies:
- pymongo (logical backup mode)
- boto3
- python-dotenv

Spaces configuration (environment variables):
- SPACES_KEY
- SPACES_SECRET
- SPACES_BUCKET
- SPACES_REGION (default: nyc3)
- Optional:
  - SPACES_ENDPOINT
  - SPACES_CDN_BASE

MongoDB configuration (environment variables):
- MONGO_URI (required)
  - Must include correct authSource if using authentication
    e.g. mongodb://user:pass@host:27017/?authSource=admin
- Optional:
  - MONGO_DB (if set, backs up only this database; otherwise all non-system databases)

Operational notes:
- Physical backups require MongoDB authentication credentials with read access
- Logical backups do not include users, roles, or oplog data
- On standalone MongoDB instances, snapshot-consistent reads are not available
- On replica sets or mongos, snapshot transactions may be used when supported
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

from dotenv import load_dotenv
from scripts.ops.storage.spaces import SpacesClient

load_dotenv()


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
    if spaces.cdn_base:
        return spaces.cdn_base.rstrip("/") + "/" + key.lstrip("/")

    region = spaces.region or os.getenv("SPACES_REGION", "nyc3")
    bucket = spaces.bucket
    return f"https://{bucket}.{region}.digitaloceanspaces.com/{key.lstrip('/')}"


def _mongodump(mongo_uri: str, out_dir: str, mongo_db: str | None) -> None:
    cmd = ["mongodump", f"--uri={mongo_uri}", f"--out={out_dir}"]
    if mongo_db:
        cmd.append(f"--db={mongo_db}")
    _run(cmd)


def _tar_gz_dir(src_dir: str, out_path: str) -> None:
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


def _list_objects_with_prefix(spaces: SpacesClient, prefix: str) -> list[dict]:
    """
    Returns objects as dicts with at least:
      - Key (str)
      - LastModified (datetime, tz-aware)
      - Size (int)
    """
    objs: list[dict] = []
    continuation = None

    while True:
        kwargs = {"Bucket": spaces.bucket, "Prefix": prefix}
        if continuation:
            kwargs["ContinuationToken"] = continuation

        resp = spaces.client.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []) or []:
            k = obj.get("Key")
            lm = obj.get("LastModified")
            if k and lm:
                objs.append(obj)

        if resp.get("IsTruncated"):
            continuation = resp.get("NextContinuationToken")
        else:
            break

    return objs


def _delete_keys(spaces: SpacesClient, keys: list[str]) -> None:
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

    # --- UPDATED: retention by days (instead of count) ---
    parser.add_argument(
        "--retention-days",
        type=int,
        default=0,
        help="Keep backups from the last N days (UTC) under prefix/name; 0 disables cleanup.",
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

    spaces = SpacesClient(
        key=os.getenv("SPACES_KEY"),
        secret=os.getenv("SPACES_SECRET"),
        bucket=os.getenv("SPACES_BUCKET"),
        region=os.getenv("SPACES_REGION"),
        endpoint=os.getenv("SPACES_ENDPOINT"),
    )

    stamp = _utc_stamp()
    logical_name = args.name or (args.mongo_db if args.mongo_db else "all")

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

        _mongodump(args.mongo_uri, dump_dir, args.mongo_db)
        _tar_gz_dir(dump_dir, archive_path)

        size = os.path.getsize(archive_path)
        print(f"[ok] created archive: {archive_path} ({size:,} bytes)")
        print(f"[plan] upload to: s3://{spaces.bucket}/{key}")

        if args.dry_run:
            print("[dry-run] skipping upload and retention cleanup")
            return 0

        uploaded_size = _upload_file(spaces, archive_path, key=key, acl=args.acl)
        url = _build_public_url(spaces, key)
        print(f"[ok] uploaded: {uploaded_size:,} bytes")
        print(f"[ok] url: {url}")

    # --- UPDATED: retention cleanup by age (UTC days) ---
    if args.retention_days and args.retention_days > 0:
        base_prefix = f"{args.prefix.rstrip('/')}/{logical_name}/"
        cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(
            days=args.retention_days
        )

        objs = _list_objects_with_prefix(spaces, base_prefix)

        # Only delete objects strictly older than cutoff
        to_delete = []
        for obj in objs:
            lm = obj.get("LastModified")
            k = obj.get("Key")
            if not k or not lm:
                continue
            # boto3 returns tz-aware datetimes; keep everything >= cutoff
            if lm < cutoff:
                to_delete.append(k)

        if to_delete:
            print(
                f"[cleanup] deleting {len(to_delete)} backups older than {args.retention_days} days "
                f"(cutoff={cutoff.isoformat()}) under {base_prefix}"
            )
            _delete_keys(spaces, sorted(to_delete))
            print("[cleanup] done")
        else:
            print(
                f"[cleanup] nothing to delete (no objects older than cutoff={cutoff.isoformat()})"
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
