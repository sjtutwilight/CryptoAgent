#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_SOURCE = REPO_ROOT / "runtime/data/dune/token-holders/1/0x514910771af9ca656af840dff83e8264ecf986ca"


def run_cmd(cmd: str) -> None:
    subprocess.run(cmd, shell=True, check=True, capture_output=True)


def upload_token_holders_test_data(
    source_path: Path,
    minio_container: str,
    bucket: str,
    target_prefix: str,
    minio_url: str,
    access_key: str,
    secret_key: str,
) -> int:
    if not source_path.exists():
        print(f"source path not found: {source_path}", file=sys.stderr)
        return 1

    check_cmd = f"docker ps --format '{{{{.Names}}}}' | grep -q '^{minio_container}$'"
    result = subprocess.run(check_cmd, shell=True, capture_output=True)
    if result.returncode != 0:
        print(f"minio container not running: {minio_container}", file=sys.stderr)
        return 1

    alias = "testminio"
    run_cmd(
        f"docker exec {minio_container} mc alias set {alias} {minio_url} {access_key} {secret_key}"
    )
    run_cmd(f"docker exec {minio_container} mkdir -p /tmp/upload-data")

    file_count = 0
    for file in source_path.glob("holders_*.json"):
        run_cmd(f"docker cp {file} {minio_container}:/tmp/upload-data/{file.name}")
        file_count += 1

    if file_count == 0:
        print(f"no holders_*.json files found: {source_path}", file=sys.stderr)
        return 1

    run_cmd(
        f"docker exec {minio_container} mc cp --recursive /tmp/upload-data/ {alias}/{bucket}/{target_prefix}/"
    )
    run_cmd(f"docker exec {minio_container} rm -rf /tmp/upload-data")

    ls_result = subprocess.run(
        f"docker exec {minio_container} mc ls {alias}/{bucket}/{target_prefix}/ | wc -l",
        shell=True,
        capture_output=True,
        text=True,
    )
    uploaded_count = int(ls_result.stdout.strip() or "0")

    print(
        f"uploaded {uploaded_count} files to s3a://{bucket}/{target_prefix}/ from {source_path}"
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upload token holders test data to MinIO")
    parser.add_argument(
        "--source-path",
        default=str(DEFAULT_SOURCE),
        help="local directory containing holders_*.json",
    )
    parser.add_argument("--minio-container", default="spark-lab-minio")
    parser.add_argument("--bucket", default="paimon-warehouse")
    parser.add_argument(
        "--target-prefix",
        default="test-data/token-holders/1/0x514910771af9ca656af840dff83e8264ecf986ca",
    )
    parser.add_argument("--minio-url", default="http://localhost:9000")
    parser.add_argument("--access-key", default="admin")
    parser.add_argument("--secret-key", default="password123")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    code = upload_token_holders_test_data(
        source_path=Path(args.source_path),
        minio_container=args.minio_container,
        bucket=args.bucket,
        target_prefix=args.target_prefix,
        minio_url=args.minio_url,
        access_key=args.access_key,
        secret_key=args.secret_key,
    )
    raise SystemExit(code)


if __name__ == "__main__":
    main()
