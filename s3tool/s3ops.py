from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional, Tuple

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


@dataclass(frozen=True)
class S3Summary:
    bucket: str
    prefix: Optional[str]
    object_count: int
    total_bytes: int

    def human_readable_size(self) -> str:
        size = self.total_bytes
        for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
            if size < 1024:
                return f"{size:.2f} {unit}"
            size /= 1024
        return f"{size:.2f} EB"


def _session(aws_profile: Optional[str], region: Optional[str]):
    if aws_profile:
        os.environ["AWS_PROFILE"] = aws_profile
    return boto3.Session(profile_name=aws_profile or None, region_name=region or None)


def _s3_client(aws_profile: Optional[str], region: Optional[str]):
    sess = _session(aws_profile, region)
    # reasonable retries & timeouts
    cfg = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=5, read_timeout=60)
    return sess.client("s3", config=cfg)


def summarize_bucket(
    bucket: str,
    prefix: Optional[str] = None,
    *,
    aws_profile: Optional[str] = None,
    region: Optional[str] = None,
) -> S3Summary:
    """
    Walks the bucket (optionally under prefix) and returns object count & total size.
    Uses ListObjectsV2 pagination.
    """
    s3 = _s3_client(aws_profile, region)
    paginator = s3.get_paginator("list_objects_v2")

    total = 0
    count = 0

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix or ""):
        for obj in page.get("Contents", []):
            count += 1
            total += int(obj["Size"])

    return S3Summary(bucket=bucket, prefix=prefix, object_count=count, total_bytes=total)


def download_object(
    bucket: str,
    key: str,
    dest_path: str,
    *,
    aws_profile: Optional[str] = None,
    region: Optional[str] = None,
) -> str:
    """Download a single S3 object (key) to a local path. Returns the destination path."""
    s3 = _s3_client(aws_profile, region)
    os.makedirs(os.path.dirname(dest_path) or ".", exist_ok=True)
    try:
        s3.download_file(bucket, key, dest_path)
    except ClientError as e:
        raise RuntimeError(f"Failed to download s3://{bucket}/{key}: {e}") from e
    return dest_path


def upload_file(
    src_path: str,
    bucket: str,
    key: str,
    *,
    content_type: Optional[str] = None,
    sse: Optional[str] = None,
    acl: Optional[str] = None,
    aws_profile: Optional[str] = None,
    region: Optional[str] = None,
) -> Tuple[str, str]:
    """
    Upload a local file to S3 at key. Returns (bucket, key).
    Optional:
      - content_type (e.g., 'application/json')
      - sse (e.g., 'AES256' or 'aws:kms')
      - acl (e.g., 'private', 'public-read')
    """
    s3 = _s3_client(aws_profile, region)
    extra_args = {}
    if content_type:
        extra_args["ContentType"] = content_type
    if sse:
        extra_args["ServerSideEncryption"] = sse
    if acl:
        extra_args["ACL"] = acl

    try:
        if extra_args:
            s3.upload_file(src_path, bucket, key, ExtraArgs=extra_args)
        else:
            s3.upload_file(src_path, bucket, key)
    except ClientError as e:
        raise RuntimeError(f"Failed to upload {src_path} to s3://{bucket}/{key}: {e}") from e

    return bucket, key
