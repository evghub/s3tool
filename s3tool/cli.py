from __future__ import annotations

import json
import os
from typing import Optional

import click

from .s3ops import summarize_bucket, download_object, upload_file


@click.group(help="Small S3 helper to summarize buckets and upload/download objects.")
@click.option("--profile", "aws_profile", default=None, help="AWS profile name (overrides env).")
@click.option("--region", default=None, help="AWS region (e.g., us-east-1).")
@click.pass_context
def main(ctx: click.Context, aws_profile: Optional[str], region: Optional[str]) -> None:
    ctx.ensure_object(dict)
    ctx.obj["profile"] = aws_profile
    ctx.obj["region"] = region


@main.command("summary", help="Show object count and total size for a bucket/prefix.")
@click.argument("bucket")
@click.option("--prefix", default=None, help="Optional prefix to limit the scan (e.g., logs/2025/).")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON.")
@click.pass_context
def summary_cmd(ctx: click.Context, bucket: str, prefix: Optional[str], as_json: bool) -> None:
    prof = ctx.obj["profile"]
    region = ctx.obj["region"]
    result = summarize_bucket(bucket, prefix, aws_profile=prof, region=region)
    if as_json:
        click.echo(
            json.dumps(
                {
                    "bucket": result.bucket,
                    "prefix": result.prefix,
                    "object_count": result.object_count,
                    "total_bytes": result.total_bytes,
                    "human_size": result.human_readable_size(),
                },
                indent=2,
            )
        )
    else:
        click.echo(f"Bucket:       {result.bucket}")
        click.echo(f"Prefix:       {result.prefix or '(none)'}")
        click.echo(f"Objects:      {result.object_count}")
        click.echo(f"Total size:   {result.total_bytes} bytes ({result.human_readable_size()})")


@main.command("download", help="Download an object to a local path.")
@click.argument("bucket")
@click.argument("key")
@click.argument("dest_path")
@click.pass_context
def download_cmd(ctx: click.Context, bucket: str, key: str, dest_path: str) -> None:
    prof = ctx.obj["profile"]
    region = ctx.obj["region"]
    path = download_object(bucket, key, dest_path, aws_profile=prof, region=region)
    click.echo(f"Downloaded to: {os.path.abspath(path)}")


@main.command("upload", help="Upload a local file to S3.")
@click.argument("src_path")
@click.argument("bucket")
@click.argument("key")
@click.option("--content-type", default=None, help="Set Content-Type metadata.")
@click.option("--sse", default=None, help="Server-side encryption (AES256 or aws:kms).")
@click.option("--acl", default=None, help="Object ACL (e.g., private, public-read).")
@click.pass_context
def upload_cmd(
    ctx: click.Context,
    src_path: str,
    bucket: str,
    key: str,
    content_type: Optional[str],
    sse: Optional[str],
    acl: Optional[str],
) -> None:
    prof = ctx.obj["profile"]
    region = ctx.obj["region"]
    b, k = upload_file(
        src_path,
        bucket,
        key,
        content_type=content_type,
        sse=sse,
        acl=acl,
        aws_profile=prof,
        region=region,
    )
    click.echo(f"Uploaded: s3://{b}/{k}")
