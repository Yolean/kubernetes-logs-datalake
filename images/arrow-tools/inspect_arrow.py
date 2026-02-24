#!/usr/bin/env python3
"""Inspect Arrow IPC files in S3 using pyarrow — independent format validator."""

import sys
import io
from datetime import datetime, timezone
import pyarrow as pa
import boto3
import pyarrow.ipc as ipc


def format_timestamp_ns(ns_value):
    """Format nanoseconds-since-epoch as ISO 8601 with nanosecond precision."""
    secs = ns_value // 1_000_000_000
    nanos = ns_value % 1_000_000_000
    dt = datetime.fromtimestamp(secs, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + f".{nanos:09d}Z"


def format_value(scalar, field):
    """Format a scalar value for display."""
    if pa.types.is_timestamp(field.type):
        ns = scalar.value
        return f"{ns} ({format_timestamp_ns(ns)})"
    return scalar.as_py()


def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <bucket> <prefix> [extension]", file=sys.stderr)
        sys.exit(1)

    bucket = sys.argv[1]
    prefix = sys.argv[2]
    extension = sys.argv[3] if len(sys.argv) > 3 else ".arrow"

    endpoint_url = "http://versitygw:7070"
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id="demoaccess",
        aws_secret_access_key="demosecret",
        region_name="us-east-1",
    )

    # List objects with prefix
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    contents = response.get("Contents", [])
    files = [obj["Key"] for obj in contents if obj["Key"].endswith(extension)]

    if not files:
        print(f"No {extension} files found in s3://{bucket}/{prefix}", file=sys.stderr)
        sys.exit(1)

    # Pick the earliest file (first chronologically by path)
    files.sort()
    key = files[0]

    obj = s3.get_object(Bucket=bucket, Key=key)
    data = obj["Body"].read()

    print(f"File: s3://{bucket}/{key}")
    print(f"Size: {len(data)} bytes")

    reader = ipc.open_file(io.BytesIO(data))
    schema = reader.schema

    total_rows = sum(
        reader.get_batch(i).num_rows for i in range(reader.num_record_batches)
    )
    print(f"Record batches: {reader.num_record_batches}")
    print(f"Total rows: {total_rows}")
    print(f"Schema ({len(schema)} fields):")
    for field in schema:
        print(f"  {field.name}: {field.type}")

    if reader.num_record_batches > 0:
        batch = reader.get_batch(0)
        n_sample = min(batch.num_rows, 5)
        if n_sample > 0:
            print(f"Sample (first {n_sample} rows):")
            for row in range(n_sample):
                print(f"  row {row}:")
                for i, field in enumerate(schema):
                    val = format_value(batch.column(i)[row], field)
                    print(f"    {field.name}: {val}")


if __name__ == "__main__":
    main()
