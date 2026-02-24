#!/usr/bin/env python3
"""Inspect Arrow IPC files in S3 using pyarrow — independent format validator."""

import sys
import io
import pyarrow as pa
import boto3
import pyarrow.ipc as ipc


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

    # Pick the most recent file
    files.sort()
    key = files[-1]

    obj = s3.get_object(Bucket=bucket, Key=key)
    data = obj["Body"].read()

    print(f"File: s3://{bucket}/{key}")
    print(f"Size: {len(data)} bytes")

    reader = ipc.open_file(io.BytesIO(data))
    schema = reader.schema

    total_rows = sum(reader.get_batch(i).num_rows for i in range(reader.num_record_batches))
    print(f"Record batches: {reader.num_record_batches}")
    print(f"Total rows: {total_rows}")
    print(f"Schema ({len(schema)} fields):")
    for field in schema:
        print(f"  {field.name}: {field.type}")

    if reader.num_record_batches > 0:
        batch = reader.get_batch(0)
        if batch.num_rows > 0:
            print("Sample (first row):")
            for i, field in enumerate(schema):
                scalar = batch.column(i)[0]
                if pa.types.is_timestamp(field.type):
                    # .value gives raw int (nanos), avoids datetime overflow
                    val = scalar.value
                else:
                    val = scalar.as_py()
                print(f"  {field.name}: {val}")


if __name__ == "__main__":
    main()
