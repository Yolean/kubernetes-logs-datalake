#!/usr/bin/env bash
set -eo pipefail

PACKAGE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
IMAGE_REF="docker.io/versity/versitygw:v1.2.0"
OUTPUT_DIR="$PACKAGE_DIR/target/images"

mkdir -p "$OUTPUT_DIR"

crane pull "$IMAGE_REF" "$OUTPUT_DIR/versitygw.tar"

echo "Saved $OUTPUT_DIR/versitygw.tar"
