#!/usr/bin/env bash
set -eo pipefail

PACKAGE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
IMAGE_NAME="yolean/fluentbit"
IMAGE_TAG="latest"
OUTPUT_DIR="$PACKAGE_DIR/target/images"

mkdir -p "$OUTPUT_DIR"

docker buildx build --load -t "$IMAGE_NAME:$IMAGE_TAG" "$PACKAGE_DIR"
docker save "$IMAGE_NAME:$IMAGE_TAG" -o "$OUTPUT_DIR/fluentbit.tar"

echo "Saved $OUTPUT_DIR/fluentbit.tar"
