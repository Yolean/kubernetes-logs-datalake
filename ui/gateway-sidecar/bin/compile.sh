#!/usr/bin/env bash
set -eo pipefail

PACKAGE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ARCH="$(uname -m)"
OUTPUT_DIR="$PACKAGE_DIR/target/bin/$ARCH"

mkdir -p "$OUTPUT_DIR"

cd "$PACKAGE_DIR"
CGO_ENABLED=0 go build -o "$OUTPUT_DIR/gateway-sidecar" .

echo "Built $OUTPUT_DIR/gateway-sidecar"
