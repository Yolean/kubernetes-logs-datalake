#!/usr/bin/env bash
set -eo pipefail

PACKAGE_DIR="$(cd "$(dirname "$0")/.." && pwd)"

mkdir -p "$PACKAGE_DIR/target/images"

cd "$PACKAGE_DIR"

ARCH="$(uname -m)" contain build . --tarball=target/images/gateway-sidecar.tar --push=false

echo "Saved $PACKAGE_DIR/target/images/gateway-sidecar.tar"
