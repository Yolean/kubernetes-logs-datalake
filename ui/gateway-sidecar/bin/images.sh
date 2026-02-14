#!/usr/bin/env bash
set -eo pipefail

PACKAGE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
IMAGE_NAME="yolean/gateway-sidecar"
OUTPUT_DIR="$PACKAGE_DIR/../../images/gateway-sidecar/target/images"

# Add GOPATH/bin to PATH for ko
export PATH="${GOPATH:-$HOME/go}/bin:$PATH"

mkdir -p "$OUTPUT_DIR"

cd "$PACKAGE_DIR"

KO_DOCKER_REPO="${IMAGE_NAME}" \
  ko build --sbom=none --bare --platform=linux/arm64 --tarball="$OUTPUT_DIR/gateway-sidecar.tar" --push=false .

echo "Saved $OUTPUT_DIR/gateway-sidecar.tar"
