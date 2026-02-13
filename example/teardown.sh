#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME="fluentbit-demo"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
export KUBECONFIG="$SCRIPT_DIR/kubeconfig"

echo "==> Deleting k3d cluster '$CLUSTER_NAME'"
k3d cluster delete "$CLUSTER_NAME"

rm -f "$SCRIPT_DIR/kubeconfig"

echo ""
echo "Cluster deleted."
