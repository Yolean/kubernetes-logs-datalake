#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME="fluentbit-demo"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
export KUBECONFIG="$SCRIPT_DIR/kubeconfig"

echo "==> Creating k3d cluster '$CLUSTER_NAME'"
k3d cluster create "$CLUSTER_NAME" \
  --kubeconfig-update-default=false

k3d kubeconfig get "$CLUSTER_NAME" > "$KUBECONFIG"
echo "    KUBECONFIG written to $KUBECONFIG"

echo "==> Importing local images into k3d"
k3d image import -c "$CLUSTER_NAME" yolean/fluentbit:latest

echo "==> Applying kustomize manifests"
kubectl apply -k "$SCRIPT_DIR"
kubectl rollout status statefulset/versitygw --timeout=120s

echo "==> Creating S3 bucket 'fluentbit-logs'"
kubectl run create-bucket --rm -i --restart=Never \
  --image=amazon/aws-cli:2.22.35 \
  --env=AWS_ACCESS_KEY_ID=demoaccess \
  --env=AWS_SECRET_ACCESS_KEY=demosecret \
  --env=AWS_DEFAULT_REGION=us-east-1 \
  --command -- aws s3 mb s3://fluentbit-logs --endpoint-url http://versitygw:7070

echo "==> Waiting for rollouts"
kubectl rollout status daemonset/fluent-bit --timeout=60s
kubectl rollout status deployment/log-generator --timeout=60s

echo ""
echo "=== Setup complete ==="
echo ""
echo "Usage:"
echo "  export KUBECONFIG=$KUBECONFIG"
echo "  kubectl logs -l app=fluent-bit                              # check for S3 upload messages"
echo "  kubectl logs -l app=log-generator                           # verify log generation"
echo "  kubectl exec versitygw-0 -- ls -la /data/fluentbit-logs/    # parquet files"
echo ""
echo "Teardown:"
echo "  bash $SCRIPT_DIR/teardown.sh"
