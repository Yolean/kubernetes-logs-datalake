#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME="fluentbit-demo"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
K3D_DIR="$SCRIPT_DIR/k3d-example"
KUBECONFIG="$K3D_DIR/kubeconfig"
KUBECTL="kubectl --kubeconfig=$KUBECONFIG"

# --- Helpers ---

fail() {
  echo "FAIL: $1" >&2
  echo "--- fluent-bit logs ---" >&2
  $KUBECTL logs -l app=fluent-bit --tail=50 2>/dev/null || true
  exit 1
}

wait_for_rollout() {
  local kind="$1" name="$2" timeout="${3:-120s}"
  echo "  Waiting for $kind/$name..."
  $KUBECTL rollout status "$kind/$name" --timeout="$timeout" || fail "$kind/$name rollout timed out"
}

# --- 1. Cluster ---

echo "==> Ensuring k3d cluster '$CLUSTER_NAME'"
if k3d cluster list "$CLUSTER_NAME" &>/dev/null; then
  echo "  Cluster already exists, reusing"
else
  k3d cluster create "$CLUSTER_NAME" \
    --kubeconfig-update-default=false \
    -p "30070:30070@server:0"
  k3d kubeconfig get "$CLUSTER_NAME" > "$KUBECONFIG"
fi

# --- 2. Build images ---

echo "==> Building container images"
./node_modules/.bin/turbo images --output-logs=new-only

# --- 3. Import into k3d ---

echo "==> Importing images into k3d"
k3d image import -c "$CLUSTER_NAME" \
  "$SCRIPT_DIR/images/fluentbit/target/images/fluentbit.tar" \
  "$SCRIPT_DIR/images/versitygw/target/images/versitygw.tar"

# --- 4. Apply manifests ---

echo "==> Applying kustomize manifests"
$KUBECTL create namespace ui --dry-run=client -o yaml | $KUBECTL apply -f -
$KUBECTL apply -k "$K3D_DIR"

# --- 5. Wait for workloads ---

echo "==> Waiting for workloads"
wait_for_rollout statefulset versitygw 120s

echo "  Creating S3 bucket (idempotent)..."
$KUBECTL run create-bucket --rm -i --restart=Never \
  --image=amazon/aws-cli:2.22.35 \
  --env=AWS_ACCESS_KEY_ID=demoaccess \
  --env=AWS_SECRET_ACCESS_KEY=demosecret \
  --env=AWS_DEFAULT_REGION=us-east-1 \
  --command -- aws s3 mb s3://fluentbit-logs --endpoint-url http://versitygw:7070 \
  2>/dev/null || true

wait_for_rollout daemonset fluent-bit 60s

# Restart fluent-bit so it picks up the bucket cleanly (it may have
# dropped early chunks before the bucket existed)
$KUBECTL rollout restart daemonset/fluent-bit
wait_for_rollout daemonset fluent-bit 60s

wait_for_rollout deployment log-generator 60s

# --- 6. Poll for parquet data ---

echo "==> Waiting for parquet data to appear (up to 120s)..."

POLL_TIMEOUT=120
POLL_INTERVAL=5
elapsed=0

LAST_ERROR=""
while true; do
  OUTPUT=$(./y-logcli --context=dev query '{namespace="default"}' 2>&1) && break
  LAST_ERROR="$OUTPUT"
  elapsed=$((elapsed + POLL_INTERVAL))
  if [ "$elapsed" -ge "$POLL_TIMEOUT" ]; then
    echo "  Last y-logcli output: $LAST_ERROR" >&2
    fail "No parquet data appeared within ${POLL_TIMEOUT}s"
  fi
  echo "  No data yet, retrying in ${POLL_INTERVAL}s... (${elapsed}/${POLL_TIMEOUT}s)"
  sleep "$POLL_INTERVAL"
done

echo "==> Data found, running assertions..."

# --- 7. Assertions ---

ERRORS=0

# Note: use here-strings (<<<) not pipes for grep -q, because
# pipefail + grep -q causes SIGPIPE when grep exits early on match.

# 7a. log-generator messages exist
if grep -q "hello from log-generator" <<< "$OUTPUT"; then
  echo "  PASS: log-generator messages found"
else
  echo "  FAIL: log-generator messages not found" >&2
  ERRORS=$((ERRORS + 1))
fi

# 7b. JSON structure - check that parquet has expected columns
if grep -q "namespace" <<< "$OUTPUT"; then
  echo "  PASS: partition column 'namespace' present"
else
  echo "  FAIL: partition column 'namespace' missing" >&2
  ERRORS=$((ERRORS + 1))
fi

if grep -q "container" <<< "$OUTPUT"; then
  echo "  PASS: partition column 'container' present"
else
  echo "  FAIL: partition column 'container' missing" >&2
  ERRORS=$((ERRORS + 1))
fi

# 7c. Cluster tag added by fluent-bit filter
LINES_OUTPUT=$(./y-logcli --context=dev query '{namespace="default"}' -o lines 2>&1)
if grep -q "cluster.*=.*dev" <<< "$LINES_OUTPUT"; then
  echo "  PASS: cluster tag 'dev' present in records"
else
  echo "  FAIL: cluster tag 'dev' not found in records" >&2
  ERRORS=$((ERRORS + 1))
fi

# 7d. SIGTERM flush - verify buffered data is flushed to S3 on shutdown
echo "==> Testing SIGTERM flush..."
MARKER="sigterm-test-$(date +%s)"

# Emit a unique marker log
$KUBECTL run "$MARKER" --rm -i --restart=Never \
  --image=busybox:1.37 \
  --command -- echo "$MARKER" \
  2>/dev/null

# Wait for fluent-bit tail to pick it up (must exceed Refresh_Interval=5)
sleep 8

# Kill fluent-bit before upload_timeout (15s) triggers a regular flush
$KUBECTL delete pod -l app=fluent-bit --grace-period=5
wait_for_rollout daemonset fluent-bit 60s

# Poll for the marker in S3 (up to 30s for the new pod to be ready)
FLUSH_TIMEOUT=30
FLUSH_INTERVAL=5
flush_elapsed=0
FLUSH_FOUND=false

while [ "$flush_elapsed" -lt "$FLUSH_TIMEOUT" ]; do
  FLUSH_OUTPUT=$(./y-logcli --context=dev query '{namespace="default"}' -o raw 2>&1) || true
  if grep -q "$MARKER" <<< "$FLUSH_OUTPUT"; then
    FLUSH_FOUND=true
    break
  fi
  flush_elapsed=$((flush_elapsed + FLUSH_INTERVAL))
  echo "  Marker not yet in S3, retrying in ${FLUSH_INTERVAL}s... (${flush_elapsed}/${FLUSH_TIMEOUT}s)"
  sleep "$FLUSH_INTERVAL"
done

if [ "$FLUSH_FOUND" = true ]; then
  echo "  PASS: SIGTERM flush - marker '$MARKER' found in S3"
else
  echo "  FAIL: SIGTERM flush - marker '$MARKER' not found in S3 (data lost on shutdown)" >&2
  ERRORS=$((ERRORS + 1))
fi

# --- 8. Result ---

echo ""
if [ "$ERRORS" -gt 0 ]; then
  fail "$ERRORS assertion(s) failed"
else
  echo "PASS: All assertions passed - state is shippable"
  exit 0
fi
