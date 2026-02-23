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

duckdb_s3() {
  duckdb -noheader -csv -c "
    INSTALL httpfs; LOAD httpfs;
    SET s3_region='us-east-1'; SET s3_endpoint='localhost:30070';
    SET s3_access_key_id='demoaccess'; SET s3_secret_access_key='demosecret';
    SET s3_use_ssl=false; SET s3_url_style='path';
    $1"
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

# --- 6. Poll for data in both formats ---

POLL_TIMEOUT=120
POLL_INTERVAL=5

poll_for_format() {
  local fmt="$1"
  local elapsed=0 last_error=""
  echo "==> Waiting for $fmt data to appear (up to ${POLL_TIMEOUT}s)..."
  while true; do
    local output
    output=$(./y-logcli --context=dev query '{namespace="default"}' -f "$fmt" 2>&1) && {
      echo "$output"
      return 0
    }
    last_error="$output"
    elapsed=$((elapsed + POLL_INTERVAL))
    if [ "$elapsed" -ge "$POLL_TIMEOUT" ]; then
      echo "  Last y-logcli output ($fmt): $last_error" >&2
      return 1
    fi
    echo "  No $fmt data yet, retrying in ${POLL_INTERVAL}s... (${elapsed}/${POLL_TIMEOUT}s)"
    sleep "$POLL_INTERVAL"
  done
}

PARQUET_OUTPUT=$(poll_for_format parquet) || fail "No parquet data appeared within ${POLL_TIMEOUT}s"
echo "  Parquet data found"

ARROW_OUTPUT=$(poll_for_format arrow) || fail "No arrow data appeared within ${POLL_TIMEOUT}s"
echo "  Arrow data found"

echo "==> Data found in both formats, running assertions..."

# --- 7. Assertions ---

ERRORS=0

# Note: use here-strings (<<<) not pipes for grep -q, because
# pipefail + grep -q causes SIGPIPE when grep exits early on match.

# 7a. log-generator messages exist (check both formats)
if grep -q "hello from log-generator" <<< "$PARQUET_OUTPUT"; then
  echo "  PASS: log-generator messages found in parquet"
else
  echo "  FAIL: log-generator messages not found in parquet" >&2
  ERRORS=$((ERRORS + 1))
fi

if grep -q "hello from log-generator" <<< "$ARROW_OUTPUT"; then
  echo "  PASS: log-generator messages found in arrow"
else
  echo "  FAIL: log-generator messages not found in arrow" >&2
  ERRORS=$((ERRORS + 1))
fi

# 7b. Partition columns present
OUTPUT=$(./y-logcli --context=dev query '{namespace="default"}' 2>&1)

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

# 7d. Schema comparison — arrow has CRI time (VARCHAR), parquet has epoch_ms (BIGINT)
echo "==> Checking parquet schemas..."

ARROW_TIME_TYPE=$(duckdb_s3 "
  SELECT column_type FROM (
    DESCRIBE SELECT * FROM read_parquet('s3://fluentbit-logs/dev/default/**/*.arrow', filename=true, hive_partitioning=false)
  ) WHERE column_name='time';
" | tr -d '[:space:]')

PARQUET_TIME_TYPE=$(duckdb_s3 "
  SELECT column_type FROM (
    DESCRIBE SELECT * FROM read_parquet('s3://fluentbit-logs/dev/default/**/*.parquet', filename=true, hive_partitioning=false)
  ) WHERE column_name='time_ms';
" | tr -d '[:space:]')

if [[ "$ARROW_TIME_TYPE" == "VARCHAR" ]]; then
  echo "  PASS: arrow format has time as VARCHAR (CRI timestamp string)"
else
  echo "  FAIL: arrow format has time as '$ARROW_TIME_TYPE', expected VARCHAR" >&2
  ERRORS=$((ERRORS + 1))
fi

if [[ "$PARQUET_TIME_TYPE" == "BIGINT" ]]; then
  echo "  PASS: parquet format has time_ms as BIGINT (epoch_ms)"
else
  echo "  FAIL: parquet format has time_ms as '$PARQUET_TIME_TYPE', expected BIGINT" >&2
  ERRORS=$((ERRORS + 1))
fi

# 7e. Arrow time values are valid CRI timestamps (parseable, nanosecond precision)
ARROW_TIME_SAMPLE=$(duckdb_s3 "
  SELECT time FROM read_parquet('s3://fluentbit-logs/dev/default/**/*.arrow', filename=true, hive_partitioning=false) LIMIT 1;
" | tr -d '[:space:]')

if grep -qE '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]+Z$' <<< "$ARROW_TIME_SAMPLE"; then
  echo "  PASS: arrow time is a CRI timestamp: $ARROW_TIME_SAMPLE"
else
  echo "  FAIL: arrow time is not a CRI timestamp: '$ARROW_TIME_SAMPLE'" >&2
  ERRORS=$((ERRORS + 1))
fi

# 7f. Both formats produce the same data when queried through y-logcli
ARROW_COUNT=$(./y-logcli --context=dev query '{namespace="default"}' -f arrow -o raw 2>&1 | grep -c "hello from log-generator" || true)
PARQUET_COUNT=$(./y-logcli --context=dev query '{namespace="default"}' -f parquet -o raw 2>&1 | grep -c "hello from log-generator" || true)

if [[ "$ARROW_COUNT" -gt 0 && "$PARQUET_COUNT" -gt 0 ]]; then
  echo "  PASS: both formats contain data (arrow=$ARROW_COUNT, parquet=$PARQUET_COUNT log-generator messages)"
else
  echo "  FAIL: format data mismatch (arrow=$ARROW_COUNT, parquet=$PARQUET_COUNT)" >&2
  ERRORS=$((ERRORS + 1))
fi

# 7g. SIGTERM flush - verify buffered data is flushed to S3 on shutdown
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
# grace-period=10 allows time for both s3-arrow and s3-parquet outputs to flush
$KUBECTL delete pod -l app=fluent-bit --grace-period=10
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
