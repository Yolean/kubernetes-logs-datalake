#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME="fluentbit-demo"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
K3D_DIR="$SCRIPT_DIR/k3d-example"
KUBECONFIG="$K3D_DIR/kubeconfig"
KUBECTL="kubectl --kubeconfig=$KUBECONFIG"

# DuckDB/nanoarrow cannot read dictionary-encoded Arrow IPC files
# (https://github.com/paleolimbot/duckdb-nanoarrow/issues/25).
# Set to "fail" to treat DuckDB arrow failures as errors.
ON_DUCKDB_ARROW_FAILURE="${ON_DUCKDB_ARROW_FAILURE:-continue}"

# --- Helpers ---

fail() {
  echo "FAIL: $1" >&2
  echo "--- fluent-bit logs ---" >&2
  $KUBECTL logs -l app=fluent-bit --tail=50 2>/dev/null || true
  exit 1
}

duckdb_arrow_assert() {
  local msg="$1"
  if [[ "$ON_DUCKDB_ARROW_FAILURE" == "fail" ]]; then
    echo "  FAIL: $msg (DuckDB nanoarrow)" >&2
    ERRORS=$((ERRORS + 1))
  else
    echo "  SKIP: $msg (DuckDB nanoarrow lacks dictionary support)"
  fi
}

wait_for_rollout() {
  local kind="$1" name="$2" timeout="${3:-120s}"
  echo "  Waiting for $kind/$name..."
  $KUBECTL rollout status "$kind/$name" --timeout="$timeout" || fail "$kind/$name rollout timed out"
}

S3_SETUP_SQL="INSTALL httpfs; LOAD httpfs;
INSTALL nanoarrow FROM community; LOAD nanoarrow;
SET s3_region='us-east-1'; SET s3_endpoint='localhost:30070';
SET s3_access_key_id='demoaccess'; SET s3_secret_access_key='demosecret';
SET s3_use_ssl=false; SET s3_url_style='path';"

duckdb_s3() {
  duckdb -noheader -csv -c "${S3_SETUP_SQL} $1"
}

duckdb_s3_show() {
  duckdb -c "${S3_SETUP_SQL} $1"
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
  "$SCRIPT_DIR/images/versitygw/target/images/versitygw.tar" \
  "$SCRIPT_DIR/images/arrow-tools/target/images/arrow-tools.tar"

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

echo "==> Probing arrow IPC via DuckDB nanoarrow (may fail with dictionary-encoded columns)..."
ARROW_OUTPUT=$(poll_for_format arrow 2>&1) || {
  echo "  DuckDB nanoarrow cannot read dictionary-encoded Arrow IPC (ON_DUCKDB_ARROW_FAILURE=$ON_DUCKDB_ARROW_FAILURE)"
  ARROW_OUTPUT=""
}

# --- 6b. Print raw file metadata for one sample of each format ---

print_arrow_metadata() {
  local glob="$1"
  local sample
  sample=$(duckdb_s3 "SELECT file FROM glob('${glob}') ORDER BY file DESC LIMIT 1;")
  echo "  --- arrow IPC (.arrow): $(basename "$sample") [DuckDB nanoarrow] ---"
  echo "  Schema (DuckDB DESCRIBE read_arrow):"
  duckdb_s3_show "
    DESCRIBE SELECT * FROM read_arrow('${sample}');
  "
}

print_parquet_metadata() {
  local glob="$1"
  local sample
  sample=$(duckdb_s3 "SELECT DISTINCT file_name FROM parquet_schema('${glob}') LIMIT 1;")
  echo "  --- parquet (.parquet): $(basename "$sample") ---"
  echo "  Schema (parquet logical types):"
  duckdb_s3_show "
    SELECT name, type, logical_type
    FROM parquet_schema('${sample}')
    WHERE name <> 'schema';
  "
  echo "  Row group metadata (encodings, sizes):"
  duckdb_s3_show "
    SELECT path_in_schema AS col, encodings, compression,
           total_compressed_size AS comp_bytes, total_uncompressed_size AS raw_bytes
    FROM parquet_metadata('${sample}');
  "
}

echo "==> File metadata (one sample per format)..."
print_arrow_metadata "s3://fluentbit-logs/dev/default/**/*.arrow" || {
  echo "  (DuckDB nanoarrow read_arrow failed — expected with dictionary-encoded Arrow IPC)"
}
print_parquet_metadata "s3://fluentbit-logs/dev/default/**/*.parquet"

echo "==> Validating arrow IPC via pyarrow (official Apache Arrow library)..."
# Find the log-generator's earliest arrow file (contains the burst with close ns timestamps)
LOG_GEN_DIR=$(duckdb_s3 "
  SELECT regexp_replace(file, '/[^/]+$', '')
  FROM glob('s3://fluentbit-logs/dev/default/**/app/**/*.arrow')
  ORDER BY file LIMIT 1;
" | sed 's|s3://fluentbit-logs/||')
echo "  log-generator prefix: $LOG_GEN_DIR"

PYARROW_OUTPUT=$($KUBECTL run arrow-inspect --rm -i --restart=Never \
  --image=yolean/arrow-tools:latest --image-pull-policy=Never \
  --command -- python /usr/local/bin/inspect_arrow.py \
  fluentbit-logs "$LOG_GEN_DIR" .arrow \
  2>/dev/null) || true
echo "$PYARROW_OUTPUT"

echo "==> Running assertions..."

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

if [[ -n "$ARROW_OUTPUT" ]]; then
  if grep -q "hello from log-generator" <<< "$ARROW_OUTPUT"; then
    echo "  PASS: log-generator messages found in arrow (DuckDB nanoarrow)"
  else
    duckdb_arrow_assert "log-generator messages not found in arrow"
  fi
else
  duckdb_arrow_assert "arrow data not readable via DuckDB nanoarrow"
fi

# 7b. Partition columns present
OUTPUT=$(./y-logcli --context=dev query '{namespace="default"}' -f parquet 2>&1)

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
LINES_OUTPUT=$(./y-logcli --context=dev query '{namespace="default"}' -f parquet -o lines 2>&1)
if grep -q "cluster.*=.*dev" <<< "$LINES_OUTPUT"; then
  echo "  PASS: cluster tag 'dev' present in records"
else
  echo "  FAIL: cluster tag 'dev' not found in records" >&2
  ERRORS=$((ERRORS + 1))
fi

# 7d. Schema comparison — both formats should be Timestamp(ns) without timezone
echo "==> Checking schemas..."

# DuckDB nanoarrow read_arrow — may fail with dictionary-encoded columns
ARROW_TIME_TYPE=$(duckdb_s3 "
  SELECT column_type FROM (
    DESCRIBE SELECT * FROM read_arrow('s3://fluentbit-logs/dev/default/**/*.arrow', filename=true)
  ) WHERE column_name='time';
" 2>/dev/null | tr -d '[:space:]') || true

PARQUET_TIME_TYPE=$(duckdb_s3 "
  SELECT column_type FROM (
    DESCRIBE SELECT * FROM read_parquet('s3://fluentbit-logs/dev/default/**/*.parquet', filename=true, hive_partitioning=false)
  ) WHERE column_name='time';
" | tr -d '[:space:]')

echo "  --- DuckDB default interpretation of persisted formats ---"
echo "  Parquet (DESCRIBE read_parquet):"
duckdb_s3_show "
  DESCRIBE SELECT * FROM read_parquet('s3://fluentbit-logs/dev/default/**/*.parquet', filename=true, hive_partitioning=false);
"
echo "  Arrow IPC (DESCRIBE read_arrow):"
duckdb_s3_show "
  DESCRIBE SELECT * FROM read_arrow('s3://fluentbit-logs/dev/default/**/*.arrow', filename=true);
" 2>&1 || echo "  (DuckDB nanoarrow failed — expected with dictionary-encoded Arrow IPC)"

if [[ -n "$ARROW_TIME_TYPE" && "$ARROW_TIME_TYPE" == "TIMESTAMP_NS" ]]; then
  echo "  PASS: arrow format has time as TIMESTAMP_NS (DuckDB nanoarrow)"
elif [[ -z "$ARROW_TIME_TYPE" ]]; then
  duckdb_arrow_assert "arrow schema not readable via DuckDB nanoarrow"
else
  echo "  FAIL: arrow format has time as '$ARROW_TIME_TYPE', expected TIMESTAMP_NS" >&2
  ERRORS=$((ERRORS + 1))
fi

# Both formats use Timestamp(ns) without timezone — DuckDB reads as TIMESTAMP_NS
if [[ "$PARQUET_TIME_TYPE" == "TIMESTAMP_NS" ]]; then
  echo "  PASS: parquet format has time as TIMESTAMP_NS (nanosecond precision)"
else
  echo "  FAIL: parquet format has time as '$PARQUET_TIME_TYPE', expected TIMESTAMP_NS" >&2
  ERRORS=$((ERRORS + 1))
fi

# 7e. pyarrow independent validation — confirms Arrow IPC is readable by official Apache Arrow
if grep -q "timestamp\[ns\]" <<< "$PYARROW_OUTPUT"; then
  echo "  PASS: pyarrow confirms time field is timestamp[ns]"
else
  echo "  FAIL: pyarrow did not find timestamp[ns] type in Arrow IPC" >&2
  ERRORS=$((ERRORS + 1))
fi

if grep -q "dictionary<values=string, indices=int8" <<< "$PYARROW_OUTPUT"; then
  echo "  PASS: pyarrow confirms dictionary<values=string, indices=int8> fields"
else
  echo "  FAIL: pyarrow did not find dictionary<values=string, indices=int8> in Arrow IPC" >&2
  ERRORS=$((ERRORS + 1))
fi

# 7f. Timestamp values are valid (parseable by DuckDB as timestamps)
ARROW_TIME_SAMPLE=$(duckdb_s3 "
  SELECT time::VARCHAR FROM read_arrow('s3://fluentbit-logs/dev/default/**/*.arrow', filename=true) LIMIT 1;
" 2>/dev/null | tr -d '[:space:]') || true

if [[ -n "$ARROW_TIME_SAMPLE" ]] && grep -qE '^[0-9]{4}-[0-9]{2}-[0-9]{2}' <<< "$ARROW_TIME_SAMPLE"; then
  echo "  PASS: arrow time is a valid timestamp: $ARROW_TIME_SAMPLE (DuckDB nanoarrow)"
elif [[ -z "$ARROW_TIME_SAMPLE" ]]; then
  duckdb_arrow_assert "arrow timestamp not readable via DuckDB nanoarrow"
else
  echo "  FAIL: arrow time is not a valid timestamp: '$ARROW_TIME_SAMPLE'" >&2
  ERRORS=$((ERRORS + 1))
fi

# 7g. Parquet data queryable through y-logcli
PARQUET_COUNT=$(./y-logcli --context=dev query '{namespace="default"}' -f parquet -o raw 2>&1 | grep -c "hello from log-generator" || true)

if [[ "$PARQUET_COUNT" -gt 0 ]]; then
  echo "  PASS: parquet contains data ($PARQUET_COUNT log-generator messages)"
else
  echo "  FAIL: no parquet data found via y-logcli" >&2
  ERRORS=$((ERRORS + 1))
fi

# 7h. SIGTERM flush - verify buffered data is flushed to S3 on shutdown
echo "==> Testing SIGTERM flush..."
MARKER="sigterm-test-$(date +%s)"

# Emit a unique marker log
$KUBECTL run "$MARKER" --rm -i --restart=Never \
  --image=busybox:1.37 \
  --command -- echo "$MARKER" \
  2>/dev/null

# Wait for fluent-bit tail to pick it up.
# Needs: CRI log flush + Refresh_Interval (5s) + read cycle.
# CI runners are slower, so use a generous wait.
sleep 15

# Kill fluent-bit before upload_timeout (15s) triggers a regular flush.
# grace-period gives time for SIGTERM handler to flush both s3-arrow and s3-parquet.
$KUBECTL delete pod -l app=fluent-bit --grace-period=15
wait_for_rollout daemonset fluent-bit 60s

# Poll for the marker in S3 (up to 60s — CI runners may be slow)
FLUSH_TIMEOUT=60
FLUSH_INTERVAL=5
flush_elapsed=0
FLUSH_FOUND=false

while [ "$flush_elapsed" -lt "$FLUSH_TIMEOUT" ]; do
  FLUSH_OUTPUT=$(./y-logcli --context=dev query '{namespace="default"}' -f parquet -o raw 2>&1) || true
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
  echo "PASS: All assertions passed"
  exit 0
fi
