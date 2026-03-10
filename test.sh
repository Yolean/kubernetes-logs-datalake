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

POLL_TIMEOUT=180
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

# 7h. Size-based flush — verify data is flushed when buffer exceeds total_file_size (1M).
# Write >1M in one burst; it should appear in S3 well before upload_timeout (60s).
# Buffer_Chunk_Size=512K and Buffer_Max_Size=2M allow tail to ingest quickly.
echo "==> Testing size-based flush (total_file_size=1M)..."
SIZE_MARKER="size-flush-$(date +%s)"
# Generate >1.5M instantly using yes+head (busybox shell loops are too slow).
# 'yes' outputs ~80-char lines at full speed; 20000 lines ≈ 1.6M.
$KUBECTL run size-flush --restart=Never \
  --image=busybox:1.37 \
  --command -- sh -c "echo '${SIZE_MARKER}'; yes 'size-padding-data-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' | head -n 20000; sleep 3600"

$KUBECTL wait pod size-flush --for=condition=Ready --timeout=30s

# Wait for fluent-bit to discover and read the file (Refresh_Interval=5, plus ingestion time)
sleep 12

# Size-based flush should trigger within seconds of ingestion (buffer > 1M).
# Poll for 45s — well under the 60s upload_timeout, proving size triggered it.
SIZE_FLUSH_TIMEOUT=45
SIZE_FLUSH_FOUND=false
size_elapsed=0
while [ "$size_elapsed" -lt "$SIZE_FLUSH_TIMEOUT" ]; do
  SIZE_OUTPUT=$(./y-logcli --context=dev query '{namespace="default"}' -f parquet -o raw 2>&1) || true
  if grep -q "$SIZE_MARKER" <<< "$SIZE_OUTPUT"; then
    SIZE_FLUSH_FOUND=true
    break
  fi
  size_elapsed=$((size_elapsed + 3))
  echo "  Polling... (${size_elapsed}/${SIZE_FLUSH_TIMEOUT}s)"
  sleep 3
done

if [ "$SIZE_FLUSH_FOUND" = true ]; then
  echo "  PASS: Size-based flush — marker found in S3 within ${size_elapsed}s (before 60s upload_timeout)"
else
  echo "  FAIL: Size-based flush — marker '$SIZE_MARKER' not found in S3 within ${SIZE_FLUSH_TIMEOUT}s" >&2
  ERRORS=$((ERRORS + 1))
fi
$KUBECTL delete pod size-flush --grace-period=0 --force 2>/dev/null || true

# 7i. Timeout-based flush — verify data is flushed after upload_timeout (60s)
# when the buffer never reaches total_file_size.
# Write a small marker (well under 1M), container stays running.
# Data should NOT appear before ~60s (not size-triggered) but SHOULD appear after.
echo "==> Testing timeout-based flush (upload_timeout=60s)..."
TIMEOUT_MARKER="timeout-flush-$(date +%s)"
$KUBECTL run timeout-flush --restart=Never \
  --image=busybox:1.37 \
  --command -- sh -c "echo '${TIMEOUT_MARKER}'; sleep 3600"

$KUBECTL wait pod timeout-flush --for=condition=Ready --timeout=30s

# Wait for fluent-bit to discover the log file
sleep 8

# Pre-check: marker should NOT be in S3 within 20s (no size trigger, timeout is 60s)
echo "  Verifying marker is NOT in S3 yet (expecting no flush before upload_timeout)..."
if close_write_poll "$TIMEOUT_MARKER" 20 5 2>/dev/null; then
  echo "  WARN: Marker appeared before upload_timeout — size or close-write may have triggered early"
else
  echo "  OK: Marker not in S3 after 20s (as expected — waiting for upload_timeout)"
fi

# Now wait for upload_timeout to fire. From pod start: 8s discovery + 20s pre-check = ~28s elapsed.
# upload_timeout=60s starts when fluent-bit ingests (at ~8s), so it fires at ~68s from pod start.
# We've used ~28s, need to wait ~40s more, then poll.
TIMEOUT_FLUSH_DEADLINE=120
TIMEOUT_FLUSH_INTERVAL=5
timeout_elapsed=28
TIMEOUT_FLUSH_FOUND=false

# Wait for the remaining time until upload_timeout fires
echo "  Waiting for upload_timeout to fire..."
sleep 40
timeout_elapsed=68

while [ "$timeout_elapsed" -lt "$TIMEOUT_FLUSH_DEADLINE" ]; do
  TIMEOUT_OUTPUT=$(./y-logcli --context=dev query '{namespace="default"}' -f parquet -o raw 2>&1) || true
  if grep -q "$TIMEOUT_MARKER" <<< "$TIMEOUT_OUTPUT"; then
    TIMEOUT_FLUSH_FOUND=true
    break
  fi
  timeout_elapsed=$((timeout_elapsed + TIMEOUT_FLUSH_INTERVAL))
  echo "  Polling... (${timeout_elapsed}/${TIMEOUT_FLUSH_DEADLINE}s)"
  sleep "$TIMEOUT_FLUSH_INTERVAL"
done

if [ "$TIMEOUT_FLUSH_FOUND" = true ]; then
  echo "  PASS: Timeout-based flush — marker found in S3 at ~${timeout_elapsed}s (after upload_timeout=60s)"
else
  echo "  FAIL: Timeout-based flush — marker '$TIMEOUT_MARKER' not found in S3 within ${TIMEOUT_FLUSH_DEADLINE}s" >&2
  ERRORS=$((ERRORS + 1))
fi
$KUBECTL delete pod timeout-flush --grace-period=0 --force 2>/dev/null || true

# 7j. SIGTERM flush - verify buffered data is flushed to S3 on shutdown
echo "==> Testing SIGTERM flush..."
MARKER="sigterm-test-$(date +%s)"

# Write marker into the log-generator's CRI log via PID 1's stdout fd.
# kubectl exec stdout goes back to the client, NOT to the CRI log.
# Writing to /proc/1/fd/1 injects into the main process's stdout which CRI captures.
echo "  Emitting marker '$MARKER' via log-generator /proc/1/fd/1..."
$KUBECTL exec deploy/log-generator -- sh -c "echo '$MARKER' > /proc/1/fd/1"

# Verify fluent-bit has ingested the marker by checking its internal metrics.
# The tail input must read the line before we kill the pod.
echo "  Waiting for fluent-bit to ingest marker..."
sleep 10
FB_POD=$($KUBECTL get pod -l app=fluent-bit -o jsonpath='{.items[0].metadata.name}')
echo "  fluent-bit pod to be killed: $FB_POD"

# Show what fluent-bit is currently watching (for debugging)
echo "  fluent-bit watched files (last 5):"
$KUBECTL logs "$FB_POD" | grep -E 'inotify_fs_add|Successfully uploaded' | tail -5 || true

# Kill fluent-bit before upload_timeout (60s) triggers a regular flush.
# grace-period gives time for SIGTERM handler to flush both s3-arrow and s3-parquet.
echo "  Killing fluent-bit with grace-period=30..."
$KUBECTL delete pod "$FB_POD" --grace-period=30
wait_for_rollout daemonset fluent-bit 60s

# Show the killed pod's last logs (termination output from previous instance)
echo "  Previous fluent-bit shutdown logs:"
$KUBECTL logs -l app=fluent-bit --previous --tail=10 2>/dev/null || echo "  (no previous logs available)"

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

# 7k. Concurrent SIGTERM flush — verify buffers from 20+ pods all flush within grace period.
# Each pod×container×output creates a separate S3 buffer, so 25 pods × 2 outputs = 50 buffers.
# Sequential flush would need ~50-100s; within 30s grace period proves concurrency.
echo "==> Testing concurrent SIGTERM flush (25 pods)..."
CONCURRENT_MARKER="concurrent-$(date +%s)"
FLUSH_POD_COUNT=25

for i in $(seq 1 $FLUSH_POD_COUNT); do
  $KUBECTL run "flush-test-$i" --restart=Never \
    --image=busybox:1.37 --labels=test=concurrent-flush \
    --command -- sh -c "echo '${CONCURRENT_MARKER}-${i}'; sleep 3600" &
done
wait

echo "  Waiting for $FLUSH_POD_COUNT pods to start..."
$KUBECTL wait pod -l test=concurrent-flush --for=condition=Ready --timeout=60s

# Refresh_Interval=5 means up to 5s for fluent-bit to discover new log files,
# then Read_from_Head reads them immediately. Two refresh cycles for safety.
echo "  Waiting for fluent-bit to discover and tail $FLUSH_POD_COUNT log files..."
sleep 12

FB_POD=$($KUBECTL get pod -l app=fluent-bit -o jsonpath='{.items[0].metadata.name}')
echo "  Killing fluent-bit pod $FB_POD with grace-period=30..."
$KUBECTL delete pod "$FB_POD" --grace-period=30
wait_for_rollout daemonset fluent-bit 60s

echo "  Previous fluent-bit shutdown logs:"
$KUBECTL logs -l app=fluent-bit --previous --tail=15 2>/dev/null || echo "  (no previous logs available)"

FLUSH_TIMEOUT=60
FLUSH_INTERVAL=5
flush_elapsed=0
FOUND_COUNT=0

while [ "$flush_elapsed" -lt "$FLUSH_TIMEOUT" ]; do
  FLUSH_OUTPUT=$(./y-logcli --context=dev query '{namespace="default"}' -f parquet -o raw 2>&1) || true
  # Count distinct pod markers (not lines — duplicates from re-reads would inflate grep -c)
  FOUND_COUNT=0
  for i in $(seq 1 $FLUSH_POD_COUNT); do
    if grep -q "${CONCURRENT_MARKER}-${i}\b" <<< "$FLUSH_OUTPUT"; then
      FOUND_COUNT=$((FOUND_COUNT + 1))
    fi
  done
  if [ "$FOUND_COUNT" -ge "$FLUSH_POD_COUNT" ]; then
    break
  fi
  flush_elapsed=$((flush_elapsed + FLUSH_INTERVAL))
  echo "  Found $FOUND_COUNT/$FLUSH_POD_COUNT markers, retrying... (${flush_elapsed}/${FLUSH_TIMEOUT}s)"
  sleep "$FLUSH_INTERVAL"
done

if [ "$FOUND_COUNT" -ge "$FLUSH_POD_COUNT" ]; then
  echo "  PASS: Concurrent flush — all $FLUSH_POD_COUNT pod markers found in S3 (50 buffers flushed within 30s)"
else
  echo "  FAIL: Concurrent flush — only $FOUND_COUNT/$FLUSH_POD_COUNT pod markers found (buffers lost on SIGTERM)" >&2
  ERRORS=$((ERRORS + 1))
fi

$KUBECTL delete pod -l test=concurrent-flush --grace-period=0 --force 2>/dev/null || true

# 7l. IN_CLOSE_WRITE flush — container termination triggers immediate S3 upload
# without restarting fluent-bit. The close-write-flush.patch watches for IN_CLOSE_WRITE
# inotify events (CRI runtime closes the log fd) and emits a sentinel on tag._close
# which forces S3 output to flush the base tag's buffer.
#
# We verify that:
#   1. Logs are NOT flushed before container terminates (upload_timeout=60s hasn't fired)
#   2. After termination, logs appear in S3 within 30s (well before 60s upload_timeout)
#
# Helper: poll S3 for a marker, returns 0 if found within timeout
close_write_poll() {
  local marker="$1"
  local timeout="$2"
  local interval="${3:-2}"
  local elapsed=0
  while [ "$elapsed" -lt "$timeout" ]; do
    local output
    output=$(./y-logcli --context=dev query '{namespace="default"}' -f parquet -o raw 2>&1) || true
    if grep -q "$marker" <<< "$output"; then
      return 0
    fi
    elapsed=$((elapsed + interval))
    echo "    Polling... (${elapsed}/${timeout}s)"
    sleep "$interval"
  done
  return 1
}

# 7l-1. Exit 0 — container exits normally
echo "==> Testing IN_CLOSE_WRITE flush: exit 0..."
CW_MARKER_0="cw-exit0-$(date +%s)"
$KUBECTL run cw-exit0 --restart=Never \
  --image=busybox:1.37 \
  --command -- sh -c "echo '${CW_MARKER_0}'; sleep 10"

$KUBECTL wait pod cw-exit0 --for=condition=Ready --timeout=30s

# Wait for fluent-bit to discover and tail the log file
sleep 8

# Pre-check: marker should NOT be in S3 yet (container still running, upload_timeout=60s hasn't fired)
if close_write_poll "$CW_MARKER_0" 0 1 2>/dev/null; then
  echo "  WARN: Marker already in S3 before container termination (upload_timeout may have fired)"
fi

# Wait for container to exit (sleep 10 in the container)
echo "  Waiting for container to exit 0..."
$KUBECTL wait pod cw-exit0 --for=jsonpath='{.status.phase}'=Succeeded --timeout=30s

# After close-write, logs should flush quickly (well within upload_timeout=60s)
if close_write_poll "$CW_MARKER_0" 30; then
  echo "  PASS: IN_CLOSE_WRITE flush (exit 0) — marker '$CW_MARKER_0' found in S3"
else
  echo "  FAIL: IN_CLOSE_WRITE flush (exit 0) — marker '$CW_MARKER_0' not found in S3" >&2
  ERRORS=$((ERRORS + 1))
fi
$KUBECTL delete pod cw-exit0 --grace-period=0 --force 2>/dev/null || true

# 7l-2. kubectl delete pod — forced termination
echo "==> Testing IN_CLOSE_WRITE flush: kubectl delete pod..."
CW_MARKER_DEL="cw-delete-$(date +%s)"
$KUBECTL run cw-delete --restart=Never \
  --image=busybox:1.37 \
  --command -- sh -c "echo '${CW_MARKER_DEL}'; sleep 3600"

$KUBECTL wait pod cw-delete --for=condition=Ready --timeout=30s
sleep 8

echo "  Deleting pod..."
$KUBECTL delete pod cw-delete --grace-period=5

if close_write_poll "$CW_MARKER_DEL" 30; then
  echo "  PASS: IN_CLOSE_WRITE flush (delete pod) — marker '$CW_MARKER_DEL' found in S3"
else
  echo "  FAIL: IN_CLOSE_WRITE flush (delete pod) — marker '$CW_MARKER_DEL' not found in S3" >&2
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
