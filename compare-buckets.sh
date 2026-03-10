#!/bin/bash
set -eo pipefail

if [ $# -lt 1 ]; then
  echo "Usage: $0 <file.json> [file.json...]"
  echo "Compares GCS bucket listings exported via: gcloud storage ls --json 'gs://bucket/**' > file.json"
  exit 1
fi

for f in "$@"; do
  abs=$(cd "$(dirname "$f")" && pwd)/$(basename "$f")

  if [ ! -s "$abs" ]; then
    echo "-- ${f}: empty file (no objects found)"
    echo ""
    continue
  fi

  duckdb -markdown -c "
SELECT
  '${f}' as source,
  count(*) as objects,
  sum(metadata.size::BIGINT) as total_bytes,
  printf('%,.1f MiB', sum(metadata.size::BIGINT) / 1048576.0) as total_size,
  max(metadata.timeCreated::TIMESTAMPTZ) as newest,
  age(now(), max(metadata.timeCreated::TIMESTAMPTZ)) as newest_age,
  min(metadata.timeCreated::TIMESTAMPTZ) as oldest
FROM read_json_auto('${abs}');
"
done
