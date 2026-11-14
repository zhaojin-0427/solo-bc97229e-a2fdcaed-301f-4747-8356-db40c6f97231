#!/usr/bin/env bash
# Happy-path demo against a running server: create a batch, upload both ends,
# query, download the originals and byte-compare them.
set -euo pipefail

BASE="${BASE:-http://localhost:8000}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

echo "== generate sample pair =="
python3 "$HERE/make_sample.py" "$WORK" --reads 1000

echo "== create batch =="
BATCH=$(curl -fsS -X POST "$BASE/batches" | jq -r .batch_id)
echo "batch_id=$BATCH"

echo "== upload R1 (order is free; R2 could go first) =="
curl -fsS -X POST --data-binary @"$WORK/sample_R1.fastq.gz" \
  -H 'Content-Type: application/gzip' "$BASE/batches/$BATCH/ends/R1/upload" | jq .

echo "== upload R2 =="
curl -fsS -X POST --data-binary @"$WORK/sample_R2.fastq.gz" \
  -H 'Content-Type: application/gzip' "$BASE/batches/$BATCH/ends/R2/upload" | jq .

echo "== query batch =="
curl -fsS "$BASE/batches/$BATCH" | jq .

echo "== download both originals and byte-compare =="
curl -fsS -o "$WORK/dl_R1.fastq.gz" "$BASE/batches/$BATCH/ends/R1/download"
curl -fsS -o "$WORK/dl_R2.fastq.gz" "$BASE/batches/$BATCH/ends/R2/download"
cmp "$WORK/sample_R1.fastq.gz" "$WORK/dl_R1.fastq.gz" && echo "R1 original intact"
cmp "$WORK/sample_R2.fastq.gz" "$WORK/dl_R2.fastq.gz" && echo "R2 original intact"
echo "DEMO OK"
