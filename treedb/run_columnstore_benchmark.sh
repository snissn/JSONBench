#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

DATA_DIR="${DATA_DIR:-$HOME/data/bluesky}"
ROWS="${ROWS:-1000000}"
TRIES="${TRIES:-3}"
QUERY_CELLS="${QUERY_CELLS:-q1 q2 q3 q4 q5}"
STORAGE_LAYOUTS="${STORAGE_LAYOUTS:-column-store column-store-prepared-metadata}"
OUT_DIR="${OUT_DIR:-/tmp/jsonbench_treedb_columnstore_$(date -u +%Y%m%d_%H%M%S)}"
GOMAP_REPLACE="${GOMAP_REPLACE:-}"
SCALE="${SCALE:-}"

scale_for_rows() {
  case "$1" in
    1000000) echo "1m" ;;
    10000000) echo "10m" ;;
    100000000) echo "100m" ;;
    1000000000) echo "1000m" ;;
    *) echo "subset" ;;
  esac
}

if [[ -z "$SCALE" ]]; then
  SCALE="$(scale_for_rows "$ROWS")"
fi

backup_dir=""
restore_go_mod() {
  if [[ -n "$backup_dir" ]]; then
    cp "$backup_dir/go.mod" go.mod
    cp "$backup_dir/go.sum" go.sum
    rm -rf "$backup_dir"
  fi
}
trap restore_go_mod EXIT

if [[ -n "$GOMAP_REPLACE" ]]; then
  backup_dir="$(mktemp -d /tmp/jsonbench_treedb_gomod_XXXXXX)"
  cp go.mod "$backup_dir/go.mod"
  cp go.sum "$backup_dir/go.sum"
  go mod edit -replace=github.com/snissn/gomap="$GOMAP_REPLACE"
fi

jsonbench_commit="$(git -C "$ROOT_DIR/.." rev-parse --short HEAD 2>/dev/null || echo unknown)"
gomap_module="$(go list -m -f '{{if .Replace}}{{.Replace.Path}}{{else}}{{.Version}}{{end}}' github.com/snissn/gomap)"

cat <<EOF
==> TreeDB column-store JSONBench default rerun
    JSONBench: $jsonbench_commit
    gomap:     $gomap_module
    data:      $DATA_DIR
    rows:      $ROWS
    scale:     $SCALE
    tries:     $TRIES
    layouts:   $STORAGE_LAYOUTS
    queries:   $QUERY_CELLS
    out:       $OUT_DIR
EOF

DATA_DIR="$DATA_DIR" \
OUT_DIR="$OUT_DIR" \
SCALES="$SCALE" \
SUBSET_ROWS="$ROWS" \
FORMATS="json" \
STORAGE_LAYOUTS="$STORAGE_LAYOUTS" \
SUITE="minimal" \
QUERY_CELLS="$QUERY_CELLS" \
TRIES="$TRIES" \
PROFILE="${PROFILE:-fast}" \
DATA_ROOT="${DATA_ROOT:-fast}" \
BATCH_SIZE="${BATCH_SIZE:-16000}" \
DUCKDB_RESULTS_DIR="${DUCKDB_RESULTS_DIR:-}" \
CLICKHOUSE_RESULTS_DIR="${CLICKHOUSE_RESULTS_DIR:-}" \
./run_matrix.sh

summary="$OUT_DIR/columnstore_summary.md"
{
  echo "# TreeDB column-store JSONBench default summary"
  echo
  echo "- JSONBench commit: \`$jsonbench_commit\`"
  echo "- gomap: \`$gomap_module\`"
  echo "- rows: \`$ROWS\`"
  echo "- tries: \`$TRIES\`"
  echo "- report: \`$OUT_DIR/report.md\`"
  echo
  go run ./cmd/jsonbench_treedb column-summary -report-json "$OUT_DIR/report.json"
} > "$summary"

cat "$summary"
echo
echo "$summary"
