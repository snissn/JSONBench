#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

DATA_DIR="${DATA_DIR:-$HOME/data/bluesky}"
ROWS="${ROWS:-1000000}"
TRIES="${TRIES:-3}"
QUERY_CELLS="${QUERY_CELLS:-q1 q2 q3 q4 q5}"
if [[ -z "${STORAGE_LAYOUTS+x}" ]]; then
  STORAGE_LAYOUTS="column-store column-store-prepared"
  RUN_DEFAULT_METADATA_LAYOUT=1
else
  RUN_DEFAULT_METADATA_LAYOUT=0
fi
METADATA_QUERY_CELLS="${METADATA_QUERY_CELLS:-q4 q5}"
OUT_DIR="${OUT_DIR:-/tmp/jsonbench_treedb_columnstore_$(date -u +%Y%m%d_%H%M%S)}"
GOMAP_REPLACE="${GOMAP_REPLACE:-}"

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
    tries:     $TRIES
    layouts:   $STORAGE_LAYOUTS
    queries:   $QUERY_CELLS
    metadata:  $([[ "$RUN_DEFAULT_METADATA_LAYOUT" == "1" ]] && echo "column-store-prepared-metadata ($METADATA_QUERY_CELLS)" || echo "custom STORAGE_LAYOUTS")
    out:       $OUT_DIR
EOF

run_matrix_cell() {
  local layouts="$1"
  local queries="$2"
  DATA_DIR="$DATA_DIR" \
  OUT_DIR="$OUT_DIR" \
  SCALES="subset" \
  SUBSET_ROWS="$ROWS" \
  FORMATS="json" \
  STORAGE_LAYOUTS="$layouts" \
  SUITE="minimal" \
  QUERY_CELLS="$queries" \
  TRIES="$TRIES" \
  PROFILE="${PROFILE:-fast}" \
  DATA_ROOT="${DATA_ROOT:-fast}" \
  BATCH_SIZE="${BATCH_SIZE:-16000}" \
  DUCKDB_RESULTS_DIR="${DUCKDB_RESULTS_DIR:-}" \
  CLICKHOUSE_RESULTS_DIR="${CLICKHOUSE_RESULTS_DIR:-}" \
  ./run_matrix.sh
}

# run_matrix.sh regenerates reports from all result.json files already present
# in OUT_DIR. Calling it twice is intentional: default metadata rows are a
# focused q4/q5 pass appended beside the direct and prepared-scan cells.
run_matrix_cell "$STORAGE_LAYOUTS" "$QUERY_CELLS"
if [[ "$RUN_DEFAULT_METADATA_LAYOUT" == "1" && -n "${METADATA_QUERY_CELLS// }" ]]; then
  run_matrix_cell "column-store-prepared-metadata" "$METADATA_QUERY_CELLS"
fi

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
