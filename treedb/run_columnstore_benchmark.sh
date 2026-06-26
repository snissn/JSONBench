#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

DATA_DIR="${DATA_DIR:-$HOME/data/bluesky}"
ROWS="${ROWS:-1000000}"
TRIES="${TRIES:-3}"
QUERY_MODE="${QUERY_MODE:-one_shot_end_to_end}"
METADATA_MODE="${METADATA_MODE:-auto_aggregate_metadata}"
QUERY_CELLS="${QUERY_CELLS:-q1 q2 q3 q4 q4a q4b q5 qexpr}"
SUITE="${SUITE:-minimal}"
VALIDATE_RECONSTRUCTION="${VALIDATE_RECONSTRUCTION:-0}"
TREEDB_ALLOW_ERRORS="${TREEDB_ALLOW_ERRORS:-0}"
if [[ -z "${STORAGE_LAYOUTS:-}" ]]; then
  STORAGE_LAYOUTS="column-store column-store-prepared"
  RUN_DEFAULT_METADATA_LAYOUT=1
else
  RUN_DEFAULT_METADATA_LAYOUT=0
fi

default_metadata_query_cells() {
  local out=""
  local query
  for query in $QUERY_CELLS; do
    case "$query" in
      q4|q4a|q4b|q5)
        out="${out:+$out }$query"
        ;;
    esac
  done
  printf '%s' "$out"
}

METADATA_QUERY_CELLS_EXPLICIT=1
if [[ -z "${METADATA_QUERY_CELLS+x}" ]]; then
  METADATA_QUERY_CELLS="$(default_metadata_query_cells)"
  METADATA_QUERY_CELLS_EXPLICIT=0
fi
OUT_DIR="${OUT_DIR:-/tmp/jsonbench_treedb_columnstore_$(date -u +%Y%m%d_%H%M%S)}"
GOMAP_REPLACE="${GOMAP_REPLACE:-}"
SCALE="${SCALE:-}"
RETAINED_PAYLOAD_ENCODING="${TREEDB_COLUMN_STORE_RETAINED_PAYLOAD_ENCODING:-}"

scale_for_rows() {
  case "$1" in
    1000000) echo "1m" ;;
    10000000) echo "10m" ;;
    100000000) echo "100m" ;;
    1000000000) echo "1000m" ;;
    *) echo "subset" ;;
  esac
}

rows_for_scale() {
  case "$1" in
    1m) echo "1000000" ;;
    10m) echo "10000000" ;;
    100m) echo "100000000" ;;
    1000m|1b) echo "1000000000" ;;
    *) echo "$ROWS" ;;
  esac
}

if [[ -z "$SCALE" ]]; then
  SCALE="$(scale_for_rows "$ROWS")"
fi
EFFECTIVE_ROWS="$(rows_for_scale "$SCALE")"
if [[ "$SCALE" != "subset" ]]; then
  ROWS="$EFFECTIVE_ROWS"
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
metadata_label="custom STORAGE_LAYOUTS"
if [[ "$RUN_DEFAULT_METADATA_LAYOUT" == "1" ]]; then
  if [[ -n "${METADATA_QUERY_CELLS// }" ]]; then
    metadata_label="column-store-prepared-metadata ($METADATA_QUERY_CELLS)"
  elif [[ "$METADATA_QUERY_CELLS_EXPLICIT" == "1" ]]; then
    metadata_label="none (METADATA_QUERY_CELLS is empty)"
  else
    metadata_label="none (QUERY_CELLS has no q4/q4a/q4b/q5 metadata cells)"
  fi
fi

cat <<EOF
==> TreeDB column-store JSONBench default rerun
    JSONBench: $jsonbench_commit
    gomap:     $gomap_module
    data:      $DATA_DIR
    rows:      $EFFECTIVE_ROWS
    scale:     $SCALE
    tries:     $TRIES
    query mode: $QUERY_MODE
    metadata mode: $METADATA_MODE
    suite:     $SUITE
    layouts:   $STORAGE_LAYOUTS
    queries:   $QUERY_CELLS
    validate:  $VALIDATE_RECONSTRUCTION
    allow errs: $TREEDB_ALLOW_ERRORS
    retained:  ${RETAINED_PAYLOAD_ENCODING:-default}
    metadata:  $metadata_label
    out:       $OUT_DIR
EOF

run_matrix_cell() {
  local layouts="$1"
  local queries="$2"
  DATA_DIR="$DATA_DIR" \
  OUT_DIR="$OUT_DIR" \
  SCALES="$SCALE" \
  SUBSET_ROWS="$ROWS" \
  FORMATS="json" \
  STORAGE_LAYOUTS="$layouts" \
  QUERY_MODE="$QUERY_MODE" \
  METADATA_MODE="$METADATA_MODE" \
  SUITE="$SUITE" \
  QUERY_CELLS="$queries" \
  VALIDATE_RECONSTRUCTION="$VALIDATE_RECONSTRUCTION" \
  ALLOW_ERRORS="$TREEDB_ALLOW_ERRORS" \
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
# focused q4/q4a/q4b/q5 pass appended beside the direct and prepared-scan cells while
# preserving the same scale/subset row selection as the primary pass.
run_matrix_cell "$STORAGE_LAYOUTS" "$QUERY_CELLS"
if [[ "$SUITE" != "full" && "$RUN_DEFAULT_METADATA_LAYOUT" == "1" && -n "${METADATA_QUERY_CELLS// }" ]]; then
  run_matrix_cell "column-store-prepared-metadata" "$METADATA_QUERY_CELLS"
fi

summary="$OUT_DIR/columnstore_summary.md"
{
  echo "# TreeDB column-store JSONBench default summary"
  echo
  echo "- JSONBench commit: \`$jsonbench_commit\`"
  echo "- gomap: \`$gomap_module\`"
  echo "- rows: \`$EFFECTIVE_ROWS\`"
  echo "- tries: \`$TRIES\`"
  echo "- query mode: \`$QUERY_MODE\`"
  echo "- metadata mode: \`$METADATA_MODE\`"
  echo "- suite: \`$SUITE\`"
  echo "- validate reconstruction: \`$VALIDATE_RECONSTRUCTION\`"
  echo "- allow errors: \`$TREEDB_ALLOW_ERRORS\`"
  echo "- retained payload encoding: \`${RETAINED_PAYLOAD_ENCODING:-default}\`"
  echo "- report: \`$OUT_DIR/report.md\`"
  echo
  go run ./cmd/jsonbench_treedb column-summary -report-json "$OUT_DIR/report.json"
} > "$summary"

cat "$summary"
echo
echo "$summary"
