#!/usr/bin/env bash
set -euo pipefail

DATA_DIR="${DATA_DIR:-$HOME/data/bluesky}"
OUT_DIR="${OUT_DIR:-results/run_$(date -u +%Y%m%d_%H%M%S)_$$}"
SCALES="${SCALES:-subset}"
FORMATS="${FORMATS:-json}"
STORAGE_LAYOUTS="${STORAGE_LAYOUTS:-row}"
SUITE="${SUITE:-minimal}"
QUERY_CELLS="${QUERY_CELLS:-q1 q2 q3 q4 q4a q4b q5}"
BATCH_SIZE="${BATCH_SIZE:-16000}"
TRIES="${TRIES:-3}"
PROFILE="${PROFILE:-fast}"
DATA_ROOT="${DATA_ROOT:-fast}"
SUBSET_ROWS="${SUBSET_ROWS:-10000}"
DUCKDB_RESULTS_DIR="${DUCKDB_RESULTS_DIR:-}"
DUCKDB_SCALES="${DUCKDB_SCALES:-1m,10m}"
CLICKHOUSE_RESULTS_DIR="${CLICKHOUSE_RESULTS_DIR:-}"
CLICKHOUSE_SCALES="${CLICKHOUSE_SCALES:-1m,10m}"
COMPACT_AFTER_LOAD="${COMPACT_AFTER_LOAD:-0}"
VALIDATE_RECONSTRUCTION="${VALIDATE_RECONSTRUCTION:-0}"

usage() {
  cat <<'EOF'
Usage: ./run_matrix.sh [flags]

Environment:
  DATA_DIR            Input directory with JSONBench .json.gz or .json files.
  OUT_DIR             Output directory. Defaults to results/run_<timestamp>_<pid>.
  SCALES              Space-separated scales. Defaults to "subset".
  FORMATS             Space-separated formats. Defaults to "json".
  STORAGE_LAYOUTS     Space-separated TreeDB storage layouts: row, column-store,
                      column-store-prepared, column-store-prepared-metadata,
                      column-store-full, or column-store-full-prepared.
                      Defaults to "row".
  SUITE               minimal, full, or all. Defaults to "minimal".
  QUERY_CELLS         Query-specific minimal cells for SUITE=minimal/all.
                      Defaults to "q1 q2 q3 q4 q4a q4b q5".
  SUBSET_ROWS         Rows for subset scale. Defaults to 10000.
  TRIES               Query attempts per cell. Defaults to 3.
  DUCKDB_RESULTS_DIR  DuckDB result JSON directory for report import.
                      Defaults to empty so reports only include this run.
  DUCKDB_SCALES       DuckDB scales to import, comma-separated or all.
                      Defaults to 1m,10m.
  CLICKHOUSE_RESULTS_DIR
                      ClickHouse result JSON directory for report import.
                      Defaults to empty.
  CLICKHOUSE_SCALES   ClickHouse scales to import, comma-separated or all.
                      Defaults to 1m,10m.
  COMPACT_AFTER_LOAD  Set to 1/true/yes/on to run full TreeDB compaction after
                      loading and before queries. Defaults to 0.
  VALIDATE_RECONSTRUCTION
                      Set to 1/true/yes/on to validate full-data column-store
                      reconstruction for column-store-full layouts. Defaults to 0.
Flags:
  -h, --help          Show this help.

The matrix runner never accepts partial data. If DATA_DIR has fewer rows than
requested, the run fails. Use SUBSET_ROWS=6 only for the checked-in smoke
fixture, or DATA_DIR="$HOME/data/bluesky" for downloaded JSONBench data.

The default suite is intentionally strict minimal JSON: q1..q5 each load only
the fields needed for that query. Full-document cells are available with
SUITE=full or SUITE=all. The collection harness opens TreeDB with the cached
leaf-log backend so full-document data roots can store oversized documents
through persistent value-log pointers.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -allow-short-data|--allow-short-data)
      echo "-allow-short-data has been removed; this benchmark must load exactly the requested rows." >&2
      echo "Use SUBSET_ROWS=6 for ./testdata/bluesky, or DATA_DIR=\"$HOME/data/bluesky\" for real data." >&2
      exit 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

mkdir -p "$OUT_DIR"

case "$SUITE" in
  minimal|full|all)
    ;;
  *)
    echo "unknown SUITE: $SUITE (expected minimal, full, or all)" >&2
    exit 2
    ;;
esac

run_cell() {
  local scale="$1"
  local format="$2"
  local storage_layout="$3"
  local projection="$4"
  local queries="$5"
  local rows_arg=()
  local cell_scale="$scale"
  local row_label="$scale"
  if [[ "$scale" == "subset" ]]; then
    cell_scale="rows${SUBSET_ROWS}"
    row_label="${SUBSET_ROWS} rows requested"
    rows_arg=(-rows "$SUBSET_ROWS")
  fi

  local cell="${cell_scale}_${storage_layout}_${format}_${projection}_${queries//,/_}"
  local compact_arg=()
  local compact_suffix=""
  local validate_arg=()
  case "$COMPACT_AFTER_LOAD" in
    1|true|TRUE|yes|YES|on|ON)
      compact_arg=(-compact-after-load)
      compact_suffix="_compacted"
      ;;
    0|false|FALSE|no|NO|off|OFF|"")
      ;;
    *)
      echo "invalid COMPACT_AFTER_LOAD=$COMPACT_AFTER_LOAD (expected 0/1)" >&2
      exit 2
      ;;
  esac
  case "$VALIDATE_RECONSTRUCTION" in
    1|true|TRUE|yes|YES|on|ON)
      case "$storage_layout" in
        column-store-full|column-store-full-prepared)
          validate_arg=(-validate-reconstruction)
          ;;
      esac
      ;;
    0|false|FALSE|no|NO|off|OFF|"")
      ;;
    *)
      echo "invalid VALIDATE_RECONSTRUCTION=$VALIDATE_RECONSTRUCTION (expected 0/1)" >&2
      exit 2
      ;;
  esac
  cell="${cell}${compact_suffix}"
  cell="${cell//\//_}"
  local cell_dir="$OUT_DIR/$cell"
  mkdir -p "$cell_dir"
  echo "==> $cell ($row_label)"
  local cmd=(
    go run ./cmd/jsonbench_treedb run
    -data-dir "$DATA_DIR" \
    -db-dir "$cell_dir/db" \
    -reset \
    -scale "$scale"
  )
  if [[ ${#rows_arg[@]} -gt 0 ]]; then
    cmd+=("${rows_arg[@]}")
  fi
  cmd+=(
    -format "$format" \
    -storage-layout "$storage_layout" \
    -projection "$projection" \
    -queries "$queries" \
    -batch-size "$BATCH_SIZE" \
    -tries "$TRIES" \
    -profile "$PROFILE" \
    -data-root "$DATA_ROOT"
  )
  if [[ ${#compact_arg[@]} -gt 0 ]]; then
    cmd+=("${compact_arg[@]}")
  fi
  if [[ ${#validate_arg[@]} -gt 0 ]]; then
    cmd+=("${validate_arg[@]}")
  fi
  cmd+=(
    -out "$cell_dir/result.json"
  )
  "${cmd[@]}"
}

for scale in $SCALES; do
  for format in $FORMATS; do
    for storage_layout in $STORAGE_LAYOUTS; do
      if [[ "$storage_layout" != "row" && "$format" != "json" ]]; then
        echo "skip format=$format for storage_layout=$storage_layout (column-store supports json only)" >&2
        continue
      fi
      if [[ "$SUITE" == "full" || "$SUITE" == "all" ]]; then
        case "$storage_layout" in
          row|column-store-full|column-store-full-prepared)
            run_cell "$scale" "$format" "$storage_layout" "full" "all"
            ;;
          *)
            echo "skip full suite for storage_layout=$storage_layout (use column-store-full or column-store-full-prepared for full-data column-store cells)" >&2
            ;;
        esac
      fi
      if [[ "$SUITE" == "minimal" || "$SUITE" == "all" ]]; then
        for query in $QUERY_CELLS; do
          run_cell "$scale" "$format" "$storage_layout" "$query" "$query"
        done
      fi
    done
  done
done

go run ./cmd/jsonbench_treedb report \
  -results-dir "$OUT_DIR" \
  -duckdb-results-dir "$DUCKDB_RESULTS_DIR" \
  -duckdb-scales "$DUCKDB_SCALES" \
  -clickhouse-results-dir "$CLICKHOUSE_RESULTS_DIR" \
  -clickhouse-scales "$CLICKHOUSE_SCALES" \
  -out "$OUT_DIR/report.md" \
  -json-out "$OUT_DIR/report.json"

echo "$OUT_DIR/report.md"
