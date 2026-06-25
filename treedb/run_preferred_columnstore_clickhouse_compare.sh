#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$ROOT_DIR/.." && pwd)"
cd "$ROOT_DIR"

DATA_DIR="${DATA_DIR:-$HOME/data/bluesky}"
ROWS="${ROWS:-10000000}"
TRIES="${TRIES:-3}"
OUT_DIR="${OUT_DIR:-/tmp/jsonbench_preferred_columnstore_clickhouse_$(date -u +%Y%m%d_%H%M%S)}"
GOMAP_REPLACE="${GOMAP_REPLACE:-}"
CLICKHOUSE_BIN="${CLICKHOUSE_BIN:-clickhouse}"
RUN_TREEDB="${RUN_TREEDB:-1}"
RUN_CLICKHOUSE="${RUN_CLICKHOUSE:-1}"
CLICKHOUSE_ALLOW_ERRORS="${CLICKHOUSE_ALLOW_ERRORS:-1}"
TREEDB_ALLOW_ERRORS="${TREEDB_ALLOW_ERRORS:-$CLICKHOUSE_ALLOW_ERRORS}"
CLICKHOUSE_MAX_FILES="${CLICKHOUSE_MAX_FILES:-}"
TREEDB_FULL_STORAGE_LAYOUTS="${TREEDB_FULL_STORAGE_LAYOUTS:-column-store-full-prepared}"
TREEDB_QUERY_STORAGE_LAYOUTS="${TREEDB_QUERY_STORAGE_LAYOUTS:-column-store-prepared-metadata}"
TREEDB_COMPACT_AFTER_LOAD="${TREEDB_COMPACT_AFTER_LOAD:-0}"
TREEDB_VALIDATE_RECONSTRUCTION="${TREEDB_VALIDATE_RECONSTRUCTION:-0}"
TREEDB_RESULT="${TREEDB_RESULT:-}"
CLICKHOUSE_RESULT="${CLICKHOUSE_RESULT:-}"

usage() {
  cat <<'EOF'
Usage: ./run_preferred_columnstore_clickhouse_compare.sh [flags]

Runs the preferred/server-shaped TreeDB column-store JSONBench rows and a local
ClickHouse JSON comparison, then writes preferred_summary.md.

Defaults reproduce the 10M experiment:

  DATA_DIR="$HOME/data/bluesky" ROWS=10000000 TRIES=3 \
    GOMAP_REPLACE=/path/to/gomap \
    ./run_preferred_columnstore_clickhouse_compare.sh

Environment:
  DATA_DIR                 JSONBench data directory. Defaults to ~/data/bluesky.
  ROWS                     Requested rows. Defaults to 10000000.
  TRIES                    Query attempts. Defaults to 3.
  OUT_DIR                  Output directory. Defaults to /tmp/jsonbench_preferred_*.
  GOMAP_REPLACE            Optional local gomap checkout for go mod replace.
  CLICKHOUSE_BIN           ClickHouse binary. Defaults to clickhouse.
  RUN_TREEDB               Set 0 to skip TreeDB when reusing report.json.
  RUN_CLICKHOUSE           Set 0 to skip ClickHouse when reusing result.json.
  CLICKHOUSE_ALLOW_ERRORS  Set 1 to match JSONBench fallback loading for rows
                           ClickHouse rejects as invalid JSON. Defaults to 1.
  TREEDB_ALLOW_ERRORS      Set 1 to skip malformed JSON rows before TreeDB
                           insertion. Defaults to CLICKHOUSE_ALLOW_ERRORS.
  CLICKHOUSE_MAX_FILES     Input file count for ClickHouse. Defaults to ROWS/1M.
  TREEDB_FULL_STORAGE_LAYOUTS
                           Full-data TreeDB layouts used for headline storage.
                           Defaults to column-store-full-prepared.
  TREEDB_QUERY_STORAGE_LAYOUTS
                           Query-shaped TreeDB layouts used for attribution.
                           Defaults to column-store-prepared-metadata.
  TREEDB_COMPACT_AFTER_LOAD
                           Set 1 to compact full-data TreeDB rows before
                           measurement. Defaults to 0.
  TREEDB_VALIDATE_RECONSTRUCTION
                           Set 1 to validate full-data TreeDB reconstruction
                           during the headline run. Defaults to 0.
  TREEDB_RESULT            Existing TreeDB report.json to summarize.
  CLICKHOUSE_RESULT        Existing ClickHouse result.json to summarize.

Outputs:
  treedb/report.md
  treedb/report.json
  clickhouse/result.json
  preferred_summary.md
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
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

require_positive_int() {
  local name="$1"
  local value="$2"
  if ! [[ "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "$name must be a positive integer, got: $value" >&2
    exit 2
  fi
}

bool_enabled() {
  case "$1" in
    1|true|TRUE|yes|YES|on|ON)
      return 0
      ;;
    0|false|FALSE|no|NO|off|OFF|"")
      return 1
      ;;
    *)
      echo "invalid boolean value: $1" >&2
      exit 2
      ;;
  esac
}

require_positive_int ROWS "$ROWS"
require_positive_int TRIES "$TRIES"
if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required to measure load time and write preferred_summary.md" >&2
  exit 1
fi

mkdir -p "$OUT_DIR"
TREE_OUT="$OUT_DIR/treedb"
CLICKHOUSE_OUT="$OUT_DIR/clickhouse"
SUMMARY_OUT="$OUT_DIR/preferred_summary.md"

run_treedb() {
  mkdir -p "$TREE_OUT"
  echo "==> TreeDB full-data storage headline"
  DATA_DIR="$DATA_DIR" \
  ROWS="$ROWS" \
  TRIES="$TRIES" \
  GOMAP_REPLACE="$GOMAP_REPLACE" \
  STORAGE_LAYOUTS="$TREEDB_FULL_STORAGE_LAYOUTS" \
  SUITE="full" \
  COMPACT_AFTER_LOAD="$TREEDB_COMPACT_AFTER_LOAD" \
  VALIDATE_RECONSTRUCTION="$TREEDB_VALIDATE_RECONSTRUCTION" \
  TREEDB_ALLOW_ERRORS="$TREEDB_ALLOW_ERRORS" \
  OUT_DIR="$TREE_OUT" \
  ./run_columnstore_benchmark.sh

  echo "==> TreeDB query-shaped attribution rows"
  DATA_DIR="$DATA_DIR" \
  ROWS="$ROWS" \
  TRIES="$TRIES" \
  GOMAP_REPLACE="$GOMAP_REPLACE" \
  STORAGE_LAYOUTS="$TREEDB_QUERY_STORAGE_LAYOUTS" \
  SUITE="minimal" \
  QUERY_CELLS="q1 q2 q3 q4 q4a q4b q5" \
  TREEDB_ALLOW_ERRORS="$TREEDB_ALLOW_ERRORS" \
  OUT_DIR="$TREE_OUT" \
  ./run_columnstore_benchmark.sh
}

clickhouse_query() {
  "$CLICKHOUSE_BIN" local --path "$CLICKHOUSE_PATH" "$@" </dev/null
}

clickhouse_insert() {
  "$CLICKHOUSE_BIN" local --path "$CLICKHOUSE_PATH" "$@"
}

clickhouse_scalar() {
  clickhouse_query --query "$1 FORMAT TSVRaw" | tail -n 1 | tr -d '\r'
}

run_clickhouse() {
  if ! command -v "$CLICKHOUSE_BIN" >/dev/null 2>&1; then
    echo "$CLICKHOUSE_BIN is required for ClickHouse comparison" >&2
    exit 1
  fi
  if ! command -v jq >/dev/null 2>&1; then
    echo "jq is required for ClickHouse result JSON" >&2
    exit 1
  fi
  if [[ "$ROWS" -lt 1000000 || $((ROWS % 1000000)) -ne 0 ]]; then
    echo "ClickHouse comparison expects ROWS to be a positive multiple of 1000000; use RUN_CLICKHOUSE=0 for smoke runs" >&2
    exit 2
  fi
  if [[ -z "$CLICKHOUSE_MAX_FILES" ]]; then
    CLICKHOUSE_MAX_FILES=$(( (ROWS + 999999) / 1000000 ))
  fi
  require_positive_int CLICKHOUSE_MAX_FILES "$CLICKHOUSE_MAX_FILES"

  rm -rf "$CLICKHOUSE_OUT"
  mkdir -p "$CLICKHOUSE_OUT"
  CLICKHOUSE_PATH="$CLICKHOUSE_OUT/chdb"
  export CLICKHOUSE_PATH

  cat > "$CLICKHOUSE_OUT/schema.sql" <<'SQL'
CREATE TABLE bluesky
(
    data JSON(
        max_dynamic_paths = 0,
        kind LowCardinality(String),
        commit.operation LowCardinality(String),
        commit.collection LowCardinality(String),
        did String,
        time_us UInt64) CODEC(ZSTD(1))
)
ENGINE = MergeTree
ORDER BY (
    data.kind,
    data.commit.operation,
    data.commit.collection,
    data.did,
    fromUnixTimestamp64Micro(data.time_us))
SETTINGS object_serialization_version = 'v3',
         dynamic_serialization_version = 'v3',
         object_shared_data_serialization_version = 'advanced',
         object_shared_data_serialization_version_for_zero_level_parts = 'map_with_buckets';
SQL

  echo "==> ClickHouse local JSON comparison"
  echo "    binary:    $CLICKHOUSE_BIN"
  echo "    data:      $DATA_DIR"
  echo "    rows:      $ROWS"
  echo "    tries:     $TRIES"
  echo "    files:     $CLICKHOUSE_MAX_FILES"
  echo "    out:       $CLICKHOUSE_OUT"

  if [[ ! -f "$PROJECT_DIR/clickhouse/queries.sql" ]]; then
    echo "queries file not found: $PROJECT_DIR/clickhouse/queries.sql" >&2
    exit 1
  fi

  load_start=$(python3 -c 'import time; print(time.time())')
  "$CLICKHOUSE_BIN" local --path "$CLICKHOUSE_PATH" --multiquery < "$CLICKHOUSE_OUT/schema.sql"
  insert_settings="min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0"
  if bool_enabled "$CLICKHOUSE_ALLOW_ERRORS"; then
    insert_settings="$insert_settings, input_format_allow_errors_num = 1000000000, input_format_allow_errors_ratio = 1"
  fi

  input_files=()
  while IFS= read -r file; do
    input_files+=("$file")
  done < <(find "$DATA_DIR" -maxdepth 1 \( -name '*.json.gz' -o -name '*.json' \) -type f | LC_ALL=C sort | head -n "$CLICKHOUSE_MAX_FILES")
  if [[ "${#input_files[@]}" -ne "$CLICKHOUSE_MAX_FILES" ]]; then
    echo "requested $CLICKHOUSE_MAX_FILES input files in $DATA_DIR, found ${#input_files[@]}" >&2
    exit 1
  fi
  for file in "${input_files[@]}"; do
    echo "loading $(basename "$file")"
    if [[ "$file" == *.gz ]]; then
      gzip -dc "$file" | clickhouse_insert --query "INSERT INTO bluesky SETTINGS $insert_settings FORMAT JSONAsObject"
    else
      clickhouse_insert --query "INSERT INTO bluesky SETTINGS $insert_settings FORMAT JSONAsObject" < "$file"
    fi
  done
  load_end=$(python3 -c 'import time; print(time.time())')
  load_seconds=$(python3 - <<PY
print($load_end - $load_start)
PY
)

  loaded_rows="$(clickhouse_scalar "SELECT count() FROM bluesky")"
  echo "    loaded:    $loaded_rows"

  : > "$CLICKHOUSE_OUT/attempts.tsv"
  qidx=0
  while IFS= read -r query || [[ -n "$query" ]]; do
    [[ -z "${query//[[:space:]]/}" ]] && continue
    qidx=$((qidx + 1))
    attempt=1
    while [[ "$attempt" -le "$TRIES" ]]; do
      echo "running ClickHouse q$qidx attempt $attempt"
      output="$(clickhouse_query --time --format=Null --progress 0 --query "$query" 2>&1)"
      sec="$(printf '%s\n' "$output" | awk '/^Elapsed:/ { gsub(/[^0-9.]/, "", $2); print $2; exit }')"
      if [[ -z "$sec" ]]; then
        sec="$(printf '%s\n' "$output" | awk '/^[0-9]+([.][0-9]+)?$/ { print $1; exit }')"
      fi
      if [[ -z "$sec" ]]; then
        echo "could not parse ClickHouse query time for q$qidx attempt $attempt" >&2
        printf '%s\n' "$output" >&2
        exit 1
      fi
      printf '%s\t%s\t%s\n' "$qidx" "$attempt" "$sec" >> "$CLICKHOUSE_OUT/attempts.tsv"
      attempt=$((attempt + 1))
    done
  done < "$PROJECT_DIR/clickhouse/queries.sql"

  jq -Rn '
    [inputs
      | select(length > 0)
      | split("\t")
      | {q: (.[0] | tonumber), attempt: (.[1] | tonumber), sec: (.[2] | tonumber)}]
    | sort_by(.q, .attempt)
    | group_by(.q)
    | map(map(.sec))
  ' < "$CLICKHOUSE_OUT/attempts.tsv" > "$CLICKHOUSE_OUT/result_times.json"

  total_size="$(clickhouse_scalar "SELECT coalesce(sum(bytes_on_disk), 0) FROM system.parts WHERE database = 'default' AND table = 'bluesky' AND active")"
  data_size="$(clickhouse_scalar "SELECT coalesce(sum(data_compressed_bytes), 0) FROM system.parts WHERE database = 'default' AND table = 'bluesky' AND active")"
  index_size="$(clickhouse_scalar "SELECT coalesce(sum(primary_key_size) + sum(marks_bytes), 0) FROM system.parts WHERE database = 'default' AND table = 'bluesky' AND active")"
  version="$(clickhouse_scalar "SELECT version()")"
  os_name="$(uname -srm)"
  machine_name="$(hostname 2>/dev/null || printf local)"

  jq -n \
    --arg system "ClickHouse" \
    --arg version "$version" \
    --arg os "$os_name" \
    --arg date "$(date -u +%F)" \
    --arg machine "$machine_name" \
    --arg retains_structure "yes" \
    --arg allow_errors "$CLICKHOUSE_ALLOW_ERRORS" \
    --argjson requested_rows "$ROWS" \
    --argjson dataset_size "$loaded_rows" \
    --argjson num_loaded_documents "$loaded_rows" \
    --argjson total_size "$total_size" \
    --argjson data_size "$data_size" \
    --argjson index_size "$index_size" \
    --argjson load_seconds "$load_seconds" \
    --slurpfile result "$CLICKHOUSE_OUT/result_times.json" \
    '{
      system: $system,
      version: $version,
      os: $os,
      date: $date,
      machine: $machine,
      retains_structure: $retains_structure,
      tags: (["local", "clickhouse-local"] + (if $allow_errors == "1" then ["allow-errors"] else [] end)),
      requested_rows: $requested_rows,
      dataset_size: $dataset_size,
      num_loaded_documents: $num_loaded_documents,
      total_size: $total_size,
      data_size: $data_size,
      index_size: $index_size,
      load_seconds: $load_seconds,
      result: $result[0]
    }' > "$CLICKHOUSE_OUT/result.json"
}

write_summary() {
  if [[ -z "$TREEDB_RESULT" ]]; then
    TREEDB_RESULT="$TREE_OUT/report.json"
  fi
  if [[ -z "$CLICKHOUSE_RESULT" ]]; then
    CLICKHOUSE_RESULT="$CLICKHOUSE_OUT/result.json"
  fi
  export OUT_DIR TREE_OUT CLICKHOUSE_OUT SUMMARY_OUT ROWS TRIES GOMAP_REPLACE TREEDB_RESULT CLICKHOUSE_RESULT
  python3 - <<'PY' > "$SUMMARY_OUT"
import json
import os

out_dir = os.environ["OUT_DIR"]
tree_out = os.environ["TREE_OUT"]
clickhouse_out = os.environ["CLICKHOUSE_OUT"]
summary_out = os.environ["SUMMARY_OUT"]
rows_requested = int(os.environ["ROWS"])
tries = os.environ["TRIES"]

def load_json(path):
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"result file not found: {path}; set OUT_DIR to an existing run "
            "or pass TREEDB_RESULT/CLICKHOUSE_RESULT when reusing partial runs"
        )
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

tree_doc = load_json(os.environ["TREEDB_RESULT"])
ch_doc = load_json(os.environ["CLICKHOUSE_RESULT"])
ch_times = ch_doc["result"]
if len(ch_times) < 5:
    raise ValueError(f"ClickHouse result has {len(ch_times)} query rows, expected at least 5")

queries = ["q1", "q2", "q3", "q4", "q4a", "q4b", "q5"]
clickhouse_query_index = {
    "q1": 0,
    "q2": 1,
    "q3": 2,
    "q4": 3,
    "q4a": 3,
    "q4b": 3,
    "q5": 4,
}
tree_report_rows = [
    row for row in tree_doc["rows"]
    if row.get("system", "TreeDB") == "TreeDB"
]

def select_rows(layouts, data_shape=None):
    selected = {}
    layout_rank = {layout: idx for idx, layout in enumerate(layouts)}
    for row in tree_report_rows:
        layout = row.get("storage_layout", "")
        if layout not in layout_rank:
            continue
        if data_shape is not None and row.get("data_shape", "") != data_shape:
            continue
        query = row.get("query")
        if query not in queries:
            continue
        current = selected.get(query)
        if current is None or layout_rank[layout] < layout_rank.get(current.get("storage_layout", ""), len(layouts)):
            selected[query] = row
    return selected

tree_storage_rows = select_rows(["column-store-full-prepared", "column-store-full"], "full-retained-json")
missing_storage = [query for query in queries if query not in tree_storage_rows]
if missing_storage:
    raise ValueError(
        "TreeDB report is missing full-data typed_column_part storage rows for: "
        + ", ".join(missing_storage)
    )

tree_attribution_rows = select_rows(
    ["column-store-prepared-metadata", "column-store-prepared", "column-store"],
    "query-shaped-projection",
)

def best(values):
    return min(values)

def median(values):
    values = sorted(values)
    mid = len(values) // 2
    if len(values) % 2:
        return values[mid]
    return (values[mid - 1] + values[mid]) / 2

def seconds(value):
    if value < 0.001:
        return f"{value * 1e6:.1f}us"
    if value < 1:
        return f"{value:.4f}s"
    return f"{value:.3f}s"

def count(value):
    value = float(value)
    if value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}B"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    if value >= 1_000:
        return f"{value / 1_000:.1f}k"
    return str(int(value))

def byte_count(value):
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    value = float(value)
    idx = 0
    while value >= 1024 and idx < len(units) - 1:
        value /= 1024
        idx += 1
    if idx == 0:
        return f"{int(value)} B"
    return f"{value:.2f} {units[idx]}"

def rows_per_second(rows, elapsed):
    return count(rows / elapsed)

def comparison(tree_sec, ch_sec):
    if tree_sec <= ch_sec:
        return f"{ch_sec / tree_sec:.1f}x faster"
    return f"{tree_sec / ch_sec:.1f}x slower"

print("# Preferred TreeDB column-store vs ClickHouse JSONBench")
print()
print(f"- Rows requested: `{rows_requested}`")
print(f"- Tries: `{tries}`")
print(f"- TreeDB report JSON: `{os.environ['TREEDB_RESULT']}`")
print(f"- ClickHouse result JSON: `{os.environ['CLICKHOUSE_RESULT']}`")
print()
print("## Full-data storage headline")
print()
print("| system/layout | query | best | loaded rows/s | scanned rows | storage | typed part | WAL | load | TreeDB vs ClickHouse |")
print("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
for idx, query in enumerate(queries):
    tree = tree_storage_rows[query]
    ch_best = best(ch_times[clickhouse_query_index[query]])
    label = tree.get("storage_layout", "column-store-full")
    scanned = tree.get("rows_scanned", 0)
    tree_dataset_size = tree.get("dataset_size", rows_requested)
    tree_rps = rows_per_second(tree_dataset_size, tree["best_seconds"])
    if scanned == 0:
        tree_rps += " logical"
    print(
        f"| TreeDB {label} | {query} | {seconds(tree['best_seconds'])} | {tree_rps} | "
        f"{count(scanned) if scanned else '0'} | {byte_count(tree.get('storage_bytes', 0))} | "
        f"{byte_count(tree.get('storage_typed_column_part_bytes', 0))} | "
        f"{byte_count(tree.get('storage_wal_bytes', 0))} | "
        f"{seconds(tree.get('load_seconds', 0))} | {comparison(tree['best_seconds'], ch_best)} |"
    )
    print(
        f"| ClickHouse JSON | {query} | {seconds(ch_best)} | "
        f"{rows_per_second(ch_doc['dataset_size'], ch_best)} | {count(ch_doc['dataset_size'])} | "
        f"{byte_count(ch_doc['total_size'])} | n/a | n/a | {seconds(ch_doc.get('load_seconds', 0))} | baseline |"
    )
print()
sample_storage = tree_storage_rows[queries[0]]
print("Full-data TreeDB storage row labels:")
print()
print(f"- data shape: `{sample_storage.get('data_shape', '')}`")
print(f"- retained payload: `{sample_storage.get('retained_payload_policy', '')}`")
print(f"- typed owner: `{sample_storage.get('typed_column_owner', '')}`")
print(f"- reconstruction valid: `{sample_storage.get('reconstruction_valid', '')}`")
print(f"- measurement phase: `{sample_storage.get('storage_measurement_phase', '')}`")
print()
if tree_attribution_rows:
    print("## Query-shaped attribution rows")
    print()
    print("| layout | query | best | scanned rows | storage | typed owner | note |")
    print("|---|---:|---:|---:|---:|---|---|")
    for query in queries:
        tree = tree_attribution_rows.get(query)
        if tree is None:
            continue
        scanned = tree.get("rows_scanned", 0)
        print(
            f"| {tree.get('storage_layout', '')} | {query} | {seconds(tree['best_seconds'])} | "
            f"{count(scanned) if scanned else '0'} | {byte_count(tree.get('storage_bytes', 0))} | "
            f"{tree.get('typed_column_owner', '')} | attribution only |"
        )
print()
print("## ClickHouse attempts")
print()
print("| query | attempts | best | median |")
print("|---:|---|---:|---:|")
for idx, values in enumerate(ch_times, start=1):
    print(f"| q{idx} | {', '.join(seconds(v) for v in values)} | {seconds(best(values))} | {seconds(median(values))} |")
print()
print("## Caveats")
print()
print("- The full-data TreeDB headline uses `column-store-full`/`column-store-full-prepared` rows with retained non-column JSON plus `typed_column_part` hot-path columns.")
print("- Query-shaped `column-store*` rows are attribution rows only; their storage is not compared to ClickHouse as apples-to-apples storage.")
print("- q4/q4a/q4b/q5 aggregate-metadata Top-K attribution rows may still report scanned rows as 0; the full-data headline reports the full-data scan path unless a full-data metadata layout is added.")
print("- q4a/q4b TreeDB rows use the same q4 grouped-min physical query; the preferred summary maps them to the q4 ClickHouse query unless a separate ClickHouse q4 fairness artifact is supplied.")
print("- ClickHouse uses `JSONAsObject`; with `CLICKHOUSE_ALLOW_ERRORS=1`, rows ClickHouse rejects as invalid JSON are skipped and reflected in the ClickHouse loaded-row count.")
print("- TreeDB uses `TREEDB_ALLOW_ERRORS=1` in this preferred comparison by default when ClickHouse allow-errors is enabled; skipped malformed JSON rows are reflected in the TreeDB loaded-row count and result JSON counters.")
print()
print(f"Summary written to `{summary_out}`.")
PY
  cat "$SUMMARY_OUT"
}

if [[ "$RUN_TREEDB" == "1" ]]; then
  run_treedb
fi
if [[ "$RUN_CLICKHOUSE" == "1" ]]; then
  run_clickhouse
fi
if [[ -z "$TREEDB_RESULT" ]]; then
  TREEDB_RESULT="$TREE_OUT/report.json"
fi
if [[ -z "$CLICKHOUSE_RESULT" ]]; then
  CLICKHOUSE_RESULT="$CLICKHOUSE_OUT/result.json"
fi
if [[ ! -f "$TREEDB_RESULT" ]]; then
  echo "missing TreeDB result: $TREEDB_RESULT; run with RUN_TREEDB=1 or pass TREEDB_RESULT" >&2
  exit 1
fi
if [[ ! -f "$CLICKHOUSE_RESULT" ]]; then
  echo "missing ClickHouse result: $CLICKHOUSE_RESULT; run with RUN_CLICKHOUSE=1 or pass CLICKHOUSE_RESULT" >&2
  exit 1
fi
write_summary
