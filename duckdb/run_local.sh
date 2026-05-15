#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

DATA_DIR="${DATA_DIR:-$HOME/data/bluesky}"
ROWS="${ROWS:-${SUBSET_ROWS:-1000000}}"
TRIES="${TRIES:-3}"
CHUNK_ROWS="${CHUNK_ROWS:-100000}"
OUT_DIR="${OUT_DIR:-$SCRIPT_DIR/local_results/run_$(date -u +%Y%m%d_%H%M%S)_$$}"
DB_PATH="${DB_PATH:-$OUT_DIR/bluesky.duckdb}"
TABLE_NAME="${TABLE_NAME:-bluesky}"
QUERY_FILE="${QUERY_FILE:-$SCRIPT_DIR/queries.sql}"
RESULT_FILE="${RESULT_FILE:-$OUT_DIR/result.json}"
INSTALL_DUCKDB="${INSTALL_DUCKDB:-0}"
KEEP_DB="${KEEP_DB:-1}"

usage() {
  cat <<'EOF'
Usage: ./run_local.sh [flags]

Runs the JSONBench DuckDB path locally and writes a JSONBench-compatible result.

Environment:
  DATA_DIR          Directory with JSONBench .json.gz or .json files.
                    Defaults to ~/data/bluesky.
  ROWS              Exact number of rows to load. Defaults to 1000000.
                    SUBSET_ROWS is accepted as an alias when ROWS is unset.
  TRIES             Query attempts per query. Defaults to 3.
  CHUNK_ROWS        Rows per temporary load chunk. Defaults to 100000.
  OUT_DIR           Output directory. Defaults to duckdb/local_results/run_*.
  DB_PATH           DuckDB database path. Defaults to OUT_DIR/bluesky.duckdb.
  DUCKDB_BIN        DuckDB CLI path. Defaults to PATH or ~/.duckdb/cli/latest.
  INSTALL_DUCKDB    Set to 1 to install DuckDB if no CLI is found.
  KEEP_DB           Set to 0 to remove DB_PATH after writing result JSON.

Flags:
  -h, --help        Show this help.

The runner never accepts partial data. If DATA_DIR has fewer rows than ROWS,
the run fails instead of silently producing a shorter benchmark.
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

install_duckdb() {
  case "$(uname -s)" in
    Darwin)
      if ! command -v brew >/dev/null 2>&1; then
        echo "DuckDB CLI not found and Homebrew is not available. Install DuckDB or set DUCKDB_BIN." >&2
        exit 1
      fi
      brew install duckdb
      ;;
    *)
      if ! command -v curl >/dev/null 2>&1; then
        echo "DuckDB CLI not found and curl is not available. Install DuckDB or set DUCKDB_BIN." >&2
        exit 1
      fi
      curl https://install.duckdb.org | sh
      ;;
  esac
}

resolve_duckdb() {
  if [[ -n "${DUCKDB_BIN:-}" ]]; then
    if [[ ! -x "$DUCKDB_BIN" ]]; then
      echo "DUCKDB_BIN is not executable: $DUCKDB_BIN" >&2
      exit 1
    fi
    printf '%s\n' "$DUCKDB_BIN"
    return
  fi
  if command -v duckdb >/dev/null 2>&1; then
    command -v duckdb
    return
  fi
  if [[ -x "$HOME/.duckdb/cli/latest/duckdb" ]]; then
    printf '%s\n' "$HOME/.duckdb/cli/latest/duckdb"
    return
  fi
  if [[ "$INSTALL_DUCKDB" == "1" ]]; then
    install_duckdb >&2
    if command -v duckdb >/dev/null 2>&1; then
      command -v duckdb
      return
    fi
    if [[ -x "$HOME/.duckdb/cli/latest/duckdb" ]]; then
      printf '%s\n' "$HOME/.duckdb/cli/latest/duckdb"
      return
    fi
  fi
  cat >&2 <<'EOF'
DuckDB CLI was not found.

Install it with:
  brew install duckdb

Or run this harness with:
  INSTALL_DUCKDB=1 ./run_local.sh

Or point at an existing binary:
  DUCKDB_BIN=/path/to/duckdb ./run_local.sh
EOF
  exit 1
}

sql_string() {
  printf "'"
  printf "%s" "$1" | sed "s/'/''/g"
  printf "'"
}

input_files() {
  find "$DATA_DIR" -maxdepth 1 \( -name '*.json.gz' -o -name '*.json' \) -type f | LC_ALL=C sort
}

materialize_rows() {
  local remaining="$ROWS"
  local loaded=0
  local file=""
  local idx=0
  local prefix=""
  local chunk=""
  local n=0
  local generated=0
  local producer_status=0
  local limiter_status=0
  local split_status=0
  local statuses=()

  mkdir -p "$CHUNK_DIR"
  while IFS= read -r file; do
    [[ -z "$file" ]] && continue
    if [[ "$remaining" -le 0 ]]; then
      break
    fi

    prefix=$(printf "%s/chunk_%06d_" "$CHUNK_DIR" "$idx")
    echo "materializing up to $remaining rows from $file"

    set +e
    set +o pipefail
    if [[ "$file" == *.gz ]]; then
      gzip -dc "$file" | head -n "$remaining" | split -a 6 -l "$CHUNK_ROWS" - "$prefix"
      statuses=("${PIPESTATUS[@]}")
      producer_status=${statuses[0]:-0}
      limiter_status=${statuses[1]:-0}
      split_status=${statuses[2]:-0}
    else
      head -n "$remaining" < "$file" | split -a 6 -l "$CHUNK_ROWS" - "$prefix"
      statuses=("${PIPESTATUS[@]}")
      producer_status=0
      limiter_status=${statuses[0]:-0}
      split_status=${statuses[1]:-0}
    fi
    set -o pipefail
    set -e

    if [[ "$producer_status" -ne 0 && "$producer_status" -ne 141 ]]; then
      echo "failed to read $file (status $producer_status)" >&2
      exit 1
    fi
    if [[ "$limiter_status" -ne 0 || "$split_status" -ne 0 ]]; then
      echo "failed to materialize chunks from $file (head $limiter_status, split $split_status)" >&2
      exit 1
    fi

    generated=0
    for chunk in "$prefix"*; do
      [[ -f "$chunk" ]] || continue
      n=$(wc -l < "$chunk" | tr -d ' ')
      if [[ "$n" -eq 0 ]]; then
        rm -f "$chunk"
        continue
      fi
      loaded=$((loaded + n))
      remaining=$((ROWS - loaded))
      generated=$((generated + 1))
    done

    if [[ "$generated" -eq 0 ]]; then
      echo "no rows found in $file" >&2
    fi
    idx=$((idx + 1))
  done < <(input_files)

  if [[ "$loaded" -lt "$ROWS" ]]; then
    echo "loaded $loaded rows, requested $ROWS from $DATA_DIR" >&2
    exit 1
  fi

  printf '%s\n' "$loaded" > "$OUT_DIR/loaded_rows.txt"
}

duckdb_csv() {
  "$DUCKDB_BIN_RESOLVED" -csv -noheader "$DB_PATH" -c "$1" | tr -d '\r'
}

table_storage_bytes() {
  local sql=""
  local value=""
  sql="select coalesce(count(distinct block_id) * (select block_size from pragma_database_size()), 0) from pragma_storage_info($(sql_string "$TABLE_NAME"));"
  if value=$(duckdb_csv "$sql" 2>/dev/null | tail -n 1); then
    if [[ "$value" =~ ^[0-9]+$ ]]; then
      printf '%s\n' "$value"
      return
    fi
  fi
  du -sk "$DB_PATH" | awk '{print $1 * 1024}'
}

run_queries() {
  local query=""
  local query_idx=0
  local attempt=0
  local log_file=""
  local time_file=""
  local sec=""

  : > "$OUT_DIR/attempts.tsv"
  while IFS= read -r query || [[ -n "$query" ]]; do
    [[ -z "${query//[[:space:]]/}" ]] && continue
    query_idx=$((query_idx + 1))
    attempt=1
    while [[ "$attempt" -le "$TRIES" ]]; do
      log_file="$OUT_DIR/q${query_idx}_attempt${attempt}.out"
      time_file="$OUT_DIR/q${query_idx}_attempt${attempt}.time"
      echo "running q$query_idx attempt $attempt"
      /usr/bin/time -p "$DUCKDB_BIN_RESOLVED" "$DB_PATH" -c "$query" > "$log_file" 2> "$time_file"
      sec=$(awk '$1 == "real" { print $2; exit }' "$time_file")
      if [[ -z "$sec" ]]; then
        echo "could not parse query time from $time_file" >&2
        exit 1
      fi
      printf '%s\t%s\t%s\n' "$query_idx" "$attempt" "$sec" >> "$OUT_DIR/attempts.tsv"
      attempt=$((attempt + 1))
    done
  done < "$QUERY_FILE"

  jq -Rn '
    [inputs
      | select(length > 0)
      | split("\t")
      | {q: (.[0] | tonumber), attempt: (.[1] | tonumber), sec: (.[2] | tonumber)}]
    | sort_by(.q, .attempt)
    | group_by(.q)
    | map(map(.sec))
  ' < "$OUT_DIR/attempts.tsv" > "$OUT_DIR/result_times.json"
}

require_positive_int ROWS "$ROWS"
require_positive_int TRIES "$TRIES"
require_positive_int CHUNK_ROWS "$CHUNK_ROWS"

if [[ ! -d "$DATA_DIR" ]]; then
  echo "DATA_DIR does not exist: $DATA_DIR" >&2
  exit 1
fi
if [[ ! -f "$QUERY_FILE" ]]; then
  echo "QUERY_FILE does not exist: $QUERY_FILE" >&2
  exit 1
fi
if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required to write result JSON" >&2
  exit 1
fi

DUCKDB_BIN_RESOLVED="$(resolve_duckdb)"
mkdir -p "$OUT_DIR"
CHUNK_DIR="$OUT_DIR/chunks"

echo "DuckDB: $DUCKDB_BIN_RESOLVED"
echo "Rows requested: $ROWS"
echo "Data directory: $DATA_DIR"
echo "Output directory: $OUT_DIR"

rm -f "$DB_PATH" "$DB_PATH.wal"
materialize_rows

"$DUCKDB_BIN_RESOLVED" "$DB_PATH" -c "create table $TABLE_NAME (j JSON);"

for chunk in "$CHUNK_DIR"/chunk_*; do
  [[ -f "$chunk" ]] || continue
  echo "loading $(basename "$chunk")"
  "$DUCKDB_BIN_RESOLVED" "$DB_PATH" -c "insert into $TABLE_NAME select * from read_ndjson_objects($(sql_string "$chunk"), ignore_errors=false, maximum_object_size=1048576000);"
done

loaded_rows="$(duckdb_csv "select count(*) from $TABLE_NAME;" | tail -n 1)"
if [[ "$loaded_rows" != "$ROWS" ]]; then
  echo "DuckDB row count is $loaded_rows, requested $ROWS" >&2
  exit 1
fi

run_queries
total_size="$(table_storage_bytes)"
version="$("$DUCKDB_BIN_RESOLVED" --version 2>/dev/null | tr '\n' ' ' | sed 's/[[:space:]]*$//')"
os_name="$(uname -srm)"
machine_name="$(hostname 2>/dev/null || printf 'local')"

jq -n \
  --arg system "DuckDB" \
  --arg version "$version" \
  --arg os "$os_name" \
  --arg date "$(date -u +%F)" \
  --arg machine "$machine_name" \
  --arg retains_structure "yes" \
  --argjson dataset_size "$ROWS" \
  --argjson num_loaded_documents "$loaded_rows" \
  --argjson total_size "$total_size" \
  --slurpfile result "$OUT_DIR/result_times.json" \
  '{
    system: $system,
    version: $version,
    os: $os,
    date: $date,
    machine: $machine,
    retains_structure: $retains_structure,
    tags: ["local"],
    dataset_size: $dataset_size,
    num_loaded_documents: $num_loaded_documents,
    total_size: $total_size,
    result: $result[0]
  }' > "$RESULT_FILE"

rm -rf "$CHUNK_DIR"
if [[ "$KEEP_DB" == "0" ]]; then
  rm -f "$DB_PATH" "$DB_PATH.wal"
fi

echo "$RESULT_FILE"
