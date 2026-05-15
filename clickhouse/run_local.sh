#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

DATA_DIR="${DATA_DIR:-$HOME/data/bluesky}"
ROWS="${ROWS:-${SUBSET_ROWS:-1000000}}"
TRIES="${TRIES:-3}"
CHUNK_ROWS="${CHUNK_ROWS:-1000000}"
OUT_DIR="${OUT_DIR:-$SCRIPT_DIR/local_results/run_$(date -u +%Y%m%d_%H%M%S)_$$}"
DB_NAME="${DB_NAME:-jsonbench_local_${ROWS}_$(date -u +%Y%m%d_%H%M%S)_$$}"
TABLE_NAME="${TABLE_NAME:-bluesky}"
QUERY_FILE="${QUERY_FILE:-$SCRIPT_DIR/queries.sql}"
RESULT_FILE="${RESULT_FILE:-$OUT_DIR/result.json}"
CLICKHOUSE_SERVER="${CLICKHOUSE_SERVER:-jsonbench-local}"
START_SERVER="${START_SERVER:-1}"
STOP_SERVER_ON_EXIT="${STOP_SERVER_ON_EXIT:-0}"
DROP_DATABASE_AFTER="${DROP_DATABASE_AFTER:-0}"
CLIENT_BIN="${CLIENT_BIN:-clickhousectl}"
RUN_Q4_FAIRNESS="${RUN_Q4_FAIRNESS:-0}"

usage() {
  cat <<'EOF'
Usage: ./run_local.sh [flags]

Runs JSONBench's ClickHouse path locally and writes a JSONBench-compatible result.

Environment:
  DATA_DIR              Directory with JSONBench .json.gz or .json files.
                        Defaults to ~/data/bluesky.
  ROWS                  Exact number of rows to load. Defaults to 1000000.
                        SUBSET_ROWS is accepted as an alias when ROWS is unset.
  TRIES                 Query attempts per query. Defaults to 3.
  CHUNK_ROWS            Rows per temporary load chunk. Defaults to 1000000.
  OUT_DIR               Output directory. Defaults to clickhouse/local_results/run_*.
  DB_NAME               Fresh ClickHouse database name. Defaults to unique jsonbench_local_*.
  CLICKHOUSE_SERVER     clickhousectl local server name. Defaults to jsonbench-local.
  START_SERVER          Set to 1 to start CLICKHOUSE_SERVER when absent. Defaults to 1.
  STOP_SERVER_ON_EXIT   Set to 1 to stop a server this script started. Defaults to 0.
  DROP_DATABASE_AFTER   Set to 1 to drop DB_NAME after writing result JSON. Defaults to 0.
  RUN_Q4_FAIRNESS       Set to 1 to create a time-ordered q4 table and write
                        q4_fairness timings into result.json. Defaults to 0.

Flags:
  -h, --help            Show this help.

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

require_db_identifier() {
  local name="$1"
  local value="$2"
  if ! [[ "$value" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
    echo "$name must be a ClickHouse identifier using [A-Za-z_][A-Za-z0-9_]*, got: $value" >&2
    exit 2
  fi
}

client() {
  (cd "$PROJECT_DIR" && "$CLIENT_BIN" local client --name "$CLICKHOUSE_SERVER" -- "$@")
}

client_scalar() {
  client --query "$1 FORMAT TSVRaw"
}

server_running() {
  (cd "$PROJECT_DIR" && "$CLIENT_BIN" local server list --json 2>/dev/null) \
    | jq -e --arg name "$CLICKHOUSE_SERVER" '.servers[]? | select(.name == $name and .running == true)' >/dev/null
}

wait_for_server() {
  local attempt=1
  while [[ "$attempt" -le 30 ]]; do
    if client --query "SELECT 1 FORMAT Null" >/dev/null 2>&1; then
      return
    fi
    sleep 1
    attempt=$((attempt + 1))
  done
  cat >&2 <<EOF
ClickHouse server '$CLICKHOUSE_SERVER' did not become ready.
Start it in another terminal and rerun:
  clickhousectl local server start --name $CLICKHOUSE_SERVER --foreground
EOF
  exit 1
}

ensure_server() {
  if server_running; then
    wait_for_server
    return
  fi
  if [[ "$START_SERVER" != "1" ]]; then
    echo "ClickHouse server '$CLICKHOUSE_SERVER' is not running. Start it or set START_SERVER=1." >&2
    exit 1
  fi
  (cd "$PROJECT_DIR" && "$CLIENT_BIN" local server start --name "$CLICKHOUSE_SERVER")
  wait_for_server
  STARTED_SERVER=1
}

cleanup() {
  if [[ "${DROP_DATABASE_AFTER}" == "1" ]]; then
    client --query "DROP DATABASE IF EXISTS $DB_NAME" >/dev/null 2>&1 || true
  fi
  if [[ "${STARTED_SERVER:-0}" == "1" && "$STOP_SERVER_ON_EXIT" == "1" ]]; then
    (cd "$PROJECT_DIR" && "$CLIENT_BIN" local server stop "$CLICKHOUSE_SERVER" >/dev/null 2>&1) || true
  fi
}
trap cleanup EXIT

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

create_schema() {
  client --query "DROP DATABASE IF EXISTS $DB_NAME"
  client --query "CREATE DATABASE $DB_NAME"
  client --query "
CREATE TABLE $DB_NAME.$TABLE_NAME
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
         object_shared_data_serialization_version_for_zero_level_parts = 'map_with_buckets'"
}

load_chunks() {
  local chunk=""
  for chunk in "$CHUNK_DIR"/chunk_*; do
    [[ -f "$chunk" ]] || continue
    echo "loading $(basename "$chunk")"
    client \
      --query "INSERT INTO $DB_NAME.$TABLE_NAME SETTINGS min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0 FORMAT JSONAsObject" \
      < "$chunk"
  done
}

create_q4_fairness_tables() {
  if [[ "$RUN_Q4_FAIRNESS" != "1" ]]; then
    printf '[]\n' > "$OUT_DIR/q4_fairness.json"
    return
  fi

  local q4_time_table="${TABLE_NAME}_q4_time"
  client --query "DROP TABLE IF EXISTS $DB_NAME.$q4_time_table"
  client --query "
CREATE TABLE $DB_NAME.$q4_time_table
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
    fromUnixTimestamp64Micro(data.time_us),
    data.kind,
    data.commit.operation,
    data.commit.collection,
    data.did)
SETTINGS object_serialization_version = 'v3',
         dynamic_serialization_version = 'v3',
         object_shared_data_serialization_version = 'advanced',
         object_shared_data_serialization_version_for_zero_level_parts = 'map_with_buckets'"
  client --query "INSERT INTO $DB_NAME.$q4_time_table SELECT * FROM $DB_NAME.$TABLE_NAME"
  client --query "OPTIMIZE TABLE $DB_NAME.$q4_time_table FINAL"
}

run_queries() {
  local query=""
  local query_idx=0
  local attempt=0
  local sec=""

  : > "$OUT_DIR/attempts.tsv"
  while IFS= read -r query || [[ -n "$query" ]]; do
    [[ -z "${query//[[:space:]]/}" ]] && continue
    query_idx=$((query_idx + 1))
    attempt=1
    while [[ "$attempt" -le "$TRIES" ]]; do
      echo "running q$query_idx attempt $attempt"
      sec=$(client --database "$DB_NAME" --time --format=Null --progress 0 --query "$query" 2>&1 | awk '/^[0-9]+([.][0-9]+)?$/ { print $1; exit }')
      if [[ -z "$sec" ]]; then
        echo "could not parse ClickHouse query time for q$query_idx attempt $attempt" >&2
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

run_q4_fairness_queries() {
  if [[ "$RUN_Q4_FAIRNESS" != "1" ]]; then
    return
  fi

  local q4_time_table="${TABLE_NAME}_q4_time"
  local attempt=0
  local sec=""
  local label=""
  local description=""
  local table=""
  local sort_key=""
  local query_shape=""
  local query=""

  : > "$OUT_DIR/q4_fairness_attempts.tsv"

  run_q4_fairness_query() {
    label="$1"
    description="$2"
    table="$3"
    sort_key="$4"
    query_shape="$5"
    query="$6"
    attempt=1
    while [[ "$attempt" -le "$TRIES" ]]; do
      echo "running $label attempt $attempt"
      sec=$(client --database "$DB_NAME" --time --format=Null --progress 0 --query "$query" 2>&1 | awk '/^[0-9]+([.][0-9]+)?$/ { print $1; exit }')
      if [[ -z "$sec" ]]; then
        echo "could not parse ClickHouse query time for $label attempt $attempt" >&2
        exit 1
      fi
      printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$label" "$description" "$table" "$sort_key" "$query_shape" "$attempt" "$sec" >> "$OUT_DIR/q4_fairness_attempts.tsv"
      attempt=$((attempt + 1))
    done
  }

  run_q4_fairness_query \
    "q4a_aggregate" \
    "Q4 aggregate over time-ordered table" \
    "$q4_time_table" \
    "time_us,kind,operation,collection,did" \
    "aggregate_min_group_order_limit" \
    "SELECT data.did::String as user_id, min(fromUnixTimestamp64Micro(data.time_us)) as first_post_ts FROM $q4_time_table WHERE data.kind = 'commit' AND data.commit.operation = 'create' AND data.commit.collection = 'app.bsky.feed.post' GROUP BY user_id ORDER BY first_post_ts ASC LIMIT 3"

  run_q4_fairness_query \
    "q4a_streaming_limit_by" \
    "Q4 streaming-shaped first user rows over time-ordered table" \
    "$q4_time_table" \
    "time_us,kind,operation,collection,did" \
    "order_by_time_limit_1_by_user_limit_3" \
    "SELECT user_id, first_post_ts FROM (SELECT data.did::String AS user_id, fromUnixTimestamp64Micro(data.time_us) AS first_post_ts FROM $q4_time_table WHERE data.kind = 'commit' AND data.commit.operation = 'create' AND data.commit.collection = 'app.bsky.feed.post' ORDER BY first_post_ts ASC, user_id ASC LIMIT 1 BY user_id) ORDER BY first_post_ts ASC, user_id ASC LIMIT 3"

  run_q4_fairness_query \
    "q4b_aggregate" \
    "Q4 aggregate over standard ClickHouse table order" \
    "$TABLE_NAME" \
    "kind,operation,collection,did,time_us" \
    "aggregate_min_group_order_limit" \
    "SELECT data.did::String as user_id, min(fromUnixTimestamp64Micro(data.time_us)) as first_post_ts FROM $TABLE_NAME WHERE data.kind = 'commit' AND data.commit.operation = 'create' AND data.commit.collection = 'app.bsky.feed.post' GROUP BY user_id ORDER BY first_post_ts ASC LIMIT 3"

  jq -Rn '
    [inputs
      | select(length > 0)
      | split("\t")
      | {
          query: .[0],
          description: .[1],
          table: .[2],
          sort_key: .[3],
          query_shape: .[4],
          attempt: (.[5] | tonumber),
          sec: (.[6] | tonumber)
        }]
    | sort_by(.query, .attempt)
    | group_by(.query)
    | map({
        query: .[0].query,
        description: .[0].description,
        table: .[0].table,
        sort_key: .[0].sort_key,
        query_shape: .[0].query_shape,
        attempts: (sort_by(.attempt) | map(.sec)),
        best: (map(.sec) | min)
      })
  ' < "$OUT_DIR/q4_fairness_attempts.tsv" > "$OUT_DIR/q4_fairness.json"
}

require_positive_int ROWS "$ROWS"
require_positive_int TRIES "$TRIES"
require_positive_int CHUNK_ROWS "$CHUNK_ROWS"
require_db_identifier DB_NAME "$DB_NAME"
require_db_identifier TABLE_NAME "$TABLE_NAME"

if [[ ! -d "$DATA_DIR" ]]; then
  echo "DATA_DIR does not exist: $DATA_DIR" >&2
  exit 1
fi
if [[ ! -f "$QUERY_FILE" ]]; then
  echo "QUERY_FILE does not exist: $QUERY_FILE" >&2
  exit 1
fi
if ! command -v "$CLIENT_BIN" >/dev/null 2>&1; then
  echo "$CLIENT_BIN is required. Install ClickHouse CLI with: curl https://clickhouse.com/cli | sh" >&2
  exit 1
fi
if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required to write result JSON" >&2
  exit 1
fi

mkdir -p "$OUT_DIR"
CHUNK_DIR="$OUT_DIR/chunks"

echo "ClickHouse server: $CLICKHOUSE_SERVER"
echo "Rows requested: $ROWS"
echo "Database: $DB_NAME"
echo "Data directory: $DATA_DIR"
echo "Output directory: $OUT_DIR"

ensure_server
materialize_rows
create_schema
load_chunks
create_q4_fairness_tables

loaded_rows="$(client_scalar "SELECT count() FROM $DB_NAME.$TABLE_NAME" | tail -n 1 | tr -d '\r')"
if [[ "$loaded_rows" != "$ROWS" ]]; then
  echo "ClickHouse row count is $loaded_rows, requested $ROWS" >&2
  exit 1
fi

run_queries
run_q4_fairness_queries
total_size="$(client_scalar "SELECT coalesce(sum(bytes_on_disk), 0) FROM system.parts WHERE database = '$DB_NAME' AND table = '$TABLE_NAME' AND active" | tail -n 1 | tr -d '\r')"
data_size="$(client_scalar "SELECT coalesce(sum(data_compressed_bytes), 0) FROM system.parts WHERE database = '$DB_NAME' AND table = '$TABLE_NAME' AND active" | tail -n 1 | tr -d '\r')"
index_size="$(client_scalar "SELECT coalesce(sum(primary_key_size) + sum(marks_bytes), 0) FROM system.parts WHERE database = '$DB_NAME' AND table = '$TABLE_NAME' AND active" | tail -n 1 | tr -d '\r')"
version="$(client_scalar "SELECT version()" | tail -n 1 | tr -d '\r')"
os_name="$(uname -srm)"
machine_name="$(hostname 2>/dev/null || printf 'local')"

jq -n \
  --arg system "ClickHouse" \
  --arg version "$version" \
  --arg os "$os_name" \
  --arg date "$(date -u +%F)" \
  --arg machine "$machine_name" \
  --arg retains_structure "yes" \
  --argjson dataset_size "$ROWS" \
  --argjson num_loaded_documents "$loaded_rows" \
  --argjson total_size "$total_size" \
  --argjson data_size "$data_size" \
  --argjson index_size "$index_size" \
  --slurpfile result "$OUT_DIR/result_times.json" \
  --slurpfile q4_fairness "$OUT_DIR/q4_fairness.json" \
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
    data_size: $data_size,
    index_size: $index_size,
    result: $result[0],
    q4_fairness: $q4_fairness[0]
  }' > "$RESULT_FILE"

rm -rf "$CHUNK_DIR"
echo "$RESULT_FILE"
