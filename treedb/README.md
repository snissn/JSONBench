# TreeDB JSONBench Harness

This directory adds a TreeDB collections direct-Go harness for JSONBench. It
loads JSONBench Bluesky NDJSON into TreeDB collections, runs the five JSONBench
queries as compiled Go scans, and writes a report that can also include the
explicitly supplied DuckDB and ClickHouse result JSON files.

This directory is the canonical home for TreeDB JSONBench reproduction and
cross-database comparison. The `snissn/gomap` repository owns TreeDB storage
engine implementation, the column-store RFC, and command-WAL readiness; this
repository owns the benchmark runners, result schema, and reports used as
evidence for that work.

Relevant `snissn/gomap` trackers:

- column-store umbrella: `https://github.com/snissn/gomap/issues/1436`
- JSONBench storage comparison: `https://github.com/snissn/gomap/issues/1462`
- column-store RFC PR: `https://github.com/snissn/gomap/pull/1527`
- command-WAL contract PR: `https://github.com/snissn/gomap/pull/1530`

The default TreeDB matrix is the strict minimal JSON suite:

- scale: `subset`, `1m`, `10m` (`100m`/`1000m` are accepted but intentionally
  not the default)
- collection format: `json`
- projection/query cells: `q1` through `q5`

`full` preserves the input JSON document. Query-specific projections store only
the fields needed by that query plus the TreeDB primary key. For example, `q1`
stores only `event = commit.collection`.

Full-document and `template-v1` row-layout cells are available with environment
overrides. A `column-store` storage layout is also available for query-shaped
`json` projection cells. It stores declared projection fields in TreeDB
physical column row assets with `retained_payload=none` and uses physical column
reducers for q1, q2, q4, and q5. q3 still uses the materialized JSON scan over the
column-store fixture because the current physical reducer API has no combined
`event, hour` grouped shape. The harness opens TreeDB with the cached leaf-log
backend so collection data roots can store oversized documents through
persistent value-log pointers.

## Test Data

The committed `treedb/testdata/bluesky` directory is only a 6-row smoke fixture.
Use it to verify the harness, not to produce benchmark numbers.

For real JSONBench data, run the upstream downloader from the JSONBench
repository root:

```sh
cd /Users/michaelseiler/dev/snissn/JSONBench
./download_data.sh
```

Choose:

- `1` for `1m`: downloads `~/data/bluesky/file_0001.json.gz`
- `2` for `10m`: downloads `~/data/bluesky/file_0001.json.gz` through
  `file_0010.json.gz`

The TreeDB harness reads `.json.gz` and `.json` files from `DATA_DIR`, sorted by
filename. The default benchmark data directory is `~/data/bluesky`, matching
`download_data.sh`.

On macOS, install `wget` first if needed:

```sh
brew install wget
```

For a 100k-row working subset, download the `1m` dataset, then run:

```sh
cd /Users/michaelseiler/dev/snissn/JSONBench/treedb
DATA_DIR="$HOME/data/bluesky" SUBSET_ROWS=100000 TRIES=1 ./run_matrix.sh
```

The run fails if fewer rows are available than requested.

To make the reported TreeDB storage column represent a post-load fully
compacted database, enable post-load maintenance:

```sh
cd /Users/michaelseiler/dev/snissn/JSONBench/treedb
DATA_DIR="$HOME/data/bluesky" SUBSET_ROWS=100000 TRIES=1 \
  DATA_ROOT=compressed COMPACT_AFTER_LOAD=1 \
  ./run_matrix.sh
```

This calls the collection-aware TreeDB `CompactStorage` path after loading and
before query timing. The compaction report records value-log rewrite/GC,
leaf-generation pack/GC, index vacuum, final settle GC, and remaining debt so
the storage column represents the audited post-compaction footprint.
`DATA_ROOT=fast` keeps the requested collection policy as fast/default, while
current cached TreeDB promotes collection data roots with large values to
value-log leaf storage at runtime.

For a quick 1MM proof run on one minimal row-layout cell:

```sh
cd /Users/michaelseiler/dev/snissn/JSONBench/treedb
DATA_DIR="$HOME/data/bluesky" SUBSET_ROWS=1000000 TRIES=1 QUERY_CELLS=q1 ./run_matrix.sh
```

For a 1MM TreeDB column-store proof run, add `STORAGE_LAYOUTS=column-store`.
Column-store cells are query-shaped, so the runner loads one projection/query
per cell:

```sh
cd /Users/michaelseiler/dev/snissn/JSONBench/treedb
DATA_DIR="$HOME/data/bluesky" SUBSET_ROWS=1000000 TRIES=1 \
  STORAGE_LAYOUTS=column-store QUERY_CELLS="q1 q2 q4 q5" \
  ./run_matrix.sh
```

q2/q4/q5 use query-specific sentinel masking during load to match JSONBench
filter semantics because the current physical column reducers do not yet expose
separate filter predicates. The matrix runner skips q3 for `column-store` by
default because q3 currently falls back to a slow materialized scan rather than
a physical aggregate; set `COLUMN_STORE_Q3_FALLBACK=1` only when you explicitly
want that fallback measurement.

## Smoke Run

```sh
cd treedb
DATA_DIR=./testdata/bluesky SUBSET_ROWS=6 TRIES=1 ./run_matrix.sh
```

The checked-in fixture has only 6 rows.

## 1MM and 10MM Run

Download JSONBench data first from the repository root:

```sh
./download_data.sh
```

Then run the TreeDB matrix:

```sh
cd treedb
SCALES="1m 10m" DATA_DIR="$HOME/data/bluesky" ./run_matrix.sh
```

The output directory defaults to `treedb/results/run_<timestamp>`. Primary
artifacts:

- `*/result.json`: one TreeDB cell result
- `report.md`: Markdown matrix report
- `report.json`: machine-readable matrix report

By default the report includes only metrics created in the current TreeDB run.
It does not import checked-in JSONBench baseline files, because those are stale
for local apples-to-apples comparisons. The matrix runner exposes
`DUCKDB_RESULTS_DIR`, `DUCKDB_SCALES`, `CLICKHOUSE_RESULTS_DIR`, and
`CLICKHOUSE_SCALES`, so local DuckDB and ClickHouse results can be used without
hand-running the report command:

```sh
cd /Users/michaelseiler/dev/snissn/JSONBench/duckdb
ROWS=1000000 TRIES=1 ./run_local.sh

cd ../clickhouse
ROWS=1000000 TRIES=1 ./run_local.sh

cd ../treedb
SCALES=1m DATA_DIR="$HOME/data/bluesky" TRIES=1 \
  DUCKDB_RESULTS_DIR="../duckdb/local_results/<duckdb-run-dir>" \
  DUCKDB_SCALES=all \
  CLICKHOUSE_RESULTS_DIR="../clickhouse/local_results/<clickhouse-run-dir>" \
  CLICKHOUSE_SCALES=all \
  ./run_matrix.sh
```

`DUCKDB_RESULTS_DIR=../duckdb/local_results` imports all local DuckDB run
directories below that parent; `CLICKHOUSE_RESULTS_DIR` behaves the same for
ClickHouse.

To intentionally compare against the checked-in upstream DuckDB files, pass
`DUCKDB_RESULTS_DIR=../duckdb/results`. That should not be used for a fresh
local benchmark report.

To explicitly run the known-risk full-document cells:

```sh
cd treedb
SUITE=full DATA_DIR="$HOME/data/bluesky" SUBSET_ROWS=100000 TRIES=1 ./run_matrix.sh
```

## Single Cell

```sh
cd treedb
go run ./cmd/jsonbench_treedb run \
  -data-dir "$HOME/data/bluesky" \
  -db-dir /tmp/jsonbench_treedb_q1 \
  -reset \
  -scale 1m \
  -format template-v1 \
  -projection q1 \
  -queries q1 \
  -out /tmp/jsonbench_treedb_q1.json
```

## Notes

This harness uses the public TreeDB collections API. The default `row` storage
layout remains the row-store/template-v1 baseline. The `column-store` layout is
a separate TreeDB physical-column cell for current reducer coverage and is
intentionally documented with its q2/q4/q5 filter-masking and q3 fallback
limitations.
