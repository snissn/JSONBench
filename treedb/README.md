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

This harness requires Go 1.25.0 or newer, matching the TreeDB module version
used by the current `github.com/snissn/gomap` dependency. Download Go toolchains
from https://go.dev/dl.

The default TreeDB matrix is the strict minimal JSON suite:

- scale: `subset`, `1m`, `10m` (`100m`/`1000m` are accepted but intentionally
  not the default)
- collection format: `json`
- projection/query cells: `q1` through `q5`

`full` preserves the input JSON document. Query-specific projections store only
the fields needed by that query plus the TreeDB primary key. For example, `q1`
stores only `event = commit.collection`.

Full-document and `template-v1` row-layout cells are available with environment
overrides. Column-store storage layouts are also available for query-shaped
`json` projection cells. They store declared projection fields in TreeDB
physical column row assets with `retained_payload=none` and use physical column
reducers for q1 through q5. The harness opens TreeDB with the cached leaf-log
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

For the default TreeDB column-store JSONBench rerun/table, use:

```sh
cd treedb
./run_columnstore_benchmark.sh
```

By default this runs 1MM rows and 3 tries for q1..q5 direct and prepared-scan
column-store layouts, plus prepared-metadata q4/q5 where aggregate metadata is
applicable. It writes the full matrix report plus the compact table used in
TreeDB benchmark updates:

- `report.md`
- `report.json`
- `columnstore_summary.md`

Useful overrides:

```sh
# smoke run over the checked-in fixture
DATA_DIR=./testdata/bluesky ROWS=6 TRIES=1 ./run_columnstore_benchmark.sh

# test against a local gomap checkout without permanently editing go.mod
GOMAP_REPLACE=/path/to/gomap ./run_columnstore_benchmark.sh

# 10MM column-store rerun; ROWS automatically selects the 10m scale/input files
ROWS=10000000 GOMAP_REPLACE=/path/to/gomap ./run_columnstore_benchmark.sh
```

For the preferred 10MM TreeDB-vs-ClickHouse experiment, use the single-entry
comparison script:

```sh
cd treedb
ROWS=10000000 TRIES=3 GOMAP_REPLACE=/path/to/gomap \
  ./run_preferred_columnstore_clickhouse_compare.sh
```

This runs a full-data TreeDB storage headline with `column-store-full-prepared`
(`typed_column_part` hot-path columns plus retained non-column JSON), then runs
the server-shaped query attribution rows (q1/q2/q3 prepared physical runners,
q4/q5 aggregate-metadata Top-K). It loads ClickHouse through `clickhouse local`
and writes `preferred_summary.md` alongside the TreeDB and ClickHouse result
JSON. Set `RUN_CLICKHOUSE=0` or `RUN_TREEDB=0` to reuse an existing half of a
run.

Useful preferred-run overrides:

```sh
# compact full-data TreeDB storage before the storage headline is measured
TREEDB_COMPACT_AFTER_LOAD=1 ./run_preferred_columnstore_clickhouse_compare.sh

# run a smoke TreeDB preferred report without ClickHouse
DATA_DIR=./testdata/bluesky ROWS=6 TRIES=1 RUN_CLICKHOUSE=0 \
  TREEDB_VALIDATE_RECONSTRUCTION=1 \
  CLICKHOUSE_RESULT=/path/to/existing/clickhouse/result.json \
  ./run_preferred_columnstore_clickhouse_compare.sh
```

The lower-level matrix runner is still available for custom cells. Column-store
minimal cells are query-shaped, so the runner loads one projection/query per
cell:

```sh
cd treedb
DATA_DIR="$HOME/data/bluesky" SUBSET_ROWS=1000000 TRIES=1 \
  STORAGE_LAYOUTS="column-store column-store-prepared column-store-prepared-metadata" \
  QUERY_CELLS="q1 q2 q3 q4 q5" \
  ./run_matrix.sh
```

Full-data TreeDB column-store rows use the explicit full layouts:

```sh
cd treedb
DATA_DIR="$HOME/data/bluesky" SUBSET_ROWS=100000 TRIES=1 \
  STORAGE_LAYOUTS="column-store-full column-store-full-prepared" \
  SUITE=full \
  VALIDATE_RECONSTRUCTION=1 \
  ./run_matrix.sh
```

Column-store execution modes are explicit:

- `column-store`: direct one-shot physical query API.
- `column-store-prepared`: prepared physical query runners outside timed
  attempts, with no aggregate metadata; q4/q5 scan base column rows.
- `column-store-prepared-metadata`: prepared physical query runners; only q4/q5
  declare `min_time_us` aggregate metadata with Top-K and answer with
  `rows_scanned=0`.
- `column-store-full`: full retained JSON cell with declared hot paths owned by
  `typed_column_part`; direct physical query API.
- `column-store-full-prepared`: full retained JSON cell with declared hot paths
  owned by `typed_column_part`; prepared physical query runners.

q1/q2/q3 prepared rows are scan-mode rows, not metadata rows. q3 uses TreeDB's
physical grouped-hour reducer over dictionary and int64 column sidecars. q4/q5
direct and prepared-scan cells use physical dictionary predicates; q4/q5
prepared aggregate-metadata cells still use query-specific sentinel masking
during load so the aggregate metadata represents the filtered JSONBench post
rows. q2 remains on the existing sentinel-masked count/distinct path. The
matrix runner includes q3 for column-store layouts by default, and the compact
`column-summary` table includes an execution-mode column.

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
a separate TreeDB physical-column direct cell, `column-store-prepared` measures
prepared physical runners without aggregate metadata, and
`column-store-prepared-metadata` adds q4/q5 Top-K aggregate metadata.
`column-store-full` and `column-store-full-prepared` are the full-data storage
comparison cells; they retain non-column JSON and declare hot paths in
`typed_column_part` assets. Query-shaped column-store layouts are intentionally
documented with their q2 and q4/q5 metadata filter-masking behavior and should
not be used as ClickHouse storage headlines.
