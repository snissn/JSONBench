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

Full-document and `template-v1` cells are available with environment overrides.
The harness opens TreeDB with the cached leaf-log backend so collection data
roots can store oversized documents through persistent value-log pointers.

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

For a quick 1MM proof run on one minimal cell:

```sh
cd /Users/michaelseiler/dev/snissn/JSONBench/treedb
DATA_DIR="$HOME/data/bluesky" SUBSET_ROWS=1000000 TRIES=1 QUERY_CELLS=q1 ./run_matrix.sh
```

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

This harness uses the public TreeDB collections API. It does not add a TreeDB
column-store mode. The minimal projections are intentionally query-specific so
we can compare full-document scans against a direct “only the needed fields were
loaded” shape.

Future TreeDB column-store work should add a separate TreeDB layout/cell here
once `snissn/gomap` has the required command-WAL publish/recovery contract. Until
then, this harness is the row-store/template-v1 baseline and normalized report
surface for column-store planning.
