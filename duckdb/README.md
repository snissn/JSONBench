# Local DuckDB JSONBench Runner

The upstream DuckDB scripts are meant for the published JSONBench environment.
`run_local.sh` is the local harness: it loads exactly the requested row count
from `~/data/bluesky`, runs the five JSONBench queries, and writes a
JSONBench-compatible `result.json`.

Install DuckDB once:

```sh
brew install duckdb
```

Or let the harness install it when no CLI is found:

```sh
INSTALL_DUCKDB=1 ROWS=100000 TRIES=1 ./run_local.sh
```

Common runs:

```sh
ROWS=100000 TRIES=1 ./run_local.sh
ROWS=1000000 TRIES=1 ./run_local.sh
```

Outputs are written under `duckdb/local_results/run_*`. The runner refuses to
produce partial results: if `ROWS=1000000`, it must load exactly 1,000,000 rows.

To compare with TreeDB in the generated report:

```sh
cd ../treedb
SCALES=1m DATA_DIR="$HOME/data/bluesky" TRIES=1 \
  DUCKDB_RESULTS_DIR="../duckdb/local_results" \
  DUCKDB_SCALES=all \
  ./run_matrix.sh
```
