# Local ClickHouse JSONBench Runner

`run_local.sh` loads exactly the requested row count into a fresh ClickHouse
database, runs the five JSONBench queries, and writes a JSONBench-compatible
`result.json`.

Install ClickHouse CLI and a local version first:

```sh
curl https://clickhouse.com/cli | sh
clickhousectl local install stable
```

Start a local server in another terminal if needed:

```sh
clickhousectl local server start --name jsonbench-local --foreground
```

Then run:

```sh
ROWS=1000000 TRIES=1 ./run_local.sh
```

Outputs are written under `clickhouse/local_results/run_*`. The runner refuses
partial data: if `ROWS=1000000`, it must load exactly 1,000,000 rows.
