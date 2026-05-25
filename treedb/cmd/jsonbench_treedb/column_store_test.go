package main

import "testing"

func TestColumnStoreLayoutMatchesRowFixture(t *testing.T) {
	for _, query := range []string{"q1", "q2", "q4", "q5"} {
		query := query
		t.Run(query, func(t *testing.T) {
			row := runJSONBenchFixtureCell(t, storageLayoutRow, query)
			column := runJSONBenchFixtureCell(t, storageLayoutColumnStore, query)
			if got, want := column.Queries[0].ResultHash, row.Queries[0].ResultHash; got != want {
				t.Fatalf("column-store result hash=%s want row hash=%s", got, want)
			}
			if got, want := column.Queries[0].RowsScanned, row.Queries[0].RowsScanned; got != want {
				t.Fatalf("column-store rows_scanned=%d want %d", got, want)
			}
		})
	}
}

func TestColumnStorePreparedMetadataLayoutMatchesRowFixture(t *testing.T) {
	for _, query := range []string{"q1", "q2", "q4", "q5"} {
		query := query
		t.Run(query, func(t *testing.T) {
			row := runJSONBenchFixtureCell(t, storageLayoutRow, query)
			column := runJSONBenchFixtureCell(t, storageLayoutColumnStorePreparedMetadata, query)
			if got, want := column.Queries[0].ResultHash, row.Queries[0].ResultHash; got != want {
				t.Fatalf("column-store-prepared-metadata result hash=%s want row hash=%s", got, want)
			}
			if query == "q4" || query == "q5" {
				if got := column.Queries[0].RowsScanned; got != 0 {
					t.Fatalf("column-store-prepared-metadata rows_scanned=%d want 0 for aggregate metadata", got)
				}
				return
			}
			if got, want := column.Queries[0].RowsScanned, row.Queries[0].RowsScanned; got != want {
				t.Fatalf("column-store-prepared-metadata rows_scanned=%d want %d", got, want)
			}
		})
	}
}

func runJSONBenchFixtureCell(t *testing.T, layout, query string) runResult {
	t.Helper()
	cfg := runConfig{
		DataDir:       "../../testdata/bluesky",
		DBDir:         t.TempDir(),
		Reset:         true,
		Scale:         "subset",
		Rows:          6,
		MaxFiles:      1,
		Format:        "json",
		StorageLayout: layout,
		Projection:    query,
		Queries:       []string{query},
		BatchSize:     defaultBatchSize,
		Profile:       "fast",
		DataRoot:      "fast",
		Collection:    defaultCollectionName,
		Checkpoint:    true,
		Tries:         1,
	}
	result, err := runTreeDBBenchmark(cfg)
	if err != nil {
		t.Fatalf("runTreeDBBenchmark(%s, %s): %v", layout, query, err)
	}
	if got := len(result.Queries); got != 1 {
		t.Fatalf("queries=%d want 1", got)
	}
	return result
}
