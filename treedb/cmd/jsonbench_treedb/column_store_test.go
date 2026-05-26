package main

import (
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestColumnStoreLayoutMatchesRowFixture(t *testing.T) {
	for _, query := range []string{"q1", "q2", "q3", "q4", "q5"} {
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

func TestColumnStoreQ1KeepsEmptyEventBucket(t *testing.T) {
	computed := renderColumnQ1(3, collections.ColumnPhysicalQueryResult{Groups: []collections.ColumnPhysicalQueryGroup{
		{Key: "app.bsky.feed.post", Count: 2},
		{Key: "", Count: 1},
	}})
	want := []queryRow{
		{"event": "app.bsky.feed.post", "count": int64(2)},
		{"event": "", "count": int64(1)},
	}
	if !reflect.DeepEqual(computed.Rows, want) {
		t.Fatalf("q1 rows=%v want %v", computed.Rows, want)
	}
}

func TestColumnStorePreparedMetadataLayoutMatchesRowFixture(t *testing.T) {
	// The checked-in JSONBench fixture is intentionally only six rows, so this is
	// a semantic smoke test; the full top-N ordering path is covered by local
	// benchmark runs over larger downloaded JSONBench datasets.
	for _, query := range []string{"q1", "q2", "q3", "q4", "q5"} {
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

func TestColumnStoreTopRowsTieBreak(t *testing.T) {
	var q4 []queryRow
	for _, row := range []queryRow{
		{"user_id": "did:d", "first_post_time_us": int64(5)},
		{"user_id": "did:b", "first_post_time_us": int64(5)},
		{"user_id": "did:a", "first_post_time_us": int64(5)},
		{"user_id": "did:c", "first_post_time_us": int64(4)},
		{"user_id": "did:e", "first_post_time_us": int64(6)},
	} {
		insertTopQueryRow(&q4, row, 3, lessQ4Row)
	}
	wantQ4 := []queryRow{
		{"user_id": "did:c", "first_post_time_us": int64(4)},
		{"user_id": "did:a", "first_post_time_us": int64(5)},
		{"user_id": "did:b", "first_post_time_us": int64(5)},
	}
	if !reflect.DeepEqual(q4, wantQ4) {
		t.Fatalf("q4 top rows=%v want %v", q4, wantQ4)
	}

	var q5 []queryRow
	for _, row := range []queryRow{
		{"user_id": "did:d", "activity_span_ms": int64(9)},
		{"user_id": "did:b", "activity_span_ms": int64(10)},
		{"user_id": "did:a", "activity_span_ms": int64(10)},
		{"user_id": "did:c", "activity_span_ms": int64(8)},
		{"user_id": "did:e", "activity_span_ms": int64(7)},
	} {
		insertTopQueryRow(&q5, row, 3, lessQ5Row)
	}
	wantQ5 := []queryRow{
		{"user_id": "did:a", "activity_span_ms": int64(10)},
		{"user_id": "did:b", "activity_span_ms": int64(10)},
		{"user_id": "did:d", "activity_span_ms": int64(9)},
	}
	if !reflect.DeepEqual(q5, wantQ5) {
		t.Fatalf("q5 top rows=%v want %v", q5, wantQ5)
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
