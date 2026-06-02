package main

import (
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestNormalizeColumnStorePreparedLayout(t *testing.T) {
	for _, raw := range []string{storageLayoutColumnStorePrepared, "column_store_prepared", "column-store-prepared-scan", "column_store_prepared_scan"} {
		got, err := normalizeStorageLayout(raw)
		if err != nil {
			t.Fatalf("normalizeStorageLayout(%q): %v", raw, err)
		}
		if got != storageLayoutColumnStorePrepared {
			t.Fatalf("normalizeStorageLayout(%q)=%q want %q", raw, got, storageLayoutColumnStorePrepared)
		}
	}
}

func TestNormalizeFullColumnStoreLayouts(t *testing.T) {
	cases := map[string]string{
		storageLayoutColumnStoreFull:         storageLayoutColumnStoreFull,
		"column_store_full":                  storageLayoutColumnStoreFull,
		"column-store-retained":              storageLayoutColumnStoreFull,
		storageLayoutColumnStoreFullPrepared: storageLayoutColumnStoreFullPrepared,
		"column_store_full_prepared":         storageLayoutColumnStoreFullPrepared,
		"column-store-retained-prepared":     storageLayoutColumnStoreFullPrepared,
	}
	for raw, want := range cases {
		got, err := normalizeStorageLayout(raw)
		if err != nil {
			t.Fatalf("normalizeStorageLayout(%q): %v", raw, err)
		}
		if got != want {
			t.Fatalf("normalizeStorageLayout(%q)=%q want %q", raw, got, want)
		}
	}
}

func TestCanonicalJSONPreservesJSONNumbers(t *testing.T) {
	got, err := canonicalJSON([]byte(`{"fraction":1.0,"large":100000000000000000001}`))
	if err != nil {
		t.Fatalf("canonicalJSON: %v", err)
	}
	const want = `{"fraction":1.0,"large":100000000000000000001}`
	if string(got) != want {
		t.Fatalf("canonicalJSON=%s want %s", got, want)
	}
}

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

func TestFullColumnStoreLayoutsMatchFullRowFixture(t *testing.T) {
	row := runJSONBenchFullFixtureCell(t, storageLayoutRow, false)
	want := make(map[string]queryRun, len(row.Queries))
	for _, query := range row.Queries {
		want[query.Name] = query
	}
	for _, layout := range []string{storageLayoutColumnStoreFull, storageLayoutColumnStoreFullPrepared} {
		layout := layout
		t.Run(layout, func(t *testing.T) {
			column := runJSONBenchFullFixtureCell(t, layout, true)
			if !column.RetainsJSON {
				t.Fatalf("%s RetainsJSON=false want true", layout)
			}
			if got, want := column.DataShape, "full-retained-json"; got != want {
				t.Fatalf("%s data_shape=%q want %q", layout, got, want)
			}
			if got, want := column.RetainedPayloadPolicy, string(collections.ColumnRetainedPayloadNonColumn); got != want {
				t.Fatalf("%s retained_payload_policy=%q want %q", layout, got, want)
			}
			if got, want := column.TypedColumnOwner, string(collections.TypedStorageOwnerColumnPart); got != want {
				t.Fatalf("%s typed_column_owner=%q want %q", layout, got, want)
			}
			if column.Reconstruction == nil || !column.Reconstruction.Valid || column.Reconstruction.Rows != column.DatasetSize {
				t.Fatalf("%s reconstruction=%+v want valid rows=%d", layout, column.Reconstruction, column.DatasetSize)
			}
			if got, want := len(column.Queries), len(row.Queries); got != want {
				t.Fatalf("%s queries=%d want %d", layout, got, want)
			}
			for _, query := range column.Queries {
				rowQuery, ok := want[query.Name]
				if !ok {
					t.Fatalf("%s unexpected query %q", layout, query.Name)
				}
				if got, want := query.ResultHash, rowQuery.ResultHash; got != want {
					t.Fatalf("%s %s result hash=%s want row hash=%s", layout, query.Name, got, want)
				}
				if got, want := query.RowsScanned, rowQuery.RowsScanned; got != want {
					t.Fatalf("%s %s rows_scanned=%d want %d", layout, query.Name, got, want)
				}
			}
		})
	}
}

func TestFullColumnStoreLayoutsDeclareNullableStringColumns(t *testing.T) {
	full, err := columnStoreConfigForProjection("q1", storageLayoutColumnStoreFullPrepared)
	if err != nil {
		t.Fatalf("columnStoreConfigForProjection full: %v", err)
	}
	queryShaped, err := columnStoreConfigForProjection("q2", storageLayoutColumnStorePrepared)
	if err != nil {
		t.Fatalf("columnStoreConfigForProjection query-shaped: %v", err)
	}

	for _, col := range full.Columns {
		switch col.ValueType {
		case collections.ColumnStoreValueString:
			if !col.Nullable {
				t.Fatalf("full-data string column %q nullable=false", col.Name)
			}
		case collections.ColumnStoreValueInt64:
			if col.Nullable {
				t.Fatalf("full-data int64 column %q nullable=true", col.Name)
			}
		}
	}
	for _, col := range queryShaped.Columns {
		if col.Nullable {
			t.Fatalf("query-shaped column %q nullable=true", col.Name)
		}
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

func TestColumnStorePreparedLayoutMatchesRowFixture(t *testing.T) {
	for _, query := range []string{"q1", "q2", "q3", "q4", "q5"} {
		query := query
		t.Run(query, func(t *testing.T) {
			row := runJSONBenchFixtureCell(t, storageLayoutRow, query)
			column := runJSONBenchFixtureCell(t, storageLayoutColumnStorePrepared, query)
			if got, want := column.Queries[0].ResultHash, row.Queries[0].ResultHash; got != want {
				t.Fatalf("column-store-prepared result hash=%s want row hash=%s", got, want)
			}
			if got, want := column.Queries[0].RowsScanned, row.Queries[0].RowsScanned; got != want {
				t.Fatalf("column-store-prepared rows_scanned=%d want %d", got, want)
			}
			if (query == "q4" || query == "q5") && column.Queries[0].RowsScanned == 0 {
				t.Fatalf("column-store-prepared %s rows_scanned=0; non-metadata prepared layout must scan base rows", query)
			}
		})
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

func runJSONBenchFullFixtureCell(t *testing.T, layout string, validateReconstruction bool) runResult {
	t.Helper()
	cfg := runConfig{
		DataDir:                "../../testdata/bluesky",
		DBDir:                  t.TempDir(),
		Reset:                  true,
		Scale:                  "subset",
		Rows:                   6,
		MaxFiles:               1,
		Format:                 "json",
		StorageLayout:          layout,
		Projection:             "full",
		Queries:                []string{"q1", "q2", "q3", "q4", "q5"},
		BatchSize:              defaultBatchSize,
		Profile:                "fast",
		DataRoot:               "fast",
		Collection:             defaultCollectionName,
		Checkpoint:             true,
		ValidateReconstruction: validateReconstruction,
		Tries:                  1,
	}
	result, err := runTreeDBBenchmark(cfg)
	if err != nil {
		t.Fatalf("runTreeDBBenchmark(%s, full): %v", layout, err)
	}
	if got := len(result.Queries); got != len(cfg.Queries) {
		t.Fatalf("queries=%d want %d", got, len(cfg.Queries))
	}
	return result
}
