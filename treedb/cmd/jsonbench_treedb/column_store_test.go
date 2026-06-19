package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
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
			assertRowScanQueryDiagnostics(t, row.Queries[0])
			assertColumnPhysicalQueryDiagnostics(t, column.Queries[0], expectedPhysicalQueryCount(query))
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
				if layout == storageLayoutColumnStoreFullPrepared && (query.Name == "q4" || query.Name == "q5") {
					if query.RowsScanned <= 0 || query.RowsScanned > rowQuery.RowsScanned {
						t.Fatalf("%s %s rows_scanned=%d want within 1..%d", layout, query.Name, query.RowsScanned, rowQuery.RowsScanned)
					}
					assertColumnPhysicalQueryDiagnostics(t, query, expectedPhysicalQueryCount(query.Name))
					assertFullPreparedTopKDiagnostics(t, query)
					continue
				}
				if got, want := query.RowsScanned, rowQuery.RowsScanned; got != want {
					t.Fatalf("%s %s rows_scanned=%d want %d", layout, query.Name, got, want)
				}
				assertColumnPhysicalQueryDiagnostics(t, query, expectedPhysicalQueryCount(query.Name))
			}
		})
	}
}

func TestColumnPhysicalRequestBoundedTopKLayouts(t *testing.T) {
	cases := []struct {
		name         string
		layout       string
		query        string
		wantTopK     int
		wantOrder    collections.ColumnPhysicalQueryTopKOrder
		wantMetadata bool
	}{
		{name: "full prepared q4", layout: storageLayoutColumnStoreFullPrepared, query: "q4", wantTopK: 3, wantOrder: collections.ColumnPhysicalQueryTopKInt64Asc},
		{name: "full prepared q5", layout: storageLayoutColumnStoreFullPrepared, query: "q5", wantTopK: 3, wantOrder: collections.ColumnPhysicalQueryTopKInt64Desc},
		{name: "metadata q4", layout: storageLayoutColumnStorePreparedMetadata, query: "q4", wantTopK: 3, wantOrder: collections.ColumnPhysicalQueryTopKInt64Asc, wantMetadata: true},
		{name: "metadata q5", layout: storageLayoutColumnStorePreparedMetadata, query: "q5", wantTopK: 3, wantOrder: collections.ColumnPhysicalQueryTopKInt64Desc, wantMetadata: true},
		{name: "query shaped prepared q4", layout: storageLayoutColumnStorePrepared, query: "q4"},
		{name: "full direct q5", layout: storageLayoutColumnStoreFull, query: "q5"},
		{name: "full prepared q3", layout: storageLayoutColumnStoreFullPrepared, query: "q3"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			req := testColumnPhysicalRequestForQuery(tc.layout, tc.query)
			if got := req.TopK; got != tc.wantTopK {
				t.Fatalf("topk=%d want %d req=%+v", got, tc.wantTopK, req)
			}
			if got := req.TopKOrder; got != tc.wantOrder {
				t.Fatalf("topk_order=%q want %q req=%+v", got, tc.wantOrder, req)
			}
			if got := req.SkipEmptyGroupKey; got != (tc.wantTopK > 0) {
				t.Fatalf("skip_empty_group_key=%v want %v req=%+v", got, tc.wantTopK > 0, req)
			}
			if got := req.AggregateMetadataName != ""; got != tc.wantMetadata {
				t.Fatalf("aggregate metadata present=%v want %v req=%+v", got, tc.wantMetadata, req)
			}
			if tc.query == "q4" || tc.query == "q5" {
				wantPredicates := !tc.wantMetadata
				if got := len(req.Predicates) > 0; got != wantPredicates {
					t.Fatalf("predicates present=%v want %v req=%+v", got, wantPredicates, req)
				}
			}
		})
	}
}

func TestFullColumnStoreLayoutsDeclareNullableStringColumns(t *testing.T) {
	full, err := columnStoreConfigForProjection("q1", storageLayoutColumnStoreFullPrepared, "")
	if err != nil {
		t.Fatalf("columnStoreConfigForProjection full: %v", err)
	}
	queryShaped, err := columnStoreConfigForProjection("q2", storageLayoutColumnStorePrepared, "")
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

func TestFullColumnStoreRetainedPayloadEncodingOverride(t *testing.T) {
	cfg, err := columnStoreConfigForProjection("full", storageLayoutColumnStoreFullPrepared, "semantic-stream-v1")
	if err != nil {
		t.Fatalf("columnStoreConfigForProjection semantic-stream-v1: %v", err)
	}
	if got, want := cfg.RetainedPayloadEncoding, collections.ColumnRetainedPayloadEncoding(collections.ColumnRetainedPayloadEncodingSemanticStreamV1); got != want {
		t.Fatalf("retained payload encoding=%q want %q", got, want)
	}
	encoding, status := collections.ColumnRetainedPayloadEncodingStatus(cfg)
	if got, want := encoding, string(collections.ColumnRetainedPayloadEncodingSemanticStreamV1); got != want {
		t.Fatalf("retained payload encoding status encoding=%q want %q", got, want)
	}
	if got, want := status, "active_semantic_stream_v1_non_column_retained_payload"; got != want {
		t.Fatalf("retained payload encoding status=%q want %q", got, want)
	}

	queryShaped, err := columnStoreConfigForProjection("q2", storageLayoutColumnStorePrepared, "semantic-stream-v1")
	if err != nil {
		t.Fatalf("columnStoreConfigForProjection query-shaped: %v", err)
	}
	if queryShaped.RetainedPayloadEncoding != "" {
		t.Fatalf("query-shaped retained payload encoding=%q want empty", queryShaped.RetainedPayloadEncoding)
	}
}

func TestColumnStoreRejectsUnknownRetainedPayloadEncodingOverride(t *testing.T) {
	for _, layout := range []string{storageLayoutColumnStoreFullPrepared, storageLayoutColumnStorePrepared} {
		layout := layout
		t.Run(layout, func(t *testing.T) {
			_, err := columnStoreConfigForProjection("q1", layout, "bogus-encoding")
			if err == nil {
				t.Fatalf("columnStoreConfigForProjection(%s) error=nil want unsupported encoding error", layout)
			}
			if !strings.Contains(err.Error(), "unsupported retained payload encoding") {
				t.Fatalf("columnStoreConfigForProjection(%s) error=%q want unsupported retained payload encoding", layout, err)
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
	if got, want := computed.Diagnostics.QueryPath, "column_physical"; got != want {
		t.Fatalf("q1 diagnostics query path=%q want %q", got, want)
	}
	if got, want := len(computed.Diagnostics.PhysicalQueries), 1; got != want {
		t.Fatalf("q1 physical query diagnostics=%d want %d", got, want)
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
			assertColumnPhysicalQueryDiagnostics(t, column.Queries[0], expectedPhysicalQueryCount(query))
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
				assertColumnPhysicalQueryDiagnostics(t, column.Queries[0], 1)
				if got := column.Queries[0].Diagnostics.TopKLimit; got != 3 {
					t.Fatalf("column-store-prepared-metadata %s topk_limit=%d want 3", query, got)
				}
				if got := column.Queries[0].Diagnostics.TopKCandidates; got == 0 {
					t.Fatalf("column-store-prepared-metadata %s topk_candidates=0 diagnostics=%+v", query, column.Queries[0].Diagnostics)
				}
				return
			}
			if got, want := column.Queries[0].RowsScanned, row.Queries[0].RowsScanned; got != want {
				t.Fatalf("column-store-prepared-metadata rows_scanned=%d want %d", got, want)
			}
			assertColumnPhysicalQueryDiagnostics(t, column.Queries[0], expectedPhysicalQueryCount(query))
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

func TestQueryAttemptProfilesWriteArtifacts(t *testing.T) {
	profileDir := t.TempDir()
	cfg := runFixtureConfig(storageLayoutRow, "q1")
	cfg.QueryProfileDir = filepath.Join(profileDir, "profiles")
	result, err := runTreeDBBenchmark(cfg)
	if err != nil {
		t.Fatalf("runTreeDBBenchmark with query profile dir: %v", err)
	}
	if result.QueryProfileDir == "" {
		t.Fatalf("query_profile_dir is empty")
	}
	if got, want := len(result.Queries), 1; got != want {
		t.Fatalf("queries=%d want %d", got, want)
	}
	query := result.Queries[0]
	if got, want := len(query.Profiles), 1; got != want {
		t.Fatalf("attempt profiles=%d want %d", got, want)
	}
	profile := query.Profiles[0]
	for _, path := range []string{profile.CPUProfile, profile.AllocsProfile} {
		if path == "" {
			t.Fatalf("profile path is empty: %+v", profile)
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat profile %s: %v", path, err)
		}
		if info.Size() == 0 {
			t.Fatalf("profile %s is empty", path)
		}
	}
	if query.Diagnostics.AttemptWallNanos <= 0 {
		t.Fatalf("attempt wall nanos=%d want >0", query.Diagnostics.AttemptWallNanos)
	}
	assertRowScanQueryDiagnostics(t, query)
}

func runFixtureConfig(layout, query string) runConfig {
	return runConfig{
		DataDir:       "../../testdata/bluesky",
		DBDir:         "",
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
}

func runJSONBenchFixtureCell(t *testing.T, layout, query string) runResult {
	t.Helper()
	cfg := runFixtureConfig(layout, query)
	cfg.DBDir = t.TempDir()
	result, err := runTreeDBBenchmark(cfg)
	if err != nil {
		t.Fatalf("runTreeDBBenchmark(%s, %s): %v", layout, query, err)
	}
	if got := len(result.Queries); got != 1 {
		t.Fatalf("queries=%d want 1", got)
	}
	return result
}

func assertRowScanQueryDiagnostics(t *testing.T, query queryRun) {
	t.Helper()
	if got, want := query.Diagnostics.QueryPath, "document_row_scan"; got != want {
		t.Fatalf("%s query path=%q want %q diagnostics=%+v", query.Name, got, want, query.Diagnostics)
	}
	if got, want := query.Diagnostics.RowsScanned, query.RowsScanned; got != want {
		t.Fatalf("%s diagnostic rows_scanned=%d want %d", query.Name, got, want)
	}
	if query.Diagnostics.RowsMatched == 0 {
		t.Fatalf("%s diagnostic rows_matched=0 diagnostics=%+v", query.Name, query.Diagnostics)
	}
	if query.Diagnostics.RowMaterializations != query.RowsScanned || query.Diagnostics.DocumentMaterializations != query.RowsScanned {
		t.Fatalf("%s materialization diagnostics=%+v rows_scanned=%d", query.Name, query.Diagnostics, query.RowsScanned)
	}
}

func assertColumnPhysicalQueryDiagnostics(t *testing.T, query queryRun, wantPhysicalQueries int) {
	t.Helper()
	if got, want := query.Diagnostics.QueryPath, "column_physical"; got != want {
		t.Fatalf("%s query path=%q want %q diagnostics=%+v", query.Name, got, want, query.Diagnostics)
	}
	if got, want := len(query.Diagnostics.PhysicalQueries), wantPhysicalQueries; got != want {
		t.Fatalf("%s physical query diagnostics=%d want %d: %+v", query.Name, got, want, query.Diagnostics.PhysicalQueries)
	}
	if query.Diagnostics.StorageSource == "" {
		t.Fatalf("%s storage source is empty: %+v", query.Name, query.Diagnostics)
	}
	if query.Diagnostics.FallbackReason == "" {
		t.Fatalf("%s fallback reason is empty: %+v", query.Name, query.Diagnostics)
	}
	if query.Diagnostics.ResultRows != query.ResultRows || query.Diagnostics.ResultGroups == 0 {
		t.Fatalf("%s result diagnostics=%+v result_rows=%d", query.Name, query.Diagnostics, query.ResultRows)
	}
}

func expectedPhysicalQueryCount(query string) int {
	if query == "q2" {
		return 2
	}
	return 1
}

func testColumnPhysicalRequestForQuery(layout, query string) collections.ColumnPhysicalQueryRequest {
	cfg := runConfig{StorageLayout: layout}
	switch query {
	case "q1":
		return columnPhysicalRequest(cfg, "q1", collections.ColumnPhysicalQueryGroupCount, "event", "", "")
	case "q2":
		return columnPhysicalRequest(cfg, "q2", collections.ColumnPhysicalQueryGroupCountDistinct, "event", "", "did")
	case "q3":
		return columnPhysicalRequest(cfg, "q3", collections.ColumnPhysicalQueryGroupHourCount, "event", "time_us", "")
	case "q4":
		return columnPhysicalRequest(cfg, "q4", collections.ColumnPhysicalQueryGroupMinInt64, "did", "time_us", "")
	case "q5":
		return columnPhysicalRequest(cfg, "q5", collections.ColumnPhysicalQueryGroupInt64Span, "did", "time_us", "")
	default:
		return columnPhysicalRequest(cfg, query, collections.ColumnPhysicalQueryGroupCount, "event", "", "")
	}
}

func assertFullPreparedTopKDiagnostics(t *testing.T, query queryRun) {
	t.Helper()
	if got := query.Diagnostics.TopKLimit; got != 3 {
		t.Fatalf("%s topk_limit=%d want 3 diagnostics=%+v", query.Name, got, query.Diagnostics)
	}
	wantOrder := string(collections.ColumnPhysicalQueryTopKInt64Asc)
	if query.Name == "q5" {
		wantOrder = string(collections.ColumnPhysicalQueryTopKInt64Desc)
	}
	if got := query.Diagnostics.TopKOrder; got != wantOrder {
		t.Fatalf("%s topk_order=%q want %q diagnostics=%+v", query.Name, got, wantOrder, query.Diagnostics)
	}
	if got := query.Diagnostics.TopKCandidates; got == 0 {
		t.Fatalf("%s topk_candidates=0 diagnostics=%+v", query.Name, query.Diagnostics)
	}
	if query.Name == "q4" && !query.Diagnostics.TimeOrderTopKUsed {
		t.Fatalf("q4 time_order_topk_used=false diagnostics=%+v", query.Diagnostics)
	}
	if query.Name == "q5" && !query.Diagnostics.BoundedTopKUsed {
		t.Fatalf("q5 bounded_topk_used=false diagnostics=%+v", query.Diagnostics)
	}
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
