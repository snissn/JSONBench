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

func TestNormalizeQueryAndMetadataModes(t *testing.T) {
	queryMode, err := normalizeQueryMode("one-shot")
	if err != nil {
		t.Fatalf("normalizeQueryMode one-shot: %v", err)
	}
	if queryMode != queryModeOneShotEndToEnd {
		t.Fatalf("query mode=%q want %q", queryMode, queryModeOneShotEndToEnd)
	}
	queryMode, err = normalizeQueryMode("hot-prepared")
	if err != nil {
		t.Fatalf("normalizeQueryMode hot-prepared: %v", err)
	}
	if queryMode != queryModeHotPreparedRun {
		t.Fatalf("query mode=%q want %q", queryMode, queryModeHotPreparedRun)
	}
	metadataMode, err := normalizeMetadataMode("no-aggmeta")
	if err != nil {
		t.Fatalf("normalizeMetadataMode no-aggmeta: %v", err)
	}
	if metadataMode != metadataModeNoAggregateMetadata {
		t.Fatalf("metadata mode=%q want %q", metadataMode, metadataModeNoAggregateMetadata)
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

func TestSecondOfDaySquareFromUnixMicrosFloorsNegativeMicros(t *testing.T) {
	tests := []struct {
		timeUS int64
		want   int64
	}{
		{timeUS: -1, want: 86_399 * 86_399},
		{timeUS: -999_999, want: 86_399 * 86_399},
		{timeUS: -1_000_000, want: 86_399 * 86_399},
		{timeUS: -1_000_001, want: 86_398 * 86_398},
		{timeUS: 0, want: 0},
		{timeUS: 1_000_000, want: 1},
	}
	for _, tt := range tests {
		if got := secondOfDaySquareFromUnixMicros(tt.timeUS); got != tt.want {
			t.Fatalf("secondOfDaySquareFromUnixMicros(%d)=%d want %d", tt.timeUS, got, tt.want)
		}
	}
}

func TestColumnStoreLayoutMatchesRowFixture(t *testing.T) {
	for _, query := range jsonBenchQueryNames {
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
			if query == "qexpr" {
				assertTypedInt64AggregateQueryDiagnostics(t, column.Queries[0])
			} else {
				assertColumnPhysicalQueryDiagnostics(t, column.Queries[0], expectedPhysicalQueryCount(query))
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
				if layout == storageLayoutColumnStoreFullPrepared && columnStoreUsesAggregateMetadata(layout, query.Name) {
					if got := query.RowsScanned; got != 0 {
						t.Fatalf("%s %s rows_scanned=%d want 0 for aggregate metadata", layout, query.Name, got)
					}
					assertColumnPhysicalQueryDiagnostics(t, query, expectedPhysicalQueryCount(query.Name))
					if query.Name == "q5" {
						assertAggregateMetadataTopKDiagnostics(t, query)
					}
					continue
				}
				if layout == storageLayoutColumnStoreFullPrepared && isQ4FamilyQuery(query.Name) {
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
				if query.Name == "qexpr" {
					assertTypedInt64AggregateQueryDiagnostics(t, query)
				} else {
					assertColumnPhysicalQueryDiagnostics(t, query, expectedPhysicalQueryCount(query.Name))
				}
			}
		})
	}
}

func TestColumnPhysicalRequestBoundedTopKLayouts(t *testing.T) {
	cases := []struct {
		name          string
		layout        string
		query         string
		wantTopK      int
		wantOrder     collections.ColumnPhysicalQueryTopKOrder
		wantMetadata  string
		wantPredicate bool
	}{
		{name: "full prepared q1", layout: storageLayoutColumnStoreFullPrepared, query: "q1", wantMetadata: columnStoreQ1AggregateMetadataName},
		{name: "full prepared q3", layout: storageLayoutColumnStoreFullPrepared, query: "q3", wantMetadata: columnStoreQ3AggregateMetadataName, wantPredicate: true},
		{name: "full prepared q4", layout: storageLayoutColumnStoreFullPrepared, query: "q4", wantTopK: 3, wantOrder: collections.ColumnPhysicalQueryTopKInt64Asc, wantPredicate: true},
		{name: "full prepared q4a", layout: storageLayoutColumnStoreFullPrepared, query: "q4a", wantTopK: 3, wantOrder: collections.ColumnPhysicalQueryTopKInt64Asc, wantPredicate: true},
		{name: "full prepared q4b", layout: storageLayoutColumnStoreFullPrepared, query: "q4b", wantTopK: 3, wantOrder: collections.ColumnPhysicalQueryTopKInt64Asc, wantPredicate: true},
		{name: "full prepared q5", layout: storageLayoutColumnStoreFullPrepared, query: "q5", wantTopK: 3, wantOrder: collections.ColumnPhysicalQueryTopKInt64Desc, wantMetadata: columnStoreQ5AggregateMetadataName, wantPredicate: true},
		{name: "metadata q4", layout: storageLayoutColumnStorePreparedMetadata, query: "q4", wantTopK: 3, wantOrder: collections.ColumnPhysicalQueryTopKInt64Asc, wantMetadata: columnStoreTopKAggregateMetadataName},
		{name: "metadata q4a", layout: storageLayoutColumnStorePreparedMetadata, query: "q4a", wantTopK: 3, wantOrder: collections.ColumnPhysicalQueryTopKInt64Asc, wantMetadata: columnStoreTopKAggregateMetadataName},
		{name: "metadata q4b", layout: storageLayoutColumnStorePreparedMetadata, query: "q4b", wantTopK: 3, wantOrder: collections.ColumnPhysicalQueryTopKInt64Asc, wantMetadata: columnStoreTopKAggregateMetadataName},
		{name: "metadata q5", layout: storageLayoutColumnStorePreparedMetadata, query: "q5", wantTopK: 3, wantOrder: collections.ColumnPhysicalQueryTopKInt64Desc, wantMetadata: columnStoreTopKAggregateMetadataName},
		{name: "query shaped prepared q4", layout: storageLayoutColumnStorePrepared, query: "q4", wantPredicate: true},
		{name: "query shaped prepared q4a", layout: storageLayoutColumnStorePrepared, query: "q4a", wantPredicate: true},
		{name: "query shaped prepared q4b", layout: storageLayoutColumnStorePrepared, query: "q4b", wantPredicate: true},
		{name: "full direct q5", layout: storageLayoutColumnStoreFull, query: "q5", wantPredicate: true},
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
			if got := req.AggregateMetadataName; got != tc.wantMetadata {
				t.Fatalf("aggregate metadata name=%q want %q req=%+v", got, tc.wantMetadata, req)
			}
			if got := len(req.Predicates) > 0; got != tc.wantPredicate {
				t.Fatalf("predicates present=%v want %v req=%+v", got, tc.wantPredicate, req)
			}
		})
	}
}

func TestColumnPhysicalRequestNoAggregateMetadataLane(t *testing.T) {
	cfg := runConfig{StorageLayout: storageLayoutColumnStoreFullPrepared, MetadataMode: metadataModeNoAggregateMetadata}
	q1 := columnPhysicalRequest(cfg, "q1", collections.ColumnPhysicalQueryGroupCount, "event", "", "")
	if q1.AggregateMetadataName != "" {
		t.Fatalf("q1 aggregate metadata=%q want empty", q1.AggregateMetadataName)
	}
	q5 := columnPhysicalRequest(cfg, "q5", collections.ColumnPhysicalQueryGroupInt64Span, "did", "time_us", "")
	if q5.AggregateMetadataName != "" {
		t.Fatalf("q5 aggregate metadata=%q want empty", q5.AggregateMetadataName)
	}
	if got := q5.TopK; got != 3 {
		t.Fatalf("q5 topk=%d want 3", got)
	}
	if got := len(q5.Predicates); got == 0 {
		t.Fatalf("q5 predicates empty; no-metadata full-data lane must still push predicates")
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
	if got, want := len(full.AggregateMetadata), 3; got != want {
		t.Fatalf("full-data aggregate metadata count=%d want %d: %+v", got, want, full.AggregateMetadata)
	}
}

func TestColumnStoreQExprProjectionUsesColumnPart(t *testing.T) {
	cfg, err := columnStoreConfigForProjection("qexpr", storageLayoutColumnStorePrepared, "")
	if err != nil {
		t.Fatalf("columnStoreConfigForProjection qexpr: %v", err)
	}
	if got, want := len(cfg.Columns), 1; got != want {
		t.Fatalf("qexpr columns=%d want %d: %+v", got, want, cfg.Columns)
	}
	col := cfg.Columns[0]
	if got, want := col.Name, "time_us"; got != want {
		t.Fatalf("qexpr column name=%q want %q", got, want)
	}
	if got, want := col.Owner, collections.TypedStorageOwnerColumnPart; got != want {
		t.Fatalf("qexpr column owner=%q want %q", got, want)
	}
	if got, want := col.ValueType, collections.ColumnStoreValueInt64; got != want {
		t.Fatalf("qexpr column value type=%q want %q", got, want)
	}
}

func TestColumnStoreQExprRunReportsColumnPartOwner(t *testing.T) {
	result := runJSONBenchFixtureCell(t, storageLayoutColumnStorePrepared, "qexpr")
	if got, want := result.TypedColumnOwner, string(collections.TypedStorageOwnerColumnPart); got != want {
		t.Fatalf("qexpr typed_column_owner=%q want %q", got, want)
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

func TestColumnStoreQ2UsesFusedCountDistinctRequest(t *testing.T) {
	for _, layout := range []string{
		storageLayoutColumnStore,
		storageLayoutColumnStorePrepared,
		storageLayoutColumnStorePreparedMetadata,
		storageLayoutColumnStoreFull,
		storageLayoutColumnStoreFullPrepared,
	} {
		layout := layout
		t.Run(layout, func(t *testing.T) {
			req := testColumnPhysicalRequestForQuery(layout, "q2")
			if got, want := req.Kind, collections.ColumnPhysicalQueryGroupCountAndDistinct; got != want {
				t.Fatalf("q2 kind=%q want %q req=%+v", got, want, req)
			}
			if got, want := req.GroupColumn, "event"; got != want {
				t.Fatalf("q2 group column=%q want %q req=%+v", got, want, req)
			}
			if got, want := req.DistinctColumn, "did"; got != want {
				t.Fatalf("q2 distinct column=%q want %q req=%+v", got, want, req)
			}
			if req.ValueColumn != "" {
				t.Fatalf("q2 value column=%q want empty req=%+v", req.ValueColumn, req)
			}
			wantPredicates := isFullDataColumnStoreLayout(layout)
			if got := len(req.Predicates) > 0; got != wantPredicates {
				t.Fatalf("q2 predicates present=%v want %v req=%+v", got, wantPredicates, req)
			}
		})
	}
}

func TestColumnStoreQ2RendersFusedCountDistinctResult(t *testing.T) {
	computed := renderColumnQ2(5, collections.ColumnPhysicalQueryResult{
		Groups: []collections.ColumnPhysicalQueryGroup{
			{Key: "event_b", Count: 2, DistinctCount: 1},
			{Key: "", Count: 1, DistinctCount: 1},
			{Key: "event_a", Count: 2, DistinctCount: 2},
			{Key: "event_c", Count: 1, DistinctCount: 1},
		},
	})
	want := []queryRow{
		{"event": "event_a", "count": int64(2), "users": int64(2)},
		{"event": "event_b", "count": int64(2), "users": int64(1)},
		{"event": "event_c", "count": int64(1), "users": int64(1)},
	}
	if !reflect.DeepEqual(computed.Rows, want) {
		t.Fatalf("q2 rows=%v want %v", computed.Rows, want)
	}
	if got, want := len(computed.Diagnostics.PhysicalQueries), 1; got != want {
		t.Fatalf("q2 physical query diagnostics=%d want %d", got, want)
	}
	if got, want := computed.Diagnostics.PhysicalQueries[0].Name, "group_count_and_distinct"; got != want {
		t.Fatalf("q2 physical query name=%q want %q", got, want)
	}
}

func TestColumnStorePreparedLayoutMatchesRowFixture(t *testing.T) {
	for _, query := range jsonBenchQueryNames {
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
			if query == "qexpr" {
				assertTypedInt64AggregateQueryDiagnostics(t, column.Queries[0])
			} else {
				assertColumnPhysicalQueryDiagnostics(t, column.Queries[0], expectedPhysicalQueryCount(query))
			}
			if (isQ4FamilyQuery(query) || query == "q5") && column.Queries[0].RowsScanned == 0 {
				t.Fatalf("column-store-prepared %s rows_scanned=0; non-metadata prepared layout must scan base rows", query)
			}
		})
	}
}

func TestColumnStorePreparedMetadataLayoutMatchesRowFixture(t *testing.T) {
	// The checked-in JSONBench fixture is intentionally only six rows, so this is
	// a semantic smoke test; the full top-N ordering path is covered by local
	// benchmark runs over larger downloaded JSONBench datasets.
	for _, query := range jsonBenchQueryNames {
		query := query
		t.Run(query, func(t *testing.T) {
			row := runJSONBenchFixtureCell(t, storageLayoutRow, query)
			column := runJSONBenchFixtureCell(t, storageLayoutColumnStorePreparedMetadata, query)
			if got, want := column.Queries[0].ResultHash, row.Queries[0].ResultHash; got != want {
				t.Fatalf("column-store-prepared-metadata result hash=%s want row hash=%s", got, want)
			}
			if isQ4FamilyQuery(query) || query == "q5" {
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
			if query == "qexpr" {
				assertTypedInt64AggregateQueryDiagnostics(t, column.Queries[0])
			} else {
				assertColumnPhysicalQueryDiagnostics(t, column.Queries[0], expectedPhysicalQueryCount(query))
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

func TestFullPreparedNoAggregateMetadataScansRows(t *testing.T) {
	row := runJSONBenchFixtureCell(t, storageLayoutRow, "q1")
	cfg := runFullFixtureConfig(storageLayoutColumnStoreFullPrepared, false)
	cfg.Queries = []string{"q1"}
	cfg.MetadataMode = metadataModeNoAggregateMetadata
	column := runJSONBenchConfig(t, cfg)
	query := column.Queries[0]
	if got, want := query.ResultHash, row.Queries[0].ResultHash; got != want {
		t.Fatalf("no-metadata q1 result hash=%s want row hash=%s", got, want)
	}
	if query.RowsScanned == 0 {
		t.Fatalf("no-metadata q1 rows_scanned=0; want typed-column scan")
	}
	if query.Diagnostics.AggregateMetadataUsed {
		t.Fatalf("no-metadata q1 aggregate_metadata_used=true diagnostics=%+v", query.Diagnostics)
	}
	if got, want := query.MetadataMode, metadataModeNoAggregateMetadata; got != want {
		t.Fatalf("metadata_mode=%q want %q", got, want)
	}
}

func TestOneShotPreparedLayoutUsesDirectRunAndReportsRenderHash(t *testing.T) {
	cfg := runFullFixtureConfig(storageLayoutColumnStoreFullPrepared, false)
	cfg.Queries = []string{"q1"}
	cfg.QueryMode = queryModeOneShotEndToEnd
	cfg.MetadataMode = metadataModeNoAggregateMetadata
	result := runJSONBenchConfig(t, cfg)
	query := result.Queries[0]
	if got, want := query.QueryMode, queryModeOneShotEndToEnd; got != want {
		t.Fatalf("query_mode=%q want %q", got, want)
	}
	if !query.Diagnostics.TypedColumnOneShotCacheMiss || !query.Diagnostics.TypedColumnOneShotCacheBuild || query.Diagnostics.TypedColumnOneShotCacheHit {
		t.Fatalf("typed-column one-shot cache hit/miss/build=%t/%t/%t want false/true/true diagnostics=%+v",
			query.Diagnostics.TypedColumnOneShotCacheHit,
			query.Diagnostics.TypedColumnOneShotCacheMiss,
			query.Diagnostics.TypedColumnOneShotCacheBuild,
			query.Diagnostics)
	}
	if query.Diagnostics.TypedColumnOneShotBuildNanos <= 0 {
		t.Fatalf("typed_column_one_shot_build_nanos=%d want >0 diagnostics=%+v", query.Diagnostics.TypedColumnOneShotBuildNanos, query.Diagnostics)
	}
	if got, want := query.Diagnostics.PrepareSetupNanos, query.Diagnostics.TypedColumnOneShotBuildNanos; got != want {
		t.Fatalf("prepare_setup_nanos=%d want typed-column one-shot build %d diagnostics=%+v", got, want, query.Diagnostics)
	}
	prepareSubphaseNanos := query.Diagnostics.TypedColumnPreparePlanNanos +
		query.Diagnostics.TypedColumnPrepareRefsNanos +
		query.Diagnostics.TypedColumnPreparePairingNanos +
		query.Diagnostics.TypedColumnPreparePartDecodeNanos +
		query.Diagnostics.TypedColumnPreparePostPrepareNanos +
		query.Diagnostics.TypedColumnPrepareSummaryNanos
	prepareFineNanos := query.Diagnostics.TypedColumnPrepareReadImageNanos +
		query.Diagnostics.TypedColumnPrepareStateBuildNanos +
		query.Diagnostics.TypedColumnPrepareDictionaryNanos +
		query.Diagnostics.TypedColumnPreparePruningNanos +
		query.Diagnostics.TypedColumnPrepareSortKeyNanos +
		query.Diagnostics.TypedColumnPrepareStatsNanos +
		query.Diagnostics.TypedColumnPrepareRangeReadNanos +
		query.Diagnostics.TypedColumnPrepareAdapterNanos +
		query.Diagnostics.TypedColumnPrepareDenseGroupNanos +
		query.Diagnostics.TypedColumnPrepareDenseValueNanos +
		query.Diagnostics.TypedColumnPrepareDensePredicateNanos +
		query.Diagnostics.TypedColumnPrepareDensePreapplyNanos
	if prepareSubphaseNanos > 0 {
		if query.Diagnostics.TypedColumnPreparePartDecodeNanos <= 0 {
			t.Fatalf("typed_column_prepare_part_decode_nanos=%d want >0 diagnostics=%+v", query.Diagnostics.TypedColumnPreparePartDecodeNanos, query.Diagnostics)
		}
		if query.Diagnostics.TypedColumnOneShotCacheStoreNanos <= 0 {
			t.Fatalf("typed_column_one_shot_cache_store_nanos=%d want >0 diagnostics=%+v", query.Diagnostics.TypedColumnOneShotCacheStoreNanos, query.Diagnostics)
		}
		if query.Diagnostics.PrepareSetupNanos < prepareSubphaseNanos {
			t.Fatalf("prepare_setup_nanos=%d smaller than subphase sum %d diagnostics=%+v", query.Diagnostics.PrepareSetupNanos, prepareSubphaseNanos, query.Diagnostics)
		}
	}
	if prepareFineNanos > 0 && query.Diagnostics.TypedColumnPrepareRangeReadNanos > 0 && query.Diagnostics.TypedColumnPrepareRangeReadBytes <= 0 {
		t.Fatalf("typed_column_prepare_range_read_bytes=%d want >0 with range_read_nanos=%d diagnostics=%+v",
			query.Diagnostics.TypedColumnPrepareRangeReadBytes,
			query.Diagnostics.TypedColumnPrepareRangeReadNanos,
			query.Diagnostics)
	}
	if query.Diagnostics.RunNanos <= 0 {
		t.Fatalf("run_nanos=%d want >0", query.Diagnostics.RunNanos)
	}
	if query.Diagnostics.RenderHashNanos <= 0 {
		t.Fatalf("render_hash_nanos=%d want >0", query.Diagnostics.RenderHashNanos)
	}
	if query.Diagnostics.TotalQueryNanos < query.Diagnostics.PrepareSetupNanos+query.Diagnostics.RunNanos+query.Diagnostics.RenderHashNanos {
		t.Fatalf("total_query_nanos=%d split prepare=%d run=%d render_hash=%d", query.Diagnostics.TotalQueryNanos, query.Diagnostics.PrepareSetupNanos, query.Diagnostics.RunNanos, query.Diagnostics.RenderHashNanos)
	}
}

func TestHotPreparedReportsRunnerPrepareDiagnostics(t *testing.T) {
	if !columnPhysicalRunnerPrepareDiagnosticsAvailableForTest() {
		t.Skip("linked gomap does not expose ColumnPhysicalQueryRunner.PrepareDiagnostics")
	}
	cfg := runFullFixtureConfig(storageLayoutColumnStoreFullPrepared, false)
	cfg.Queries = []string{"q5"}
	cfg.QueryMode = queryModeHotPreparedRun
	cfg.MetadataMode = metadataModeNoAggregateMetadata
	result := runJSONBenchConfig(t, cfg)
	query := result.Queries[0]
	if got, want := query.QueryMode, queryModeHotPreparedRun; got != want {
		t.Fatalf("query_mode=%q want %q", got, want)
	}
	if query.Diagnostics.TypedColumnOneShotCacheBuild || query.Diagnostics.TypedColumnOneShotBuildNanos != 0 {
		t.Fatalf("hot-prepared query reported one-shot setup diagnostics=%+v", query.Diagnostics)
	}
	if query.Diagnostics.PrepareSetupNanos <= 0 {
		t.Fatalf("prepare_setup_nanos=%d want >0 diagnostics=%+v", query.Diagnostics.PrepareSetupNanos, query.Diagnostics)
	}
	prepareSubphaseNanos := typedColumnPrepareDiagnosticNanos(query.Diagnostics)
	if prepareSubphaseNanos == 0 {
		t.Skipf("linked gomap exposes prepare diagnostics, but fixture timers rounded to zero: %+v", query.Diagnostics)
	}
	if query.Diagnostics.TypedColumnPreparePartDecodeNanos <= 0 {
		t.Fatalf("typed_column_prepare_part_decode_nanos=%d want >0 diagnostics=%+v", query.Diagnostics.TypedColumnPreparePartDecodeNanos, query.Diagnostics)
	}
	if got, want := len(query.Diagnostics.PhysicalQueries), 1; got != want {
		t.Fatalf("physical query diagnostics=%d want %d: %+v", got, want, query.Diagnostics.PhysicalQueries)
	}
	if physSubphaseNanos := typedColumnPhysicalPrepareDiagnosticNanos(query.Diagnostics.PhysicalQueries[0]); physSubphaseNanos == 0 {
		t.Fatalf("physical query prepare diagnostics were not merged: %+v", query.Diagnostics.PhysicalQueries[0])
	}
}

func TestPreparedColumnQueryAppliesPrepareDiagnosticsByPhysicalName(t *testing.T) {
	diag := queryDiagnostics{
		PhysicalQueries: []queryPhysicalDiagnostic{
			{Name: "group_count", RowsScanned: 10},
		},
	}
	prepared := preparedColumnQuery{
		prepare: []queryPhysicalDiagnostic{
			{
				Name:                                                  "group_count",
				DenseInt64SpanPredicateBlocksSkipped:                  7,
				TypedColumnPrepareWorkerCount:                         8,
				TypedColumnPreparePlanNanos:                           1,
				TypedColumnPrepareRefsNanos:                           2,
				TypedColumnPreparePairingNanos:                        3,
				TypedColumnPreparePartDecodeNanos:                     4,
				TypedColumnPreparePostPrepareNanos:                    5,
				TypedColumnPrepareSummaryNanos:                        6,
				TypedColumnPrepareReadImageNanos:                      7,
				TypedColumnPrepareStateBuildNanos:                     8,
				TypedColumnPrepareDictionaryNanos:                     9,
				TypedColumnPreparePruningNanos:                        10,
				TypedColumnPrepareSortKeyNanos:                        11,
				TypedColumnPrepareStatsNanos:                          12,
				TypedColumnPrepareRangeReadNanos:                      13,
				TypedColumnPrepareRangeReadBytes:                      14,
				TypedColumnPrepareAdapterNanos:                        15,
				TypedColumnPrepareDenseGroupNanos:                     16,
				TypedColumnPrepareDenseValueNanos:                     17,
				TypedColumnPrepareDensePredicateNanos:                 18,
				TypedColumnPrepareDensePreapplyNanos:                  19,
				TypedColumnPrepareQ2GroupRankNanos:                    20,
				TypedColumnPrepareQ2DistinctRankNanos:                 21,
				TypedColumnPrepareQ2LocalRankNanos:                    22,
				TypedColumnPrepareQ2DenseGroupGlobalRankNanos:         23,
				TypedColumnPrepareQ2DenseDistinctGlobalRankNanos:      24,
				TypedColumnPrepareQ2DensePartLocalRankNanos:           25,
				TypedColumnPrepareQ2GroupGlobalDictionaryRankNanos:    26,
				TypedColumnPrepareQ2DistinctGlobalDictionaryRankNanos: 27,
				TypedColumnPrepareQ2GroupGlobalLocalRankNanos:         101,
				TypedColumnPrepareQ2DistinctGlobalLocalRankNanos:      102,
				TypedColumnPrepareQ2GroupGlobalCodeRemapNanos:         28,
				TypedColumnPrepareQ2DistinctGlobalCodeRemapNanos:      29,
				TypedColumnPrepareQ2DenseDistinctRankPlanNanos:        30,
				TypedColumnPrepareQ2DenseDistinctRankCollectRefsNanos: 31,
				TypedColumnPrepareQ2DenseDistinctRankBuildShardsNanos: 32,
				TypedColumnPrepareQ2DenseDistinctRankShardCount:       4,
				TypedColumnPrepareQ2DenseDistinctRankRefs:             33,
				TypedColumnPrepareQ2DenseDistinctRankMaxShardRefs:     12,
				TypedColumnPrepareQ2DenseDistinctGlobalRanks:          44,
			},
			{
				Name: "group_count",
				TypedColumnPrepareQ2GroupGlobalLocalRankNanos:         1000,
				TypedColumnPrepareQ2DistinctGlobalLocalRankNanos:      2000,
				TypedColumnPrepareQ2DenseDistinctRankPlanNanos:        100,
				TypedColumnPrepareQ2DenseDistinctRankCollectRefsNanos: 200,
				TypedColumnPrepareQ2DenseDistinctRankBuildShardsNanos: 300,
				TypedColumnPrepareQ2DenseDistinctRankShardCount:       3,
				TypedColumnPrepareQ2DenseDistinctRankRefs:             400,
				TypedColumnPrepareQ2DenseDistinctRankMaxShardRefs:     99,
				TypedColumnPrepareQ2DenseDistinctGlobalRanks:          40,
			},
		},
	}
	prepared.applyPrepareDiagnostics(&diag)
	if got, want := len(diag.PhysicalQueries), 1; got != want {
		t.Fatalf("physical query diagnostics=%d want %d: %+v", got, want, diag.PhysicalQueries)
	}
	if got, want := diag.TypedColumnPreparePlanNanos, int64(1); got != want {
		t.Fatalf("typed_column_prepare_plan_nanos=%d want %d", got, want)
	}
	if got, want := diag.TypedColumnPrepareRangeReadBytes, int64(14); got != want {
		t.Fatalf("typed_column_prepare_range_read_bytes=%d want %d", got, want)
	}
	if got, want := diag.TypedColumnPrepareWorkerCount, 8; got != want {
		t.Fatalf("typed_column_prepare_worker_count=%d want %d", got, want)
	}
	if got, want := diag.DenseInt64SpanPredicateBlocksSkipped, 7; got != want {
		t.Fatalf("dense_int64_span_predicate_blocks_skipped=%d want %d", got, want)
	}
	if got, want := diag.PhysicalQueries[0].RowsScanned, 10; got != want {
		t.Fatalf("physical rows_scanned=%d want %d", got, want)
	}
	if got, want := diag.PhysicalQueries[0].TypedColumnPrepareWorkerCount, 8; got != want {
		t.Fatalf("physical typed_column_prepare_worker_count=%d want %d", got, want)
	}
	if got, want := diag.PhysicalQueries[0].DenseInt64SpanPredicateBlocksSkipped, 7; got != want {
		t.Fatalf("physical dense_int64_span_predicate_blocks_skipped=%d want %d", got, want)
	}
	if got, want := diag.PhysicalQueries[0].TypedColumnPrepareDensePreapplyNanos, int64(19); got != want {
		t.Fatalf("physical typed_column_prepare_dense_preapply_nanos=%d want %d", got, want)
	}
	if got, want := diag.TypedColumnPrepareQ2GroupRankNanos, int64(20); got != want {
		t.Fatalf("typed_column_prepare_q2_group_rank_nanos=%d want %d", got, want)
	}
	if got, want := diag.TypedColumnPrepareQ2DistinctRankNanos, int64(21); got != want {
		t.Fatalf("typed_column_prepare_q2_distinct_rank_nanos=%d want %d", got, want)
	}
	if got, want := diag.TypedColumnPrepareQ2LocalRankNanos, int64(22); got != want {
		t.Fatalf("typed_column_prepare_q2_local_rank_nanos=%d want %d", got, want)
	}
	if got, want := diag.TypedColumnPrepareQ2DenseGroupGlobalRankNanos, int64(23); got != want {
		t.Fatalf("typed_column_prepare_q2_dense_group_global_rank_nanos=%d want %d", got, want)
	}
	if got, want := diag.TypedColumnPrepareQ2DenseDistinctGlobalRankNanos, int64(24); got != want {
		t.Fatalf("typed_column_prepare_q2_dense_distinct_global_rank_nanos=%d want %d", got, want)
	}
	if got, want := diag.TypedColumnPrepareQ2DensePartLocalRankNanos, int64(25); got != want {
		t.Fatalf("typed_column_prepare_q2_dense_part_local_rank_nanos=%d want %d", got, want)
	}
	if got, want := diag.TypedColumnPrepareQ2GroupGlobalDictionaryRankNanos, int64(26); got != want {
		t.Fatalf("typed_column_prepare_q2_group_global_dictionary_rank_nanos=%d want %d", got, want)
	}
	if got, want := diag.TypedColumnPrepareQ2DistinctGlobalDictionaryRankNanos, int64(27); got != want {
		t.Fatalf("typed_column_prepare_q2_distinct_global_dictionary_rank_nanos=%d want %d", got, want)
	}
	if got, want := diag.TypedColumnPrepareQ2GroupGlobalLocalRankNanos, int64(1101); got != want {
		t.Fatalf("typed_column_prepare_q2_group_global_local_rank_nanos=%d want %d", got, want)
	}
	if got, want := diag.TypedColumnPrepareQ2DistinctGlobalLocalRankNanos, int64(2102); got != want {
		t.Fatalf("typed_column_prepare_q2_distinct_global_local_rank_nanos=%d want %d", got, want)
	}
	if got, want := diag.TypedColumnPrepareQ2GroupGlobalCodeRemapNanos, int64(28); got != want {
		t.Fatalf("typed_column_prepare_q2_group_global_code_remap_nanos=%d want %d", got, want)
	}
	if got, want := diag.TypedColumnPrepareQ2DistinctGlobalCodeRemapNanos, int64(29); got != want {
		t.Fatalf("typed_column_prepare_q2_distinct_global_code_remap_nanos=%d want %d", got, want)
	}
	if got, want := diag.TypedColumnPrepareQ2DenseDistinctRankPlanNanos, int64(130); got != want {
		t.Fatalf("typed_column_prepare_q2_dense_distinct_rank_plan_nanos=%d want %d", got, want)
	}
	if got, want := diag.TypedColumnPrepareQ2DenseDistinctRankCollectRefsNanos, int64(231); got != want {
		t.Fatalf("typed_column_prepare_q2_dense_distinct_rank_collect_refs_nanos=%d want %d", got, want)
	}
	if got, want := diag.TypedColumnPrepareQ2DenseDistinctRankBuildShardsNanos, int64(332); got != want {
		t.Fatalf("typed_column_prepare_q2_dense_distinct_rank_build_shards_nanos=%d want %d", got, want)
	}
	if got, want := diag.TypedColumnPrepareQ2DenseDistinctRankShardCount, 4; got != want {
		t.Fatalf("typed_column_prepare_q2_dense_distinct_rank_shard_count=%d want %d", got, want)
	}
	if got, want := diag.TypedColumnPrepareQ2DenseDistinctRankRefs, 433; got != want {
		t.Fatalf("typed_column_prepare_q2_dense_distinct_rank_refs=%d want %d", got, want)
	}
	if got, want := diag.TypedColumnPrepareQ2DenseDistinctRankMaxShardRefs, 99; got != want {
		t.Fatalf("typed_column_prepare_q2_dense_distinct_rank_max_shard_refs=%d want %d", got, want)
	}
	if got, want := diag.TypedColumnPrepareQ2DenseDistinctGlobalRanks, 44; got != want {
		t.Fatalf("typed_column_prepare_q2_dense_distinct_global_ranks=%d want %d", got, want)
	}
	if got, want := diag.PhysicalQueries[0].TypedColumnPrepareQ2LocalRankNanos, int64(22); got != want {
		t.Fatalf("physical typed_column_prepare_q2_local_rank_nanos=%d want %d", got, want)
	}
	if got, want := diag.PhysicalQueries[0].TypedColumnPrepareQ2DenseDistinctGlobalRankNanos, int64(24); got != want {
		t.Fatalf("physical typed_column_prepare_q2_dense_distinct_global_rank_nanos=%d want %d", got, want)
	}
	if got, want := diag.PhysicalQueries[0].TypedColumnPrepareQ2GroupGlobalLocalRankNanos, int64(1101); got != want {
		t.Fatalf("physical typed_column_prepare_q2_group_global_local_rank_nanos=%d want %d", got, want)
	}
	if got, want := diag.PhysicalQueries[0].TypedColumnPrepareQ2DistinctGlobalLocalRankNanos, int64(2102); got != want {
		t.Fatalf("physical typed_column_prepare_q2_distinct_global_local_rank_nanos=%d want %d", got, want)
	}
	if got, want := diag.PhysicalQueries[0].TypedColumnPrepareQ2DistinctGlobalCodeRemapNanos, int64(29); got != want {
		t.Fatalf("physical typed_column_prepare_q2_distinct_global_code_remap_nanos=%d want %d", got, want)
	}
	if got, want := diag.PhysicalQueries[0].TypedColumnPrepareQ2DenseDistinctRankPlanNanos, int64(130); got != want {
		t.Fatalf("physical typed_column_prepare_q2_dense_distinct_rank_plan_nanos=%d want %d", got, want)
	}
	if got, want := diag.PhysicalQueries[0].TypedColumnPrepareQ2DenseDistinctRankCollectRefsNanos, int64(231); got != want {
		t.Fatalf("physical typed_column_prepare_q2_dense_distinct_rank_collect_refs_nanos=%d want %d", got, want)
	}
	if got, want := diag.PhysicalQueries[0].TypedColumnPrepareQ2DenseDistinctRankBuildShardsNanos, int64(332); got != want {
		t.Fatalf("physical typed_column_prepare_q2_dense_distinct_rank_build_shards_nanos=%d want %d", got, want)
	}
	if got, want := diag.PhysicalQueries[0].TypedColumnPrepareQ2DenseDistinctRankShardCount, 4; got != want {
		t.Fatalf("physical typed_column_prepare_q2_dense_distinct_rank_shard_count=%d want %d", got, want)
	}
	if got, want := diag.PhysicalQueries[0].TypedColumnPrepareQ2DenseDistinctRankRefs, 433; got != want {
		t.Fatalf("physical typed_column_prepare_q2_dense_distinct_rank_refs=%d want %d", got, want)
	}
	if got, want := diag.PhysicalQueries[0].TypedColumnPrepareQ2DenseDistinctRankMaxShardRefs, 99; got != want {
		t.Fatalf("physical typed_column_prepare_q2_dense_distinct_rank_max_shard_refs=%d want %d", got, want)
	}
	if got, want := diag.PhysicalQueries[0].TypedColumnPrepareQ2DenseDistinctGlobalRanks, 44; got != want {
		t.Fatalf("physical typed_column_prepare_q2_dense_distinct_global_ranks=%d want %d", got, want)
	}
}

func TestPhysicalQueryDiagnosticMapsOptionalQ2DenseRankDiagnostics(t *testing.T) {
	var upstream collections.ColumnPhysicalQueryDiagnostics
	upstreamValue := reflect.ValueOf(&upstream).Elem()
	fields := []struct {
		name    string
		want    int64
		read    func(queryPhysicalDiagnostic) int64
		present bool
	}{
		{
			name: "TypedColumnPrepareQ2GroupGlobalLocalRankNanos",
			want: 34,
			read: func(phys queryPhysicalDiagnostic) int64 {
				return phys.TypedColumnPrepareQ2GroupGlobalLocalRankNanos
			},
		},
		{
			name: "TypedColumnPrepareQ2DistinctGlobalLocalRankNanos",
			want: 35,
			read: func(phys queryPhysicalDiagnostic) int64 {
				return phys.TypedColumnPrepareQ2DistinctGlobalLocalRankNanos
			},
		},
		{
			name: "TypedColumnPrepareQ2DenseGroupGlobalRankNanos",
			want: 31,
			read: func(phys queryPhysicalDiagnostic) int64 {
				return phys.TypedColumnPrepareQ2DenseGroupGlobalRankNanos
			},
		},
		{
			name: "TypedColumnPrepareQ2DenseDistinctGlobalRankNanos",
			want: 32,
			read: func(phys queryPhysicalDiagnostic) int64 {
				return phys.TypedColumnPrepareQ2DenseDistinctGlobalRankNanos
			},
		},
		{
			name: "TypedColumnPrepareQ2DensePartLocalRankNanos",
			want: 33,
			read: func(phys queryPhysicalDiagnostic) int64 {
				return phys.TypedColumnPrepareQ2DensePartLocalRankNanos
			},
		},
	}
	for i := range fields {
		field := upstreamValue.FieldByName(fields[i].name)
		if !field.IsValid() {
			continue
		}
		if !field.CanSet() || field.Kind() != reflect.Int64 {
			t.Fatalf("%s kind=%s canSet=%t, want settable int64", fields[i].name, field.Kind(), field.CanSet())
		}
		field.SetInt(fields[i].want)
		fields[i].present = true
	}

	phys := physicalQueryDiagnostic(namedColumnPhysicalResult{
		Name: "q2_dense_rank",
		Result: collections.ColumnPhysicalQueryResult{
			Diagnostics: upstream,
		},
	})
	for _, tc := range fields {
		want := int64(0)
		if tc.present {
			want = tc.want
		}
		if got := tc.read(phys); got != want {
			t.Fatalf("%s mapped value=%d want %d", tc.name, got, want)
		}
	}
}

func TestPhysicalQueryDiagnosticMapsOptionalQ2DenseDistinctRankDiagnostics(t *testing.T) {
	var upstream collections.ColumnPhysicalQueryDiagnostics
	upstreamValue := reflect.ValueOf(&upstream).Elem()
	fields := []struct {
		name      string
		wantInt64 int64
		wantInt   int
		readInt64 func(queryPhysicalDiagnostic) int64
		readInt   func(queryPhysicalDiagnostic) int
		present   bool
	}{
		{
			name:      "TypedColumnPrepareQ2DenseDistinctRankPlanNanos",
			wantInt64: 41,
			readInt64: func(phys queryPhysicalDiagnostic) int64 {
				return phys.TypedColumnPrepareQ2DenseDistinctRankPlanNanos
			},
		},
		{
			name:      "TypedColumnPrepareQ2DenseDistinctRankCollectRefsNanos",
			wantInt64: 42,
			readInt64: func(phys queryPhysicalDiagnostic) int64 {
				return phys.TypedColumnPrepareQ2DenseDistinctRankCollectRefsNanos
			},
		},
		{
			name:      "TypedColumnPrepareQ2DenseDistinctRankBuildShardsNanos",
			wantInt64: 43,
			readInt64: func(phys queryPhysicalDiagnostic) int64 {
				return phys.TypedColumnPrepareQ2DenseDistinctRankBuildShardsNanos
			},
		},
		{
			name:    "TypedColumnPrepareQ2DenseDistinctRankShardCount",
			wantInt: 44,
			readInt: func(phys queryPhysicalDiagnostic) int {
				return phys.TypedColumnPrepareQ2DenseDistinctRankShardCount
			},
		},
		{
			name:    "TypedColumnPrepareQ2DenseDistinctRankRefs",
			wantInt: 45,
			readInt: func(phys queryPhysicalDiagnostic) int {
				return phys.TypedColumnPrepareQ2DenseDistinctRankRefs
			},
		},
		{
			name:    "TypedColumnPrepareQ2DenseDistinctRankMaxShardRefs",
			wantInt: 46,
			readInt: func(phys queryPhysicalDiagnostic) int {
				return phys.TypedColumnPrepareQ2DenseDistinctRankMaxShardRefs
			},
		},
		{
			name:    "TypedColumnPrepareQ2DenseDistinctGlobalRanks",
			wantInt: 47,
			readInt: func(phys queryPhysicalDiagnostic) int {
				return phys.TypedColumnPrepareQ2DenseDistinctGlobalRanks
			},
		},
	}
	for i := range fields {
		field := upstreamValue.FieldByName(fields[i].name)
		if !field.IsValid() {
			continue
		}
		switch {
		case fields[i].readInt64 != nil:
			if !field.CanSet() || field.Kind() != reflect.Int64 {
				t.Fatalf("%s kind=%s canSet=%t, want settable int64", fields[i].name, field.Kind(), field.CanSet())
			}
			field.SetInt(fields[i].wantInt64)
		case fields[i].readInt != nil:
			if !field.CanSet() || field.Kind() != reflect.Int {
				t.Fatalf("%s kind=%s canSet=%t, want settable int", fields[i].name, field.Kind(), field.CanSet())
			}
			field.SetInt(int64(fields[i].wantInt))
		default:
			t.Fatalf("%s has no reader", fields[i].name)
		}
		fields[i].present = true
	}

	phys := physicalQueryDiagnostic(namedColumnPhysicalResult{
		Name: "q2_dense_distinct_rank",
		Result: collections.ColumnPhysicalQueryResult{
			Diagnostics: upstream,
		},
	})
	for _, tc := range fields {
		if tc.readInt64 != nil {
			want := int64(0)
			if tc.present {
				want = tc.wantInt64
			}
			if got := tc.readInt64(phys); got != want {
				t.Fatalf("%s mapped value=%d want %d", tc.name, got, want)
			}
			continue
		}
		want := 0
		if tc.present {
			want = tc.wantInt
		}
		if got := tc.readInt(phys); got != want {
			t.Fatalf("%s mapped value=%d want %d", tc.name, got, want)
		}
	}
}

func TestFirstTouchAfterOpenRequiresSingleAttempt(t *testing.T) {
	cfg := runFullFixtureConfig(storageLayoutColumnStoreFullPrepared, false)
	cfg.Queries = []string{"q1"}
	cfg.QueryMode = queryModeFirstTouchAfterOpen
	cfg.Tries = 2
	if _, err := runTreeDBBenchmark(cfg); err == nil || !strings.Contains(err.Error(), "pass -tries 1") {
		t.Fatalf("runTreeDBBenchmark first-touch tries=2 error=%v want tries error", err)
	}
}

func TestOneShotEndToEndRequiresSingleAttempt(t *testing.T) {
	cfg := runFullFixtureConfig(storageLayoutColumnStoreFullPrepared, false)
	cfg.Queries = []string{"q1"}
	cfg.QueryMode = queryModeOneShotEndToEnd
	cfg.Tries = 2
	if _, err := runTreeDBBenchmark(cfg); err == nil || !strings.Contains(err.Error(), "one submitted execution") {
		t.Fatalf("runTreeDBBenchmark one-shot tries=2 error=%v want one-shot tries error", err)
	}
}

func columnPhysicalRunnerPrepareDiagnosticsAvailableForTest() bool {
	_, ok := reflect.TypeOf((*collections.ColumnPhysicalQueryRunner)(nil)).MethodByName("PrepareDiagnostics")
	return ok
}

func typedColumnPrepareDiagnosticNanos(diag queryDiagnostics) int64 {
	return diag.TypedColumnPreparePlanNanos +
		diag.TypedColumnPrepareRefsNanos +
		diag.TypedColumnPreparePairingNanos +
		diag.TypedColumnPreparePartDecodeNanos +
		diag.TypedColumnPreparePostPrepareNanos +
		diag.TypedColumnPrepareSummaryNanos +
		diag.TypedColumnPrepareReadImageNanos +
		diag.TypedColumnPrepareStateBuildNanos +
		diag.TypedColumnPrepareDictionaryNanos +
		diag.TypedColumnPreparePruningNanos +
		diag.TypedColumnPrepareSortKeyNanos +
		diag.TypedColumnPrepareStatsNanos +
		diag.TypedColumnPrepareRangeReadNanos +
		diag.TypedColumnPrepareAdapterNanos +
		diag.TypedColumnPrepareDenseGroupNanos +
		diag.TypedColumnPrepareDenseValueNanos +
		diag.TypedColumnPrepareDensePredicateNanos +
		diag.TypedColumnPrepareDensePreapplyNanos
}

func typedColumnPhysicalPrepareDiagnosticNanos(diag queryPhysicalDiagnostic) int64 {
	return diag.TypedColumnPreparePlanNanos +
		diag.TypedColumnPrepareRefsNanos +
		diag.TypedColumnPreparePairingNanos +
		diag.TypedColumnPreparePartDecodeNanos +
		diag.TypedColumnPreparePostPrepareNanos +
		diag.TypedColumnPrepareSummaryNanos +
		diag.TypedColumnPrepareReadImageNanos +
		diag.TypedColumnPrepareStateBuildNanos +
		diag.TypedColumnPrepareDictionaryNanos +
		diag.TypedColumnPreparePruningNanos +
		diag.TypedColumnPrepareSortKeyNanos +
		diag.TypedColumnPrepareStatsNanos +
		diag.TypedColumnPrepareRangeReadNanos +
		diag.TypedColumnPrepareAdapterNanos +
		diag.TypedColumnPrepareDenseGroupNanos +
		diag.TypedColumnPrepareDenseValueNanos +
		diag.TypedColumnPrepareDensePredicateNanos +
		diag.TypedColumnPrepareDensePreapplyNanos
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
	result := runJSONBenchConfig(t, cfg)
	if got := len(result.Queries); got != 1 {
		t.Fatalf("queries=%d want 1", got)
	}
	return result
}

func runJSONBenchConfig(t *testing.T, cfg runConfig) runResult {
	t.Helper()
	if cfg.DBDir == "" {
		cfg.DBDir = t.TempDir()
	}
	result, err := runTreeDBBenchmark(cfg)
	if err != nil {
		t.Fatalf("runTreeDBBenchmark(%s, %v): %v", cfg.StorageLayout, cfg.Queries, err)
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

func assertTypedInt64AggregateQueryDiagnostics(t *testing.T, query queryRun) {
	t.Helper()
	if got, want := query.Diagnostics.QueryPath, "typed_column_int64_aggregate"; got != want {
		t.Fatalf("%s query path=%q want %q diagnostics=%+v", query.Name, got, want, query.Diagnostics)
	}
	if got, want := len(query.Diagnostics.PhysicalQueries), 1; got != want {
		t.Fatalf("%s physical query diagnostics=%d want %d: %+v", query.Name, got, want, query.Diagnostics.PhysicalQueries)
	}
	if got, want := query.Diagnostics.PhysicalQueries[0].Name, "second_of_day_square_sum"; got != want {
		t.Fatalf("%s physical query name=%q want %q", query.Name, got, want)
	}
	if query.Diagnostics.StorageSource != "typed_column_part" {
		t.Fatalf("%s storage source=%q want typed_column_part diagnostics=%+v", query.Name, query.Diagnostics.StorageSource, query.Diagnostics)
	}
	if query.Diagnostics.RowsScanned != query.RowsScanned || query.RowsScanned == 0 {
		t.Fatalf("%s rows scanned diagnostics=%+v rows_scanned=%d", query.Name, query.Diagnostics, query.RowsScanned)
	}
	if query.Diagnostics.RowMaterializations != 0 || query.Diagnostics.DocumentMaterializations != 0 {
		t.Fatalf("%s materializations diagnostics=%+v", query.Name, query.Diagnostics)
	}
	if query.Diagnostics.AggregateMetadataUsed {
		t.Fatalf("%s aggregate_metadata_used=true diagnostics=%+v", query.Name, query.Diagnostics)
	}
	if query.Diagnostics.JSONReconstructionUsed {
		t.Fatalf("%s json_reconstruction_used=true diagnostics=%+v", query.Name, query.Diagnostics)
	}
}

func expectedPhysicalQueryCount(query string) int {
	return 1
}

func testColumnPhysicalRequestForQuery(layout, query string) collections.ColumnPhysicalQueryRequest {
	cfg := runConfig{StorageLayout: layout}
	switch query {
	case "q1":
		return columnPhysicalRequest(cfg, "q1", collections.ColumnPhysicalQueryGroupCount, "event", "", "")
	case "q2":
		return columnPhysicalRequest(cfg, "q2", collections.ColumnPhysicalQueryGroupCountAndDistinct, "event", "", "did")
	case "q3":
		return columnPhysicalRequest(cfg, "q3", collections.ColumnPhysicalQueryGroupHourCount, "event", "time_us", "")
	case "q4", "q4a", "q4b":
		return columnPhysicalRequest(cfg, query, collections.ColumnPhysicalQueryGroupMinInt64, "did", "time_us", "")
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
	if isQ4FamilyQuery(query.Name) && !query.Diagnostics.TimeOrderTopKUsed {
		t.Fatalf("%s time_order_topk_used=false diagnostics=%+v", query.Name, query.Diagnostics)
	}
	if query.Name == "q5" && !query.Diagnostics.BoundedTopKUsed {
		t.Fatalf("q5 bounded_topk_used=false diagnostics=%+v", query.Diagnostics)
	}
}

func assertAggregateMetadataTopKDiagnostics(t *testing.T, query queryRun) {
	t.Helper()
	if got := query.Diagnostics.TopKLimit; got != 3 {
		t.Fatalf("%s topk_limit=%d want 3 diagnostics=%+v", query.Name, got, query.Diagnostics)
	}
	if got := query.Diagnostics.TopKCandidates; got == 0 {
		t.Fatalf("%s topk_candidates=0 diagnostics=%+v", query.Name, query.Diagnostics)
	}
}

func runJSONBenchFullFixtureCell(t *testing.T, layout string, validateReconstruction bool) runResult {
	t.Helper()
	cfg := runFullFixtureConfig(layout, validateReconstruction)
	result := runJSONBenchConfig(t, cfg)
	if got := len(result.Queries); got != len(cfg.Queries) {
		t.Fatalf("queries=%d want %d", got, len(cfg.Queries))
	}
	return result
}

func runFullFixtureConfig(layout string, validateReconstruction bool) runConfig {
	return runConfig{
		DataDir:                "../../testdata/bluesky",
		Reset:                  true,
		Scale:                  "subset",
		Rows:                   6,
		MaxFiles:               1,
		Format:                 "json",
		StorageLayout:          layout,
		Projection:             "full",
		Queries:                append([]string(nil), jsonBenchQueryNames...),
		BatchSize:              defaultBatchSize,
		Profile:                "fast",
		DataRoot:               "fast",
		Collection:             defaultCollectionName,
		Checkpoint:             true,
		ValidateReconstruction: validateReconstruction,
		Tries:                  1,
	}
}

func isQ4FamilyQuery(query string) bool {
	return query == "q4" || query == "q4a" || query == "q4b"
}
