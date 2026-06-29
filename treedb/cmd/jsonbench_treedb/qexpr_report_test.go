package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestQExprReportRowsExposeTypedExpressionEvidence(t *testing.T) {
	dir := t.TempDir()
	result := runResult{
		SchemaVersion:    schemaVersion,
		System:           "TreeDB",
		Scale:            "subset",
		DatasetSize:      6,
		StorageLayout:    storageLayoutColumnStoreFullPrepared,
		Projection:       "full",
		QueryMode:        queryModeFirstTouchAfterOpen,
		MetadataMode:     metadataModeNoAggregateMetadata,
		DataShape:        "full-retained-json",
		TypedColumnOwner: "typed_column_part",
		Storage:          storageResult{TotalBytes: 1000},
		Queries: []queryRun{
			{
				Name:        "q1",
				BestSec:     0.001,
				MedianSec:   0.001,
				RowsScanned: 6,
				Diagnostics: queryDiagnostics{
					QueryPath:     "column_physical",
					StorageSource: "typed_column_part",
					RowsScanned:   6,
				},
			},
			{
				Name:        "qexpr",
				BestSec:     0.001,
				MedianSec:   0.001,
				RowsScanned: 6,
				Diagnostics: queryDiagnostics{
					QueryPath:      "typed_column_int64_aggregate",
					StorageSource:  "typed_column_part",
					FallbackReason: "none",
					RowsScanned:    6,
					PhysicalQueries: []queryPhysicalDiagnostic{{
						Name:          "second_of_day_square_sum",
						StorageSource: "typed_column_part",
						RowsScanned:   6,
					}},
				},
			},
		},
	}
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal result: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "result.json"), raw, 0o644); err != nil {
		t.Fatalf("write result: %v", err)
	}

	rows, err := collectTreeDBRows(dir)
	if err != nil {
		t.Fatalf("collect TreeDB rows: %v", err)
	}
	if got, want := len(rows), 2; got != want {
		t.Fatalf("rows=%d want %d: %+v", got, want, rows)
	}
	if rows[0].TypedCellsVisited != nil || rows[0].ExpressionKind != "" || rows[0].PrecomputedExpressionUsed != nil {
		t.Fatalf("non-qexpr row unexpectedly has expression evidence: %+v", rows[0])
	}
	qexpr := rows[1]
	if qexpr.TypedCellsVisited == nil || *qexpr.TypedCellsVisited != 6 {
		t.Fatalf("qexpr typed_cells_visited=%v want 6: %+v", qexpr.TypedCellsVisited, qexpr)
	}
	if got, want := qexpr.TypedCellsBasis, "rows_scanned"; got != want {
		t.Fatalf("qexpr typed_cells_basis=%q want %q", got, want)
	}
	if got, want := qexpr.ExpressionKind, "sum(second_of_day_square)"; got != want {
		t.Fatalf("qexpr expression_kind=%q want %q", got, want)
	}
	if qexpr.PrecomputedExpressionUsed == nil || *qexpr.PrecomputedExpressionUsed {
		t.Fatalf("qexpr precomputed_expression_used=%v want false", qexpr.PrecomputedExpressionUsed)
	}
	reportJSON, err := json.Marshal(qexpr)
	if err != nil {
		t.Fatalf("marshal qexpr report row: %v", err)
	}
	for _, want := range []string{
		`"typed_cells_visited":6`,
		`"typed_cells_basis":"rows_scanned"`,
		`"expression_kind":"sum(second_of_day_square)"`,
		`"precomputed_expression_used":false`,
		`"row_materializations":0`,
		`"document_materializations":0`,
		`"aggregate_metadata_used":false`,
		`"json_reconstruction_used":false`,
	} {
		if !strings.Contains(string(reportJSON), want) {
			t.Fatalf("qexpr report JSON missing %s\n%s", want, reportJSON)
		}
	}
}

func TestRenderMarkdownReportIncludesQExprExpressionEvidence(t *testing.T) {
	typedCells := 6
	precomputed := false
	doc := reportDocument{GeneratedAt: "2026-06-01T00:00:00Z", Rows: []reportRow{{
		System:                         "TreeDB",
		Scale:                          "6 rows",
		DatasetSize:                    6,
		Format:                         "json",
		StorageLayout:                  storageLayoutColumnStoreFullPrepared,
		Projection:                     "full",
		QueryMode:                      queryModeFirstTouchAfterOpen,
		MetadataMode:                   metadataModeNoAggregateMetadata,
		QueryPath:                      "typed_column_int64_aggregate",
		StorageSource:                  "typed_column_part",
		Query:                          "qexpr",
		BestSec:                        0.001,
		MedianSec:                      0.001,
		RowsScanned:                    6,
		TypedCellsVisited:              &typedCells,
		TypedCellsBasis:                "rows_scanned",
		ExpressionKind:                 "sum(second_of_day_square)",
		PrecomputedExpressionUsed:      &precomputed,
		AggregateMetadataUsed:          false,
		StorageBytes:                   1000,
		StorageDurableBytesWALExcluded: 1000,
	}}}
	got := string(renderMarkdownReport(doc))
	for _, want := range []string{
		"## TreeDB Expression Evidence",
		"| rows/scale | layout | query | expression | typed cells visited | basis | precomputed expression | aggregate metadata | path | source |",
		"| 6 rows | column-store-full-prepared:json/full | qexpr | sum(second_of_day_square) | 6 | rows_scanned | false | false | typed_column_int64_aggregate | typed_column_part |",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("markdown report missing %q\n%s", want, got)
		}
	}
}
