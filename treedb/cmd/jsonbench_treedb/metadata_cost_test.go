package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestAggregateMetadataLoadAccountingAppliesSplitInsertCost(t *testing.T) {
	var accounting aggregateMetadataLoadAccounting
	accounting.addInsertStats(collections.CollectionInsertStats{
		ColumnPublishAggregateMetadataPrepare: 200 * time.Millisecond,
		ColumnPublishAssetAppend:              100 * time.Millisecond,
		ColumnPublishAggregateMetadataBytes:   25,
		ColumnPublishSharedAppendBytes:        100,
	})
	var out loadResult
	accounting.apply(&out)

	if got, want := out.AggregateMetadataPrepareSec, 0.2; got != want {
		t.Fatalf("aggregate metadata prepare seconds=%f want %f", got, want)
	}
	if got, want := out.AggregateMetadataAppendShareSec, 0.025; got != want {
		t.Fatalf("aggregate metadata append share seconds=%f want %f", got, want)
	}
	if got, want := out.AggregateMetadataInsertCostSec, 0.225; got != want {
		t.Fatalf("aggregate metadata insert cost seconds=%f want %f", got, want)
	}
	if got, want := out.AggregateMetadataInsertCostBasis, metadataCostInsertBasisAggregateMetadataSplit; got != want {
		t.Fatalf("aggregate metadata insert cost basis=%q want %q", got, want)
	}
	if got, want := out.AggregateMetadataBytes, int64(25); got != want {
		t.Fatalf("aggregate metadata bytes=%d want %d", got, want)
	}
	if got, want := out.AggregateMetadataSharedAppendBytes, int64(100); got != want {
		t.Fatalf("aggregate metadata shared append bytes=%d want %d", got, want)
	}
}

func TestCollectTreeDBRowsPrefersSplitMetadataInsertCost(t *testing.T) {
	rows := collectTreeDBRowsForMetadataCostTest(t, loadResult{
		InsertSec:                          3.25,
		AggregateMetadataPrepareSec:        0.2,
		AggregateMetadataAppendShareSec:    0.025,
		AggregateMetadataInsertCostSec:     0.225,
		AggregateMetadataInsertCostBasis:   metadataCostInsertBasisAggregateMetadataSplit,
		AggregateMetadataBytes:             25,
		AggregateMetadataSharedAppendBytes: 100,
	})
	if got, want := len(rows), 1; got != want {
		t.Fatalf("rows=%d want %d", got, want)
	}
	row := rows[0]
	if got, want := row.MetadataCostInsertSec, 0.225; got != want {
		t.Fatalf("metadata cost insert seconds=%f want %f", got, want)
	}
	if got, want := row.MetadataCostInsertBasis, metadataCostInsertBasisAggregateMetadataSplit; got != want {
		t.Fatalf("metadata cost insert basis=%q want %q", got, want)
	}
}

func TestCollectTreeDBRowsFallsBackToFullLoadMetadataInsertCost(t *testing.T) {
	rows := collectTreeDBRowsForMetadataCostTest(t, loadResult{
		InsertSec: 3.25,
	})
	if got, want := len(rows), 1; got != want {
		t.Fatalf("rows=%d want %d", got, want)
	}
	row := rows[0]
	if got, want := row.MetadataCostInsertSec, 3.25; got != want {
		t.Fatalf("fallback metadata cost insert seconds=%f want %f", got, want)
	}
	if got, want := row.MetadataCostInsertBasis, metadataCostInsertBasisFullLoadUpperBound; got != want {
		t.Fatalf("fallback metadata cost insert basis=%q want %q", got, want)
	}
}

func collectTreeDBRowsForMetadataCostTest(t *testing.T, load loadResult) []reportRow {
	t.Helper()
	dir := t.TempDir()
	result := runResult{
		SchemaVersion: schemaVersion,
		System:        "TreeDB",
		Scale:         "subset",
		DatasetSize:   6,
		Load:          load,
		Queries: []queryRun{{
			Name:      "q4",
			BestSec:   0.001,
			MedianSec: 0.001,
			Diagnostics: queryDiagnostics{
				AggregateMetadataUsed: true,
			},
		}},
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
	return rows
}
