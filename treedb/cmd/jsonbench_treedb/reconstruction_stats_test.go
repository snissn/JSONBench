package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestReadDocumentScanStats(t *testing.T) {
	got := documentScanStatsResultFromCollectionStats(collections.CollectionDocumentScanStats{
		GenericFallback:         true,
		LocatorLookupBatches:    3,
		LocatorLookups:          10,
		PointRowFetches:         10,
		ReconstructedRows:       10,
		MaxRecordWindow:         256,
		MaxVisibleRowWindow:     256,
		MaxTypedGenerations:     1,
		MaxTypedDecodedBytes:    4096,
		MaxTypedSourcePartBytes: 8192,
		MaxRetainedBlocks:       8,
	})
	if got == nil || !got.GenericFallback || got.LocatorLookupBatches != 3 || got.LocatorLookups != 10 || got.PointRowFetches != 10 || got.ReconstructedRows != 10 {
		t.Fatalf("scan stats mismatch: %+v", got)
	}
	if got.MaxRecordWindow != 256 || got.MaxVisibleRowWindow != 256 || got.MaxTypedGenerations != 1 || got.MaxTypedDecodedBytes != 4096 || got.MaxTypedSourcePartBytes != 8192 || got.MaxRetainedBlocks != 8 {
		t.Fatalf("scan bounds mismatch: %+v", got)
	}
}

func TestAggregateReportPreservesAndRendersReconstructionScanStats(t *testing.T) {
	dir := t.TempDir()
	result := runResult{
		SchemaVersion: schemaVersion,
		System:        "TreeDB",
		ScaleLabel:    "10 rows",
		DatasetSize:   10,
		Format:        "json",
		StorageLayout: storageLayoutColumnStoreFullPrepared,
		Projection:    "full",
		Reconstruction: &reconstructionResult{
			Valid: true,
			ScanStats: &documentScanStatsResult{
				CertifiedMonotonicPath:  true,
				PhysicalPasses:          1,
				PhysicalRows:            10,
				PhysicalBytes:           4096,
				LocatorLookupBatches:    2,
				LocatorLookups:          10,
				PointRowFetches:         10,
				ReconstructedRows:       10,
				MaxRecordWindow:         256,
				MaxVisibleRowWindow:     128,
				MaxTypedGenerations:     1,
				MaxTypedDecodedBytes:    8192,
				MaxTypedSourcePartBytes: 16384,
				MaxRetainedBlocks:       8,
			},
		},
		Queries: []queryRun{{Name: "q1"}},
	}
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "result.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}

	rows, err := collectTreeDBRows(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ReconstructionScanStats == nil {
		t.Fatalf("aggregate rows lost reconstruction scan stats: %+v", rows)
	}
	if got := rows[0].ReconstructionScanStats; !got.CertifiedMonotonicPath || got.PhysicalRows != 10 || got.LocatorLookups != 10 || got.MaxTypedSourcePartBytes != 16384 {
		t.Fatalf("aggregate reconstruction scan stats mismatch: %+v", got)
	}

	markdown := string(renderMarkdownReport(reportDocument{Rows: rows}))
	for _, want := range []string{
		"## TreeDB Reconstruction Scan Stats",
		"| 10 rows | column-store-full-prepared:json/full | true | false | 1 | 10 | 4.00 KiB |",
		"| 0 | 2 | 10 | 10 | 10 | 256 | 128 | 1 | 8.00 KiB | 16.00 KiB | 8 | 0 |",
	} {
		if !strings.Contains(markdown, want) {
			t.Fatalf("markdown missing %q\n%s", want, markdown)
		}
	}
}
