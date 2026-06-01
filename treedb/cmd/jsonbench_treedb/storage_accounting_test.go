package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDirectoryUsageClassifiesIncludedAndExcludedBytes(t *testing.T) {
	dir := t.TempDir()
	files := map[string]int{
		"maindb/index.db": 100,
		"dictdb/index.db": 40,
		"maindb/column_assets/bluesky/column-assets/assets/segments/part.tca": 70,
		"maindb/column_assets/bluesky/column-assets/assets/indexes/part.idx":  30,
		"maindb/column_assets/bluesky/column-assets/quarantine/old.tca":       20,
		"maindb/leaf_vlog/value-l255-000001.log":                              50,
		"maindb/value_vlog/value-l0-000001.log":                               60,
		"maindb/wal/000001.wal":                                               25,
		"maindb/format.json":                                                  10,
		"dictdb/format.json":                                                  11,
		"maindb/vlog_ref_counts.meta":                                         12,
		"maindb/LOCK":                                                         7,
		"dictdb/writer.lock":                                                  8,
		"maindb/tmp/rewrite.part":                                             9,
		"maindb/cpu_q1_treedb.pprof":                                          13,
		"maindb/benchprof_results.json":                                       14,
	}
	for rel, size := range files {
		writeSizedFile(t, filepath.Join(dir, rel), size)
	}

	got, err := directoryUsage(dir, 10)
	if err != nil {
		t.Fatalf("directoryUsage: %v", err)
	}
	if got.TotalBytes != 428 {
		t.Fatalf("included bytes=%d want 428; categories=%+v", got.TotalBytes, got.Categories)
	}
	if got.GrossBytes != 479 {
		t.Fatalf("gross bytes=%d want 479", got.GrossBytes)
	}
	if got.ExcludedBytes != 51 {
		t.Fatalf("excluded bytes=%d want 51", got.ExcludedBytes)
	}
	if got.FileCount != 11 || got.ExcludedFileCount != 5 {
		t.Fatalf("file counts included=%d excluded=%d want 11/5", got.FileCount, got.ExcludedFileCount)
	}
	if got.BytesPerRow != 42.8 {
		t.Fatalf("bytes/row=%f want 42.8", got.BytesPerRow)
	}
	assertStorageCategory(t, got, "primary_index", true, 100, 1)
	assertStorageCategory(t, got, "dictionary_index", true, 40, 1)
	assertStorageCategory(t, got, "column_asset_segments", true, 70, 1)
	assertStorageCategory(t, got, "column_asset_indexes", true, 30, 1)
	assertStorageCategory(t, got, "column_asset_quarantine", true, 20, 1)
	assertStorageCategory(t, got, "leaf_vlog", true, 50, 1)
	assertStorageCategory(t, got, "value_vlog", true, 60, 1)
	assertStorageCategory(t, got, "wal", true, 25, 1)
	assertStorageCategory(t, got, "format_metadata", true, 10, 1)
	assertStorageCategory(t, got, "dictionary_db_metadata", true, 11, 1)
	assertStorageCategory(t, got, "refcount_metadata", true, 12, 1)
	assertStorageCategory(t, got, "runtime_locks", false, 15, 2)
	assertStorageCategory(t, got, "temp_transient", false, 9, 1)
	assertStorageCategory(t, got, "profile_artifacts", false, 27, 2)
	assertStorageCategoriesDoNotDoubleCount(t, got)
}

func TestClassifyTreeDBStorageFile(t *testing.T) {
	for _, tc := range []struct {
		rel      string
		category string
		included bool
	}{
		{"maindb/index.db", "primary_index", true},
		{"dictdb/index.db", "dictionary_index", true},
		{"maindb/column_assets/ns/column-assets/assets/segments/segment-000001.tca", "column_asset_segments", true},
		{"maindb/wal/command-wal-journal-owner.lock", "runtime_locks", false},
		{"maindb/wal/command-000001.wal", "wal", true},
		{"maindb/value_vlog/value-l0-000001.log", "value_vlog", true},
		{"maindb/leaf_vlog/value-l255-000001.log", "leaf_vlog", true},
		{"maindb/trace.out", "profile_artifacts", false},
		{"maindb/tmp/rewrite.partial", "temp_transient", false},
	} {
		category, included := classifyTreeDBStorageFile(tc.rel)
		if category != tc.category || included != tc.included {
			t.Fatalf("classifyTreeDBStorageFile(%q)=(%q,%v) want (%q,%v)", tc.rel, category, included, tc.category, tc.included)
		}
	}
}

func TestRenderMarkdownReportLabelsDataShapeAndClickHouseBest(t *testing.T) {
	retainsJSON := true
	doc := reportDocument{GeneratedAt: "2026-06-01T00:00:00Z", Rows: []reportRow{
		{
			System:        "TreeDB",
			Scale:         "6 rows",
			DatasetSize:   6,
			Format:        "json",
			StorageLayout: storageLayoutColumnStoreFull,
			Projection:    "full",
			DataShape:     "full-retained-json",
			Query:         "q1",
			BestSec:       0.004,
			MedianSec:     0.004,
			AttemptsSec:   []float64{0.004},
			RowsScanned:   6,
			StorageBytes:  4096,
			RetainsJSON:   &retainsJSON,
		},
		{
			System:       "ClickHouse",
			Scale:        "6 rows",
			DatasetSize:  6,
			Format:       "json",
			Projection:   "full",
			DataShape:    "full-json",
			Query:        "q1",
			BestSec:      0.002,
			MedianSec:    0.002,
			AttemptsSec:  []float64{0.002},
			RowsScanned:  6,
			StorageBytes: 2048,
			RetainsJSON:  &retainsJSON,
		},
	}}
	got := string(renderMarkdownReport(doc))
	for _, want := range []string{
		"| rows/scale | system | shape | layout | query |",
		"| 6 rows | TreeDB | full-retained-json | column-store-full:json/full | q1 |",
		"| 6 rows | ClickHouse | full-json | json/full | q1 |",
		"| rows/scale | query | fastest system/layout | best | TreeDB best | DuckDB best | ClickHouse best | TreeDB / ClickHouse |",
		"| 6 rows | q1 | ClickHouse json/full | 0.0020s | 0.0040s |  | 0.0020s | 2.00x |",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("report missing %q\n%s", want, got)
		}
	}
}

func writeSizedFile(t *testing.T, path string, size int) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, make([]byte, size), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func assertStorageCategory(t *testing.T, storage storageResult, category string, included bool, bytes int64, files int) {
	t.Helper()
	for _, got := range storage.Categories {
		if got.Category != category {
			continue
		}
		if got.Included != included || got.Bytes != bytes || got.FileCount != files {
			t.Fatalf("category %s=(included=%v bytes=%d files=%d) want (%v,%d,%d)", category, got.Included, got.Bytes, got.FileCount, included, bytes, files)
		}
		return
	}
	t.Fatalf("missing category %s in %+v", category, storage.Categories)
}

func assertStorageCategoriesDoNotDoubleCount(t *testing.T, storage storageResult) {
	t.Helper()
	var included, excluded int64
	for _, category := range storage.Categories {
		if category.Included {
			included += category.Bytes
		} else {
			excluded += category.Bytes
		}
	}
	if included != storage.TotalBytes {
		t.Fatalf("included category bytes=%d total=%d", included, storage.TotalBytes)
	}
	if excluded != storage.ExcludedBytes {
		t.Fatalf("excluded category bytes=%d total=%d", excluded, storage.ExcludedBytes)
	}
	if included+excluded != storage.GrossBytes {
		t.Fatalf("category gross=%d storage gross=%d", included+excluded, storage.GrossBytes)
	}
}
