package main

import (
	"strings"
	"testing"
)

func TestRenderColumnStoreCompactSummary(t *testing.T) {
	doc := reportDocument{Rows: []reportRow{
		{System: "DuckDB", StorageLayout: "", Query: "q1", DatasetSize: 1_000_000, BestSec: 0.01},
		{System: "TreeDB", StorageLayout: storageLayoutColumnStorePreparedMetadata, Query: "q4", DatasetSize: 1_000_000, BestSec: 0.0005, RowsScanned: 0, StorageBytes: 1024, LoadSec: 2},
		{System: "TreeDB", StorageLayout: storageLayoutColumnStorePreparedMetadata, Query: "q4a", DatasetSize: 1_000_000, BestSec: 0.0004, RowsScanned: 0, StorageBytes: 1024, LoadSec: 2},
		{System: "TreeDB", StorageLayout: storageLayoutColumnStorePreparedMetadata, Query: "q3", DatasetSize: 1_000_000, BestSec: 0.010, RowsScanned: 1_000_000, StorageBytes: 1024, LoadSec: 2},
		{System: "TreeDB", StorageLayout: storageLayoutColumnStorePrepared, Query: "q4", DatasetSize: 1_000_000, BestSec: 0.020, RowsScanned: 1_000_000, StorageBytes: 1024, LoadSec: 2},
		{System: "TreeDB", StorageLayout: storageLayoutColumnStore, Query: "q3", DatasetSize: 1_000_000, BestSec: 0.025, RowsScanned: 1_000_000, StorageBytes: 2 * 1024 * 1024, LoadSec: 3},
		{System: "TreeDB", StorageLayout: storageLayoutColumnStoreFull, DataShape: "full-retained-json", Query: "q2", DatasetSize: 1_000_000, BestSec: 0.030, RowsScanned: 1_000_000, StorageBytes: 4 * 1024 * 1024, LoadSec: 4},
		{System: "TreeDB", StorageLayout: storageLayoutColumnStoreFullPrepared, DataShape: "full-retained-json", Query: "q1", DatasetSize: 1_000_000, BestSec: 0.004, RowsScanned: 0, StorageBytes: 5 * 1024 * 1024, LoadSec: 5},
	}}
	got := string(renderColumnStoreCompactSummary(doc))
	for _, want := range []string{
		"| layout | shape | mode | query | best | loaded rows/s | scanned rows | storage | load |",
		"| column-store | query-shaped-projection | direct physical scan | q3 | 0.0250s | 40.0M | 1.0M | 2.00 MiB | 3.000s |",
		"| column-store-prepared | query-shaped-projection | prepared physical scan | q4 | 0.0200s | 50.0M | 1.0M | 1.00 KiB | 2.000s |",
		"| column-store-prepared-metadata | query-shaped-projection | prepared physical scan | q3 | 0.0100s | 100.0M | 1.0M | 1.00 KiB | 2.000s |",
		"| column-store-prepared-metadata | query-shaped-projection | prepared metadata top-k | q4 | 500.0us | 2.00B logical | 0 | 1.00 KiB | 2.000s |",
		"| column-store-prepared-metadata | query-shaped-projection | prepared metadata top-k | q4a | 400.0us | 2.50B logical | 0 | 1.00 KiB | 2.000s |",
		"| column-store-full | full-retained-json | direct physical scan | q2 | 0.0300s | 33.3M | 1.0M | 4.00 MiB | 4.000s |",
		"| column-store-full-prepared | full-retained-json | full-prepared aggregate metadata | q1 | 0.0040s | 250.0M logical | 0 | 5.00 MiB | 5.000s |",
		"`full-retained-json` rows store enough JSON payload",
		"`prepared metadata top-k` applies to query-shaped q4/q4a/q4b/q5",
		"`full-prepared aggregate metadata` applies to full-retained q1/q3/q5",
		"`qexpr` is an arbitrary-expression typed-column scan/evaluation lane",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("summary missing %q\n%s", want, got)
		}
	}
	if strings.Contains(got, "DuckDB") {
		t.Fatalf("summary should filter non-TreeDB rows:\n%s", got)
	}
}
