package main

import (
	"strings"
	"testing"
)

func TestRenderColumnStoreCompactSummary(t *testing.T) {
	doc := reportDocument{Rows: []reportRow{
		{System: "DuckDB", StorageLayout: "", Query: "q1", DatasetSize: 1_000_000, BestSec: 0.01},
		{System: "TreeDB", StorageLayout: storageLayoutColumnStorePreparedMetadata, Query: "q4", DatasetSize: 1_000_000, BestSec: 0.0005, RowsScanned: 0, StorageBytes: 1024, LoadSec: 2},
		{System: "TreeDB", StorageLayout: storageLayoutColumnStore, Query: "q3", DatasetSize: 1_000_000, BestSec: 0.025, RowsScanned: 1_000_000, StorageBytes: 2 * 1024 * 1024, LoadSec: 3},
	}}
	got := string(renderColumnStoreCompactSummary(doc))
	for _, want := range []string{
		"| layout | query | best | loaded rows/s | scanned rows | storage | load |",
		"| column-store | q3 | 0.0250s | 40.0M | 1.0M | 2.00 MiB | 3.000s |",
		"| column-store-prepared-metadata | q4 | 500.0us | 2.00B logical | 0 | 1.00 KiB | 2.000s |",
		"Metadata paths with `scanned rows` = 0 answer from aggregate metadata.",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("summary missing %q\n%s", want, got)
		}
	}
	if strings.Contains(got, "DuckDB") {
		t.Fatalf("summary should filter non-TreeDB rows:\n%s", got)
	}
}
