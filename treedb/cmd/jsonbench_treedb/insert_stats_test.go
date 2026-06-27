package main

import (
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestInsertStatsAccountingReportsRetainedValueLogPointerization(t *testing.T) {
	var accounting insertStatsAccounting
	if got := accounting.result(); got != nil {
		t.Fatalf("empty insert stats result=%+v want nil", got)
	}

	accounting.add(collections.CollectionInsertStats{
		RetainedPayloadPrepare:              100 * time.Millisecond,
		RetainedPayloadRows:                 10,
		RetainedPayloadDeclaredRows:         9,
		RetainedPayloadSemanticStreamBlocks: 2,
		RetainedPayloadValueLogPointerize:   30 * time.Millisecond,
		RetainedPayloadValueLogValues:       7,
		RetainedPayloadValueLogBytes:        700,
		RetainedStreamValueLogPointerize:    40 * time.Millisecond,
		RetainedStreamValueLogValues:        3,
		RetainedStreamValueLogBytes:         300,
	})
	accounting.add(collections.CollectionInsertStats{
		RetainedPayloadPrepare:              50 * time.Millisecond,
		RetainedPayloadRows:                 5,
		RetainedPayloadDeclaredRows:         5,
		RetainedPayloadSemanticStreamBlocks: 1,
		RetainedPayloadValueLogPointerize:   20 * time.Millisecond,
		RetainedPayloadValueLogValues:       2,
		RetainedPayloadValueLogBytes:        200,
		RetainedStreamValueLogPointerize:    10 * time.Millisecond,
		RetainedStreamValueLogValues:        1,
		RetainedStreamValueLogBytes:         100,
	})

	got := accounting.result()
	if got == nil {
		t.Fatal("insert stats result=nil want populated")
	}
	if got.RetainedPayloadPrepareSec != 0.15 {
		t.Fatalf("retained payload prepare sec=%f want 0.15", got.RetainedPayloadPrepareSec)
	}
	if got.RetainedPayloadRows != 15 || got.RetainedPayloadDeclaredRows != 14 || got.RetainedPayloadSemanticStreamBlocks != 3 {
		t.Fatalf("retained payload counters mismatch: %+v", got)
	}
	if got.RetainedPayloadValueLogPointerizeSec != 0.05 || got.RetainedPayloadValueLogValues != 9 || got.RetainedPayloadValueLogBytes != 900 {
		t.Fatalf("primary value-log pointerize stats mismatch: %+v", got)
	}
	if got.RetainedStreamValueLogPointerizeSec != 0.05 || got.RetainedStreamValueLogValues != 4 || got.RetainedStreamValueLogBytes != 400 {
		t.Fatalf("stream value-log pointerize stats mismatch: %+v", got)
	}
}

func TestRenderMarkdownReportIncludesTreeDBInsertStats(t *testing.T) {
	doc := reportDocument{Rows: []reportRow{{
		System:                                 "TreeDB",
		Scale:                                  "10 rows",
		Format:                                 "json",
		StorageLayout:                          storageLayoutColumnStoreFullPrepared,
		Projection:                             "full",
		LoadSec:                                2,
		InsertSec:                              1.5,
		InsertStatsRetainedPayloadPrepareSec:   0.1,
		InsertStatsRetainedPayloadRows:         10,
		InsertStatsRetainedPayloadDeclaredRows: 10,
		InsertStatsRetainedPayloadSemanticStreamBlocks:  2,
		InsertStatsRetainedPayloadValueLogPointerizeSec: 0.03,
		InsertStatsRetainedPayloadValueLogValues:        8,
		InsertStatsRetainedPayloadValueLogBytes:         800,
		InsertStatsRetainedStreamValueLogPointerizeSec:  0.04,
		InsertStatsRetainedStreamValueLogValues:         2,
		InsertStatsRetainedStreamValueLogBytes:          200,
	}}}

	got := string(renderMarkdownReport(doc))
	for _, want := range []string{
		"## TreeDB Insert Stats",
		"| rows/scale | layout | load | insert | retained prepare | retained rows | declared rows | stream blocks | primary vlog pointerize | primary vlog values | primary vlog bytes | stream vlog pointerize | stream vlog values | stream vlog bytes |",
		"| 10 rows | column-store-full-prepared:json/full | 2.000s | 1.500s | 0.1000s | 10 | 10 | 2 | 0.0300s | 8 | 800 B | 0.0400s | 2 | 200 B |",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("markdown missing %q\n%s", want, got)
		}
	}
}
