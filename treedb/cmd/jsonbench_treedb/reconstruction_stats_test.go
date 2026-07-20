package main

import (
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
