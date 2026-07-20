package main

import "testing"

type testDocumentScanStats struct {
	CertifiedMonotonicPath    bool
	GenericFallback           bool
	PhysicalPasses            uint64
	PhysicalRows              uint64
	PhysicalBytes             uint64
	PhysicalDecodedBlocks     uint64
	LocatorLookupBatches      uint64
	LocatorLookups            uint64
	PointRowFetches           uint64
	ReconstructedRows         uint64
	MaxRecordWindow           uint64
	MaxVisibleRowWindow       uint64
	MaxTypedGenerations       uint64
	MaxTypedDecodedBytes      uint64
	MaxTypedSourcePartBytes   uint64
	MaxRetainedBlocks         uint64
	PreflightProjectedColumns uint64
}

type testDocumentScanStatsSource struct{}

func (testDocumentScanStatsSource) LastDocumentScanStats() testDocumentScanStats {
	return testDocumentScanStats{
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
	}
}

func TestReadDocumentScanStats(t *testing.T) {
	got := readDocumentScanStats(testDocumentScanStatsSource{})
	if got == nil || !got.GenericFallback || got.LocatorLookupBatches != 3 || got.LocatorLookups != 10 || got.PointRowFetches != 10 || got.ReconstructedRows != 10 {
		t.Fatalf("scan stats mismatch: %+v", got)
	}
	if got.MaxRecordWindow != 256 || got.MaxVisibleRowWindow != 256 || got.MaxTypedGenerations != 1 || got.MaxTypedDecodedBytes != 4096 || got.MaxTypedSourcePartBytes != 8192 || got.MaxRetainedBlocks != 8 {
		t.Fatalf("scan bounds mismatch: %+v", got)
	}
}
