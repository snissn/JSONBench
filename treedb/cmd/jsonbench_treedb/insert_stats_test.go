package main

import (
	"reflect"
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

func TestInsertStatsAccountingReportsColumnPublishStats(t *testing.T) {
	var accounting insertStatsAccounting
	stats := collections.CollectionInsertStats{
		Publish:                               90 * time.Millisecond,
		ColumnPublishBuildColumnDelta:         10 * time.Millisecond,
		ColumnPublishBuildSystemDelta:         11 * time.Millisecond,
		ColumnPublishCommit:                   12 * time.Millisecond,
		ColumnPublishDocumentExtraction:       13 * time.Millisecond,
		ColumnPublishDeclaredColumnEncoding:   14 * time.Millisecond,
		ColumnPublishAssetPreparation:         15 * time.Millisecond,
		ColumnPublishRowAssetPreparation:      16 * time.Millisecond,
		ColumnPublishTypedColumnPreparation:   17 * time.Millisecond,
		ColumnPublishDictionaryPreparation:    22 * time.Millisecond,
		ColumnPublishInt64Preparation:         23 * time.Millisecond,
		ColumnPublishAggregateMetadataPrepare: 24 * time.Millisecond,
		ColumnPublishRowSidecarSharedBuild:    25 * time.Millisecond,
		ColumnPublishAssetAppend:              26 * time.Millisecond,
		ColumnPublishAssetAppendOpen:          27 * time.Millisecond,
		ColumnPublishAssetAppendWrite:         28 * time.Millisecond,
		ColumnPublishAssetAppendClose:         29 * time.Millisecond,
		ColumnPublishAssetAppendFileSync:      30 * time.Millisecond,
		ColumnPublishAssetAppendFileClose:     31 * time.Millisecond,
		ColumnPublishAssetAppendDirSync:       32 * time.Millisecond,
		ColumnPublishAssetAppendCleanup:       33 * time.Millisecond,
		ColumnPublishManifestEncode:           34 * time.Millisecond,
		ColumnPublishAssetClosureValidation:   35 * time.Millisecond,
		ColumnPublishRootDeltaConstruction:    36 * time.Millisecond,
		ColumnPublishSystemDeltaConstruction:  37 * time.Millisecond,
		ColumnPublishRootDeltaMaterialization: 38 * time.Millisecond,
		ColumnPublishRows:                     100,
		ColumnPublishPreparedAssets:           7,
		ColumnPublishRowAssetBytes:            101,
		ColumnPublishRowAssetCount:            1,
		ColumnPublishTypedColumnBytes:         202,
		ColumnPublishTypedColumnCount:         2,
		ColumnPublishDictionaryBytes:          303,
		ColumnPublishDictionaryCount:          3,
		ColumnPublishInt64Bytes:               404,
		ColumnPublishInt64Count:               4,
		ColumnPublishAggregateMetadataBytes:   505,
		ColumnPublishAggregateMetadataCount:   5,
		ColumnPublishSharedAppendBytes:        606,
		ColumnPublishSharedAppendCount:        6,
		ColumnPublishRequiredAssetBytes:       707,
		ColumnPublishManifestBytes:            808,
	}
	setOptionalDurationField(&stats, "ColumnPublishTypedColumnDictionaryBuild", 18*time.Millisecond)
	setOptionalDurationField(&stats, "ColumnPublishTypedColumnRowMaterialization", 19*time.Millisecond)
	setOptionalDurationField(&stats, "ColumnPublishTypedColumnPartBuild", 20*time.Millisecond)
	setOptionalDurationField(&stats, "ColumnPublishTypedColumnImageBuild", 21*time.Millisecond)
	accounting.add(stats)
	accounting.add(collections.CollectionInsertStats{
		Publish:                             10 * time.Millisecond,
		ColumnPublishAssetAppendFileSync:    4 * time.Millisecond,
		ColumnPublishAggregateMetadataBytes: 50,
		ColumnPublishSharedAppendBytes:      60,
	})

	got := accounting.result()
	if got == nil {
		t.Fatal("insert stats result=nil want populated")
	}
	if got.PublishSec != 0.1 || got.ColumnPublishBuildColumnDeltaSec != 0.01 || got.ColumnPublishCommitSec != 0.012 {
		t.Fatalf("column publish top-level timings mismatch: %+v", got)
	}
	if got.ColumnPublishAssetAppendFileSyncSec != 0.034 || got.ColumnPublishAssetAppendFileCloseSec != 0.031 || got.ColumnPublishAssetAppendDirSyncSec != 0.032 {
		t.Fatalf("column publish durable append timings mismatch: %+v", got)
	}
	if optionalDurationField(stats, "ColumnPublishTypedColumnDictionaryBuild") > 0 &&
		(got.ColumnPublishTypedDictionarySec != 0.018 || got.ColumnPublishTypedRowsSec != 0.019 || got.ColumnPublishTypedPartSec != 0.02 || got.ColumnPublishTypedImageSec != 0.021) {
		t.Fatalf("column publish typed-column subphase timings mismatch: %+v", got)
	}
	if got.ColumnPublishRows != 100 || got.ColumnPublishPreparedAssets != 7 || got.ColumnPublishRequiredAssetBytes != 707 || got.ColumnPublishManifestBytes != 808 {
		t.Fatalf("column publish counters mismatch: %+v", got)
	}
	if got.ColumnPublishAggregateMetadataBytes != 555 || got.ColumnPublishSharedAppendBytes != 666 {
		t.Fatalf("column publish byte counters mismatch: %+v", got)
	}
}

func TestOptionalDurationField(t *testing.T) {
	type sample struct {
		Duration time.Duration
		Count    int64
	}
	if got := optionalDurationField(sample{Duration: 12 * time.Millisecond}, "Duration"); got != 12*time.Millisecond {
		t.Fatalf("duration field=%s want 12ms", got)
	}
	if got := optionalDurationField(sample{Count: 12}, "Count"); got != 0 {
		t.Fatalf("non-duration field=%s want 0", got)
	}
	if got := optionalDurationField(collections.CollectionInsertStats{}, "ColumnPublishTypedColumnDictionaryBuild"); got < 0 {
		t.Fatalf("optional duration should never be negative: %s", got)
	}
}

func setOptionalDurationField(v any, name string, value time.Duration) {
	field := reflect.ValueOf(v).Elem().FieldByName(name)
	if !field.IsValid() || !field.CanSet() || field.Type() != reflect.TypeOf(time.Duration(0)) {
		return
	}
	field.SetInt(int64(value))
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
		InsertStatsRetainedPayloadSemanticStreamBlocks:   2,
		InsertStatsRetainedPayloadValueLogPointerizeSec:  0.03,
		InsertStatsRetainedPayloadValueLogValues:         8,
		InsertStatsRetainedPayloadValueLogBytes:          800,
		InsertStatsRetainedStreamValueLogPointerizeSec:   0.04,
		InsertStatsRetainedStreamValueLogValues:          2,
		InsertStatsRetainedStreamValueLogBytes:           200,
		InsertStatsPublishSec:                            1.4,
		InsertStatsColumnPublishBuildColumnDeltaSec:      0.2,
		InsertStatsColumnPublishCommitSec:                0.3,
		InsertStatsColumnPublishAssetPreparationSec:      0.4,
		InsertStatsColumnPublishRowAssetPrepareSec:       0.05,
		InsertStatsColumnPublishTypedColumnPrepareSec:    0.06,
		InsertStatsColumnPublishTypedDictionarySec:       0.061,
		InsertStatsColumnPublishTypedRowsSec:             0.062,
		InsertStatsColumnPublishTypedPartSec:             0.063,
		InsertStatsColumnPublishTypedImageSec:            0.064,
		InsertStatsColumnPublishDictionaryPrepareSec:     0.07,
		InsertStatsColumnPublishInt64PrepareSec:          0.08,
		InsertStatsColumnPublishAggregateMetadataSec:     0.09,
		InsertStatsColumnPublishRowSidecarSharedBuildSec: 0.11,
		InsertStatsColumnPublishAssetAppendSec:           0.12,
		InsertStatsColumnPublishAssetAppendOpenSec:       0.01,
		InsertStatsColumnPublishAssetAppendWriteSec:      0.02,
		InsertStatsColumnPublishAssetAppendCloseSec:      0.03,
		InsertStatsColumnPublishAssetAppendFileSyncSec:   0.025,
		InsertStatsColumnPublishAssetAppendFileCloseSec:  0.004,
		InsertStatsColumnPublishAssetAppendDirSyncSec:    0.003,
		InsertStatsColumnPublishRows:                     10,
		InsertStatsColumnPublishPreparedAssets:           6,
		InsertStatsColumnPublishRowAssetBytes:            100,
		InsertStatsColumnPublishTypedColumnBytes:         200,
		InsertStatsColumnPublishDictionaryBytes:          300,
		InsertStatsColumnPublishInt64Bytes:               400,
		InsertStatsColumnPublishAggregateMetadataBytes:   500,
		InsertStatsColumnPublishSharedAppendBytes:        600,
		InsertStatsColumnPublishRequiredAssetBytes:       700,
		InsertStatsColumnPublishManifestBytes:            800,
	}}}

	got := string(renderMarkdownReport(doc))
	for _, want := range []string{
		"## TreeDB Insert Stats",
		"| rows/scale | layout | load | insert | retained prepare | retained rows | declared rows | stream blocks | primary vlog pointerize | primary vlog values | primary vlog bytes | stream vlog pointerize | stream vlog values | stream vlog bytes |",
		"| 10 rows | column-store-full-prepared:json/full | 2.000s | 1.500s | 0.1000s | 10 | 10 | 2 | 0.0300s | 8 | 800 B | 0.0400s | 2 | 200 B |",
		"## TreeDB Column Publish Insert Stats",
		"| rows/scale | layout | load | insert | publish | build column delta | commit | asset prepare | row asset | typed column | typed dictionary | typed rows | typed part | typed image | dictionary | int64 | aggregate metadata | shared build | asset append | append open | append write | append close | file sync | file close | dir sync | rows | assets | row asset bytes | typed column bytes | dictionary bytes | int64 bytes | aggregate metadata bytes | shared append bytes | required asset bytes | manifest bytes |",
		"| 10 rows | column-store-full-prepared:json/full | 2.000s | 1.500s | 1.400s | 0.2000s | 0.3000s | 0.4000s | 0.0500s | 0.0600s | 0.0610s | 0.0620s | 0.0630s | 0.0640s | 0.0700s | 0.0800s | 0.0900s | 0.1100s | 0.1200s | 0.0100s | 0.0200s | 0.0300s | 0.0250s | 0.0040s | 0.0030s | 10 | 6 | 100 B | 200 B | 300 B | 400 B | 500 B | 600 B | 700 B | 800 B |",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("markdown missing %q\n%s", want, got)
		}
	}
}
