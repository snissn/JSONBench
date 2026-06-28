package main

import (
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
)

type insertStatsResult struct {
	RetainedPayloadPrepareSec             float64 `json:"retained_payload_prepare_seconds,omitempty"`
	RetainedPayloadRows                   int     `json:"retained_payload_rows,omitempty"`
	RetainedPayloadDeclaredRows           int     `json:"retained_payload_declared_rows,omitempty"`
	RetainedPayloadSemanticStreamBlocks   int     `json:"retained_payload_semantic_stream_blocks,omitempty"`
	RetainedPayloadValueLogPointerizeSec  float64 `json:"retained_payload_value_log_pointerize_seconds,omitempty"`
	RetainedPayloadValueLogValues         int     `json:"retained_payload_value_log_values,omitempty"`
	RetainedPayloadValueLogBytes          int64   `json:"retained_payload_value_log_bytes,omitempty"`
	RetainedStreamValueLogPointerizeSec   float64 `json:"retained_stream_value_log_pointerize_seconds,omitempty"`
	RetainedStreamValueLogValues          int     `json:"retained_stream_value_log_values,omitempty"`
	RetainedStreamValueLogBytes           int64   `json:"retained_stream_value_log_bytes,omitempty"`
	PublishSec                            float64 `json:"publish_seconds,omitempty"`
	ColumnPublishBuildColumnDeltaSec      float64 `json:"column_publish_build_column_delta_seconds,omitempty"`
	ColumnPublishBuildSystemDeltaSec      float64 `json:"column_publish_build_system_delta_seconds,omitempty"`
	ColumnPublishCommitSec                float64 `json:"column_publish_commit_seconds,omitempty"`
	ColumnPublishDocumentExtractionSec    float64 `json:"column_publish_document_extraction_seconds,omitempty"`
	ColumnPublishDeclaredColumnSec        float64 `json:"column_publish_declared_column_encoding_seconds,omitempty"`
	ColumnPublishAssetPreparationSec      float64 `json:"column_publish_asset_preparation_seconds,omitempty"`
	ColumnPublishRowAssetPrepareSec       float64 `json:"column_publish_row_asset_prepare_seconds,omitempty"`
	ColumnPublishTypedColumnPrepareSec    float64 `json:"column_publish_typed_column_prepare_seconds,omitempty"`
	ColumnPublishDictionaryPrepareSec     float64 `json:"column_publish_dictionary_sidecar_prepare_seconds,omitempty"`
	ColumnPublishInt64PrepareSec          float64 `json:"column_publish_int64_sidecar_prepare_seconds,omitempty"`
	ColumnPublishAggregateMetadataSec     float64 `json:"column_publish_aggregate_metadata_prepare_seconds,omitempty"`
	ColumnPublishRowSidecarSharedBuildSec float64 `json:"column_publish_row_sidecar_shared_build_seconds,omitempty"`
	ColumnPublishAssetAppendSec           float64 `json:"column_publish_asset_append_seconds,omitempty"`
	ColumnPublishAssetAppendOpenSec       float64 `json:"column_publish_asset_append_open_seconds,omitempty"`
	ColumnPublishAssetAppendWriteSec      float64 `json:"column_publish_asset_append_write_seconds,omitempty"`
	ColumnPublishAssetAppendCloseSec      float64 `json:"column_publish_asset_append_close_seconds,omitempty"`
	ColumnPublishAssetAppendFileSyncSec   float64 `json:"column_publish_asset_append_file_sync_seconds,omitempty"`
	ColumnPublishAssetAppendFileCloseSec  float64 `json:"column_publish_asset_append_file_close_seconds,omitempty"`
	ColumnPublishAssetAppendDirSyncSec    float64 `json:"column_publish_asset_append_dir_sync_seconds,omitempty"`
	ColumnPublishAssetAppendCleanupSec    float64 `json:"column_publish_asset_append_cleanup_seconds,omitempty"`
	ColumnPublishManifestEncodeSec        float64 `json:"column_publish_manifest_encode_seconds,omitempty"`
	ColumnPublishAssetClosureSec          float64 `json:"column_publish_asset_closure_validation_seconds,omitempty"`
	ColumnPublishRootDeltaSec             float64 `json:"column_publish_root_delta_construction_seconds,omitempty"`
	ColumnPublishSystemDeltaSec           float64 `json:"column_publish_system_delta_construction_seconds,omitempty"`
	ColumnPublishRootDeltaMaterializeSec  float64 `json:"column_publish_root_delta_materialization_seconds,omitempty"`
	ColumnPublishRows                     int     `json:"column_publish_rows,omitempty"`
	ColumnPublishPreparedAssets           int     `json:"column_publish_prepared_assets,omitempty"`
	ColumnPublishRowAssetBytes            int64   `json:"column_publish_row_asset_bytes,omitempty"`
	ColumnPublishRowAssetCount            int     `json:"column_publish_row_asset_count,omitempty"`
	ColumnPublishTypedColumnBytes         int64   `json:"column_publish_typed_column_bytes,omitempty"`
	ColumnPublishTypedColumnCount         int     `json:"column_publish_typed_column_count,omitempty"`
	ColumnPublishDictionaryBytes          int64   `json:"column_publish_dictionary_sidecar_bytes,omitempty"`
	ColumnPublishDictionaryCount          int     `json:"column_publish_dictionary_sidecar_count,omitempty"`
	ColumnPublishInt64Bytes               int64   `json:"column_publish_int64_sidecar_bytes,omitempty"`
	ColumnPublishInt64Count               int     `json:"column_publish_int64_sidecar_count,omitempty"`
	ColumnPublishAggregateMetadataBytes   int64   `json:"column_publish_aggregate_metadata_bytes,omitempty"`
	ColumnPublishAggregateMetadataCount   int     `json:"column_publish_aggregate_metadata_count,omitempty"`
	ColumnPublishSharedAppendBytes        int64   `json:"column_publish_shared_asset_append_bytes,omitempty"`
	ColumnPublishSharedAppendCount        int     `json:"column_publish_shared_asset_append_count,omitempty"`
	ColumnPublishRequiredAssetBytes       int64   `json:"column_publish_required_asset_bytes,omitempty"`
	ColumnPublishManifestBytes            int64   `json:"column_publish_manifest_bytes,omitempty"`
}

type insertStatsAccounting struct {
	retainedPayloadPrepare              time.Duration
	retainedPayloadRows                 int
	retainedPayloadDeclaredRows         int
	retainedPayloadSemanticBlocks       int
	retainedPayloadValueLogPointerize   time.Duration
	retainedPayloadValueLogValues       int
	retainedPayloadValueLogBytes        int64
	retainedStreamValueLogPointerize    time.Duration
	retainedStreamValueLogValues        int
	retainedStreamValueLogBytes         int64
	publish                             time.Duration
	columnPublishBuildColumnDelta       time.Duration
	columnPublishBuildSystemDelta       time.Duration
	columnPublishCommit                 time.Duration
	columnPublishDocumentExtraction     time.Duration
	columnPublishDeclaredColumn         time.Duration
	columnPublishAssetPreparation       time.Duration
	columnPublishRowAssetPrepare        time.Duration
	columnPublishTypedColumnPrepare     time.Duration
	columnPublishDictionaryPrepare      time.Duration
	columnPublishInt64Prepare           time.Duration
	columnPublishAggregateMetadata      time.Duration
	columnPublishRowSidecarSharedBuild  time.Duration
	columnPublishAssetAppend            time.Duration
	columnPublishAssetAppendOpen        time.Duration
	columnPublishAssetAppendWrite       time.Duration
	columnPublishAssetAppendClose       time.Duration
	columnPublishAssetAppendFileSync    time.Duration
	columnPublishAssetAppendFileClose   time.Duration
	columnPublishAssetAppendDirSync     time.Duration
	columnPublishAssetAppendCleanup     time.Duration
	columnPublishManifestEncode         time.Duration
	columnPublishAssetClosure           time.Duration
	columnPublishRootDelta              time.Duration
	columnPublishSystemDelta            time.Duration
	columnPublishRootDeltaMaterialize   time.Duration
	columnPublishRows                   int
	columnPublishPreparedAssets         int
	columnPublishRowAssetBytes          int64
	columnPublishRowAssetCount          int
	columnPublishTypedColumnBytes       int64
	columnPublishTypedColumnCount       int
	columnPublishDictionaryBytes        int64
	columnPublishDictionaryCount        int
	columnPublishInt64Bytes             int64
	columnPublishInt64Count             int
	columnPublishAggregateMetadataBytes int64
	columnPublishAggregateMetadataCount int
	columnPublishSharedAppendBytes      int64
	columnPublishSharedAppendCount      int
	columnPublishRequiredAssetBytes     int64
	columnPublishManifestBytes          int64
}

func (a *insertStatsAccounting) add(stats collections.CollectionInsertStats) {
	a.retainedPayloadPrepare += stats.RetainedPayloadPrepare
	a.retainedPayloadRows += stats.RetainedPayloadRows
	a.retainedPayloadDeclaredRows += stats.RetainedPayloadDeclaredRows
	a.retainedPayloadSemanticBlocks += stats.RetainedPayloadSemanticStreamBlocks
	a.retainedPayloadValueLogPointerize += stats.RetainedPayloadValueLogPointerize
	a.retainedPayloadValueLogValues += stats.RetainedPayloadValueLogValues
	a.retainedPayloadValueLogBytes += stats.RetainedPayloadValueLogBytes
	a.retainedStreamValueLogPointerize += stats.RetainedStreamValueLogPointerize
	a.retainedStreamValueLogValues += stats.RetainedStreamValueLogValues
	a.retainedStreamValueLogBytes += stats.RetainedStreamValueLogBytes
	a.publish += stats.Publish
	a.columnPublishBuildColumnDelta += stats.ColumnPublishBuildColumnDelta
	a.columnPublishBuildSystemDelta += stats.ColumnPublishBuildSystemDelta
	a.columnPublishCommit += stats.ColumnPublishCommit
	a.columnPublishDocumentExtraction += stats.ColumnPublishDocumentExtraction
	a.columnPublishDeclaredColumn += stats.ColumnPublishDeclaredColumnEncoding
	a.columnPublishAssetPreparation += stats.ColumnPublishAssetPreparation
	a.columnPublishRowAssetPrepare += stats.ColumnPublishRowAssetPreparation
	a.columnPublishTypedColumnPrepare += stats.ColumnPublishTypedColumnPreparation
	a.columnPublishDictionaryPrepare += stats.ColumnPublishDictionaryPreparation
	a.columnPublishInt64Prepare += stats.ColumnPublishInt64Preparation
	a.columnPublishAggregateMetadata += stats.ColumnPublishAggregateMetadataPrepare
	a.columnPublishRowSidecarSharedBuild += stats.ColumnPublishRowSidecarSharedBuild
	a.columnPublishAssetAppend += stats.ColumnPublishAssetAppend
	a.columnPublishAssetAppendOpen += stats.ColumnPublishAssetAppendOpen
	a.columnPublishAssetAppendWrite += stats.ColumnPublishAssetAppendWrite
	a.columnPublishAssetAppendClose += stats.ColumnPublishAssetAppendClose
	a.columnPublishAssetAppendFileSync += stats.ColumnPublishAssetAppendFileSync
	a.columnPublishAssetAppendFileClose += stats.ColumnPublishAssetAppendFileClose
	a.columnPublishAssetAppendDirSync += stats.ColumnPublishAssetAppendDirSync
	a.columnPublishAssetAppendCleanup += stats.ColumnPublishAssetAppendCleanup
	a.columnPublishManifestEncode += stats.ColumnPublishManifestEncode
	a.columnPublishAssetClosure += stats.ColumnPublishAssetClosureValidation
	a.columnPublishRootDelta += stats.ColumnPublishRootDeltaConstruction
	a.columnPublishSystemDelta += stats.ColumnPublishSystemDeltaConstruction
	a.columnPublishRootDeltaMaterialize += stats.ColumnPublishRootDeltaMaterialization
	a.columnPublishRows += stats.ColumnPublishRows
	a.columnPublishPreparedAssets += stats.ColumnPublishPreparedAssets
	a.columnPublishRowAssetBytes += stats.ColumnPublishRowAssetBytes
	a.columnPublishRowAssetCount += stats.ColumnPublishRowAssetCount
	a.columnPublishTypedColumnBytes += stats.ColumnPublishTypedColumnBytes
	a.columnPublishTypedColumnCount += stats.ColumnPublishTypedColumnCount
	a.columnPublishDictionaryBytes += stats.ColumnPublishDictionaryBytes
	a.columnPublishDictionaryCount += stats.ColumnPublishDictionaryCount
	a.columnPublishInt64Bytes += stats.ColumnPublishInt64Bytes
	a.columnPublishInt64Count += stats.ColumnPublishInt64Count
	a.columnPublishAggregateMetadataBytes += stats.ColumnPublishAggregateMetadataBytes
	a.columnPublishAggregateMetadataCount += stats.ColumnPublishAggregateMetadataCount
	a.columnPublishSharedAppendBytes += stats.ColumnPublishSharedAppendBytes
	a.columnPublishSharedAppendCount += stats.ColumnPublishSharedAppendCount
	a.columnPublishRequiredAssetBytes += stats.ColumnPublishRequiredAssetBytes
	a.columnPublishManifestBytes += stats.ColumnPublishManifestBytes
}

func (a insertStatsAccounting) result() *insertStatsResult {
	if !a.hasRetainedStats() && !a.hasColumnPublishStats() {
		return nil
	}
	return &insertStatsResult{
		RetainedPayloadPrepareSec:             a.retainedPayloadPrepare.Seconds(),
		RetainedPayloadRows:                   a.retainedPayloadRows,
		RetainedPayloadDeclaredRows:           a.retainedPayloadDeclaredRows,
		RetainedPayloadSemanticStreamBlocks:   a.retainedPayloadSemanticBlocks,
		RetainedPayloadValueLogPointerizeSec:  a.retainedPayloadValueLogPointerize.Seconds(),
		RetainedPayloadValueLogValues:         a.retainedPayloadValueLogValues,
		RetainedPayloadValueLogBytes:          a.retainedPayloadValueLogBytes,
		RetainedStreamValueLogPointerizeSec:   a.retainedStreamValueLogPointerize.Seconds(),
		RetainedStreamValueLogValues:          a.retainedStreamValueLogValues,
		RetainedStreamValueLogBytes:           a.retainedStreamValueLogBytes,
		PublishSec:                            a.publish.Seconds(),
		ColumnPublishBuildColumnDeltaSec:      a.columnPublishBuildColumnDelta.Seconds(),
		ColumnPublishBuildSystemDeltaSec:      a.columnPublishBuildSystemDelta.Seconds(),
		ColumnPublishCommitSec:                a.columnPublishCommit.Seconds(),
		ColumnPublishDocumentExtractionSec:    a.columnPublishDocumentExtraction.Seconds(),
		ColumnPublishDeclaredColumnSec:        a.columnPublishDeclaredColumn.Seconds(),
		ColumnPublishAssetPreparationSec:      a.columnPublishAssetPreparation.Seconds(),
		ColumnPublishRowAssetPrepareSec:       a.columnPublishRowAssetPrepare.Seconds(),
		ColumnPublishTypedColumnPrepareSec:    a.columnPublishTypedColumnPrepare.Seconds(),
		ColumnPublishDictionaryPrepareSec:     a.columnPublishDictionaryPrepare.Seconds(),
		ColumnPublishInt64PrepareSec:          a.columnPublishInt64Prepare.Seconds(),
		ColumnPublishAggregateMetadataSec:     a.columnPublishAggregateMetadata.Seconds(),
		ColumnPublishRowSidecarSharedBuildSec: a.columnPublishRowSidecarSharedBuild.Seconds(),
		ColumnPublishAssetAppendSec:           a.columnPublishAssetAppend.Seconds(),
		ColumnPublishAssetAppendOpenSec:       a.columnPublishAssetAppendOpen.Seconds(),
		ColumnPublishAssetAppendWriteSec:      a.columnPublishAssetAppendWrite.Seconds(),
		ColumnPublishAssetAppendCloseSec:      a.columnPublishAssetAppendClose.Seconds(),
		ColumnPublishAssetAppendFileSyncSec:   a.columnPublishAssetAppendFileSync.Seconds(),
		ColumnPublishAssetAppendFileCloseSec:  a.columnPublishAssetAppendFileClose.Seconds(),
		ColumnPublishAssetAppendDirSyncSec:    a.columnPublishAssetAppendDirSync.Seconds(),
		ColumnPublishAssetAppendCleanupSec:    a.columnPublishAssetAppendCleanup.Seconds(),
		ColumnPublishManifestEncodeSec:        a.columnPublishManifestEncode.Seconds(),
		ColumnPublishAssetClosureSec:          a.columnPublishAssetClosure.Seconds(),
		ColumnPublishRootDeltaSec:             a.columnPublishRootDelta.Seconds(),
		ColumnPublishSystemDeltaSec:           a.columnPublishSystemDelta.Seconds(),
		ColumnPublishRootDeltaMaterializeSec:  a.columnPublishRootDeltaMaterialize.Seconds(),
		ColumnPublishRows:                     a.columnPublishRows,
		ColumnPublishPreparedAssets:           a.columnPublishPreparedAssets,
		ColumnPublishRowAssetBytes:            a.columnPublishRowAssetBytes,
		ColumnPublishRowAssetCount:            a.columnPublishRowAssetCount,
		ColumnPublishTypedColumnBytes:         a.columnPublishTypedColumnBytes,
		ColumnPublishTypedColumnCount:         a.columnPublishTypedColumnCount,
		ColumnPublishDictionaryBytes:          a.columnPublishDictionaryBytes,
		ColumnPublishDictionaryCount:          a.columnPublishDictionaryCount,
		ColumnPublishInt64Bytes:               a.columnPublishInt64Bytes,
		ColumnPublishInt64Count:               a.columnPublishInt64Count,
		ColumnPublishAggregateMetadataBytes:   a.columnPublishAggregateMetadataBytes,
		ColumnPublishAggregateMetadataCount:   a.columnPublishAggregateMetadataCount,
		ColumnPublishSharedAppendBytes:        a.columnPublishSharedAppendBytes,
		ColumnPublishSharedAppendCount:        a.columnPublishSharedAppendCount,
		ColumnPublishRequiredAssetBytes:       a.columnPublishRequiredAssetBytes,
		ColumnPublishManifestBytes:            a.columnPublishManifestBytes,
	}
}

func (a insertStatsAccounting) hasRetainedStats() bool {
	return a.retainedPayloadPrepare > 0 ||
		a.retainedPayloadRows > 0 ||
		a.retainedPayloadDeclaredRows > 0 ||
		a.retainedPayloadSemanticBlocks > 0 ||
		a.retainedPayloadValueLogPointerize > 0 ||
		a.retainedPayloadValueLogValues > 0 ||
		a.retainedPayloadValueLogBytes > 0 ||
		a.retainedStreamValueLogPointerize > 0 ||
		a.retainedStreamValueLogValues > 0 ||
		a.retainedStreamValueLogBytes > 0
}

func (a insertStatsAccounting) hasColumnPublishStats() bool {
	return a.publish > 0 ||
		a.columnPublishBuildColumnDelta > 0 ||
		a.columnPublishBuildSystemDelta > 0 ||
		a.columnPublishCommit > 0 ||
		a.columnPublishDocumentExtraction > 0 ||
		a.columnPublishDeclaredColumn > 0 ||
		a.columnPublishAssetPreparation > 0 ||
		a.columnPublishRowAssetPrepare > 0 ||
		a.columnPublishTypedColumnPrepare > 0 ||
		a.columnPublishDictionaryPrepare > 0 ||
		a.columnPublishInt64Prepare > 0 ||
		a.columnPublishAggregateMetadata > 0 ||
		a.columnPublishRowSidecarSharedBuild > 0 ||
		a.columnPublishAssetAppend > 0 ||
		a.columnPublishAssetAppendOpen > 0 ||
		a.columnPublishAssetAppendWrite > 0 ||
		a.columnPublishAssetAppendClose > 0 ||
		a.columnPublishAssetAppendFileSync > 0 ||
		a.columnPublishAssetAppendFileClose > 0 ||
		a.columnPublishAssetAppendDirSync > 0 ||
		a.columnPublishAssetAppendCleanup > 0 ||
		a.columnPublishManifestEncode > 0 ||
		a.columnPublishAssetClosure > 0 ||
		a.columnPublishRootDelta > 0 ||
		a.columnPublishSystemDelta > 0 ||
		a.columnPublishRootDeltaMaterialize > 0 ||
		a.columnPublishRows > 0 ||
		a.columnPublishPreparedAssets > 0 ||
		a.columnPublishRowAssetBytes > 0 ||
		a.columnPublishRowAssetCount > 0 ||
		a.columnPublishTypedColumnBytes > 0 ||
		a.columnPublishTypedColumnCount > 0 ||
		a.columnPublishDictionaryBytes > 0 ||
		a.columnPublishDictionaryCount > 0 ||
		a.columnPublishInt64Bytes > 0 ||
		a.columnPublishInt64Count > 0 ||
		a.columnPublishAggregateMetadataBytes > 0 ||
		a.columnPublishAggregateMetadataCount > 0 ||
		a.columnPublishSharedAppendBytes > 0 ||
		a.columnPublishSharedAppendCount > 0 ||
		a.columnPublishRequiredAssetBytes > 0 ||
		a.columnPublishManifestBytes > 0
}
