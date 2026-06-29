package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type reportConfig struct {
	ResultsDir           string
	DuckDBResultsDir     string
	DuckDBScales         map[string]struct{}
	ClickHouseResultsDir string
	ClickHouseScales     map[string]struct{}
	Out                  string
	JSONOut              string
}

type reportDocument struct {
	SchemaVersion string      `json:"schema_version"`
	GeneratedAt   string      `json:"generated_at"`
	Rows          []reportRow `json:"rows"`
}

type reportRow struct {
	System                                           string    `json:"system"`
	Engine                                           string    `json:"engine,omitempty"`
	Scale                                            string    `json:"scale"`
	RequestedRows                                    int       `json:"requested_rows,omitempty"`
	DatasetSize                                      int       `json:"dataset_size"`
	RowCount                                         int       `json:"row_count"`
	InputRows                                        int       `json:"input_rows,omitempty"`
	SkippedInvalidJSONRows                           int       `json:"skipped_invalid_json_rows,omitempty"`
	Format                                           string    `json:"format,omitempty"`
	StorageLayout                                    string    `json:"storage_layout,omitempty"`
	Projection                                       string    `json:"projection,omitempty"`
	QueryMode                                        string    `json:"query_mode,omitempty"`
	MetadataMode                                     string    `json:"metadata_mode,omitempty"`
	Profile                                          string    `json:"profile,omitempty"`
	DataRoot                                         string    `json:"data_root,omitempty"`
	DataShape                                        string    `json:"data_shape,omitempty"`
	ExecutionMode                                    string    `json:"execution_mode,omitempty"`
	QueryPath                                        string    `json:"query_path,omitempty"`
	StorageSource                                    string    `json:"storage_source,omitempty"`
	FallbackReason                                   string    `json:"fallback_reason,omitempty"`
	MetadataDataScanPath                             string    `json:"metadata_data_scan_path,omitempty"`
	SortLayout                                       string    `json:"sort_layout,omitempty"`
	CompressionMode                                  string    `json:"compression_mode,omitempty"`
	MutationMode                                     string    `json:"mutation_mode,omitempty"`
	DocumentScanFallback                             bool      `json:"document_scan_fallback"`
	RetainedPayloadPolicy                            string    `json:"retained_payload_policy,omitempty"`
	RetainedPayloadEncoding                          string    `json:"retained_payload_encoding,omitempty"`
	RetainedPayloadEncodingStatus                    string    `json:"retained_payload_encoding_status,omitempty"`
	ColumnReconstructionPolicy                       string    `json:"column_reconstruction_policy,omitempty"`
	TypedColumnOwner                                 string    `json:"typed_column_owner,omitempty"`
	ReconstructionStatus                             string    `json:"reconstruction_status,omitempty"`
	Query                                            string    `json:"query"`
	BestSec                                          float64   `json:"best_seconds"`
	MedianSec                                        float64   `json:"median_seconds"`
	AttemptsSec                                      []float64 `json:"attempts_seconds"`
	RowsScanned                                      int       `json:"rows_scanned,omitempty"`
	TypedCellsVisited                                *int      `json:"typed_cells_visited,omitempty"`
	TypedCellsBasis                                  string    `json:"typed_cells_basis,omitempty"`
	ExpressionKind                                   string    `json:"expression_kind,omitempty"`
	PrecomputedExpressionUsed                        *bool     `json:"precomputed_expression_used,omitempty"`
	RowsMatched                                      int       `json:"rows_matched,omitempty"`
	ReduceRows                                       int       `json:"reduce_rows,omitempty"`
	ResultGroups                                     int       `json:"result_groups,omitempty"`
	PredicateCount                                   int       `json:"predicate_count,omitempty"`
	TopKLimit                                        int       `json:"topk_limit,omitempty"`
	TopKCandidates                                   int       `json:"topk_candidates,omitempty"`
	BoundedTopKUsed                                  bool      `json:"bounded_topk_used,omitempty"`
	TimeOrderTopKUsed                                bool      `json:"time_order_topk_used,omitempty"`
	SortKeyMarkChecks                                int       `json:"sort_key_mark_checks,omitempty"`
	SortKeyMarkMatches                               int       `json:"sort_key_mark_matches,omitempty"`
	SortKeyMarkSkips                                 int       `json:"sort_key_mark_skips,omitempty"`
	SortKeyMarkFallbackReason                        string    `json:"sort_key_mark_fallback_reason,omitempty"`
	SortedGroupedDistinctReady                       bool      `json:"sorted_grouped_distinct_ready,omitempty"`
	SortedGroupedDistinctUsed                        bool      `json:"sorted_grouped_distinct_used,omitempty"`
	SortedGroupedDistinctFallback                    string    `json:"sorted_grouped_distinct_fallback_reason,omitempty"`
	DenseGroupCountUsed                              bool      `json:"dense_group_count_used,omitempty"`
	DenseGroupCountDistinctUsed                      bool      `json:"dense_group_count_distinct_used,omitempty"`
	DenseGroupHourCountUsed                          bool      `json:"dense_group_hour_count_used,omitempty"`
	DenseInt64SpanUsed                               bool      `json:"dense_int64_span_used,omitempty"`
	DictionaryCodeHits                               int       `json:"dictionary_code_hits,omitempty"`
	PredicateDictionaryCodeHits                      int       `json:"predicate_dictionary_code_hits,omitempty"`
	Int64ValueHits                                   int       `json:"int64_value_hits,omitempty"`
	DecodedPayloadBytes                              uint64    `json:"decoded_payload_bytes,omitempty"`
	DecodedMetadataBytes                             uint64    `json:"decoded_metadata_bytes,omitempty"`
	PhysicalBytesScanned                             int64     `json:"physical_bytes_scanned,omitempty"`
	MappedBytes                                      uint64    `json:"mapped_bytes,omitempty"`
	HeapCopyBytes                                    uint64    `json:"heap_copy_bytes,omitempty"`
	RowMaterializations                              int       `json:"row_materializations,omitempty"`
	DocumentMaterializations                         int       `json:"document_materializations,omitempty"`
	FallbackReads                                    int       `json:"fallback_reads,omitempty"`
	AggregateMetadataUsed                            bool      `json:"aggregate_metadata_used,omitempty"`
	AggregateMetadataRefs                            int       `json:"aggregate_metadata_refs,omitempty"`
	AggregateMetadataStorageBytes                    int64     `json:"aggregate_metadata_storage_bytes,omitempty"`
	AggregateMetadataSidecarBytes                    int64     `json:"aggregate_metadata_sidecar_bytes,omitempty"`
	AggregateMetadataEmbeddedBytes                   int64     `json:"aggregate_metadata_embedded_bytes,omitempty"`
	MetadataCostStorageBytes                         int64     `json:"metadata_cost_storage_bytes,omitempty"`
	MetadataCostStorageBasis                         string    `json:"metadata_cost_storage_basis,omitempty"`
	MetadataCostInsertSec                            float64   `json:"metadata_cost_insert_seconds,omitempty"`
	MetadataCostInsertBasis                          string    `json:"metadata_cost_insert_basis,omitempty"`
	JSONReconstructionUsed                           bool      `json:"json_reconstruction_used,omitempty"`
	TypedColumnOneShotCacheHit                       bool      `json:"typed_column_one_shot_cache_hit,omitempty"`
	TypedColumnOneShotCacheMiss                      bool      `json:"typed_column_one_shot_cache_miss,omitempty"`
	TypedColumnOneShotCacheBuild                     bool      `json:"typed_column_one_shot_cache_build,omitempty"`
	TypedColumnOneShotBuildNanos                     int64     `json:"typed_column_one_shot_build_nanos,omitempty"`
	TypedColumnPrepareWorkerCount                    int       `json:"typed_column_prepare_worker_count,omitempty"`
	TypedColumnPreparePlanNanos                      int64     `json:"typed_column_prepare_plan_nanos,omitempty"`
	TypedColumnPrepareRefsNanos                      int64     `json:"typed_column_prepare_refs_nanos,omitempty"`
	TypedColumnPreparePairingNanos                   int64     `json:"typed_column_prepare_pairing_nanos,omitempty"`
	TypedColumnPreparePartDecodeNanos                int64     `json:"typed_column_prepare_part_decode_nanos,omitempty"`
	TypedColumnPreparePostPrepareNanos               int64     `json:"typed_column_prepare_post_prepare_nanos,omitempty"`
	TypedColumnPrepareSummaryNanos                   int64     `json:"typed_column_prepare_summary_nanos,omitempty"`
	TypedColumnOneShotCacheStoreNanos                int64     `json:"typed_column_one_shot_cache_store_nanos,omitempty"`
	TypedColumnPrepareReadImageNanos                 int64     `json:"typed_column_prepare_read_image_nanos,omitempty"`
	TypedColumnPrepareStateBuildNanos                int64     `json:"typed_column_prepare_state_build_nanos,omitempty"`
	TypedColumnPrepareDictionaryNanos                int64     `json:"typed_column_prepare_dictionary_nanos,omitempty"`
	TypedColumnPreparePruningNanos                   int64     `json:"typed_column_prepare_pruning_nanos,omitempty"`
	TypedColumnPrepareSortKeyNanos                   int64     `json:"typed_column_prepare_sort_key_nanos,omitempty"`
	TypedColumnPrepareStatsNanos                     int64     `json:"typed_column_prepare_stats_nanos,omitempty"`
	TypedColumnPrepareRangeReadNanos                 int64     `json:"typed_column_prepare_range_read_nanos,omitempty"`
	TypedColumnPrepareRangeReadBytes                 int64     `json:"typed_column_prepare_range_read_bytes,omitempty"`
	TypedColumnPrepareAdapterNanos                   int64     `json:"typed_column_prepare_adapter_nanos,omitempty"`
	TypedColumnPrepareDenseGroupNanos                int64     `json:"typed_column_prepare_dense_group_nanos,omitempty"`
	TypedColumnPrepareDenseValueNanos                int64     `json:"typed_column_prepare_dense_value_nanos,omitempty"`
	TypedColumnPrepareDensePredicateNanos            int64     `json:"typed_column_prepare_dense_predicate_nanos,omitempty"`
	TypedColumnPrepareDensePreapplyNanos             int64     `json:"typed_column_prepare_dense_preapply_nanos,omitempty"`
	TypedColumnPrepareQ2GroupRankNanos               int64     `json:"typed_column_prepare_q2_group_rank_nanos,omitempty"`
	TypedColumnPrepareQ2DistinctRankNanos            int64     `json:"typed_column_prepare_q2_distinct_rank_nanos,omitempty"`
	TypedColumnPrepareQ2LocalRankNanos               int64     `json:"typed_column_prepare_q2_local_rank_nanos,omitempty"`
	PrepareSetupNanos                                int64     `json:"prepare_setup_nanos,omitempty"`
	RunNanos                                         int64     `json:"run_nanos,omitempty"`
	ResultRenderNanos                                int64     `json:"result_render_nanos,omitempty"`
	HashNanos                                        int64     `json:"hash_nanos,omitempty"`
	RenderHashNanos                                  int64     `json:"render_hash_nanos,omitempty"`
	AttemptWallNanos                                 int64     `json:"attempt_wall_nanos,omitempty"`
	TotalQueryNanos                                  int64     `json:"total_query_nanos,omitempty"`
	PhysicalQueryCount                               int       `json:"physical_query_count,omitempty"`
	StorageBytes                                     int64     `json:"storage_bytes,omitempty"`
	StorageGrossBytes                                int64     `json:"storage_gross_bytes,omitempty"`
	StorageExcludedBytes                             int64     `json:"storage_excluded_bytes,omitempty"`
	StorageDurableBytesWALExcluded                   int64     `json:"storage_durable_bytes_wal_excluded"`
	StorageWALBytesExcludedFromDurable               int64     `json:"storage_wal_bytes_excluded_from_durable_storage"`
	StorageWALExcludedNote                           string    `json:"storage_durable_bytes_wal_excluded_note,omitempty"`
	StorageColumnAssetBytes                          int64     `json:"storage_column_asset_bytes,omitempty"`
	StorageTypedColumnPartBytes                      int64     `json:"storage_typed_column_part_bytes,omitempty"`
	StorageTypedColumnSectionBytes                   int64     `json:"storage_typed_column_section_bytes,omitempty"`
	StoragePrimaryIndexBytes                         int64     `json:"storage_primary_index_bytes,omitempty"`
	StorageLeafVLogBytes                             int64     `json:"storage_leaf_vlog_bytes,omitempty"`
	StorageWALBytes                                  int64     `json:"storage_wal_bytes,omitempty"`
	BaselineDataBytes                                int64     `json:"baseline_data_bytes,omitempty"`
	BaselineIndexBytes                               int64     `json:"baseline_index_bytes,omitempty"`
	StorageAccountingScope                           string    `json:"storage_accounting_scope,omitempty"`
	StorageMeasurementPhase                          string    `json:"storage_measurement_phase,omitempty"`
	LoadSec                                          float64   `json:"load_seconds,omitempty"`
	InsertSec                                        float64   `json:"insert_seconds,omitempty"`
	InsertStatsRetainedPayloadPrepareSec             float64   `json:"insert_stats_retained_payload_prepare_seconds,omitempty"`
	InsertStatsRetainedPayloadRows                   int       `json:"insert_stats_retained_payload_rows,omitempty"`
	InsertStatsRetainedPayloadDeclaredRows           int       `json:"insert_stats_retained_payload_declared_rows,omitempty"`
	InsertStatsRetainedPayloadSemanticStreamBlocks   int       `json:"insert_stats_retained_payload_semantic_stream_blocks,omitempty"`
	InsertStatsRetainedPayloadValueLogPointerizeSec  float64   `json:"insert_stats_retained_payload_value_log_pointerize_seconds,omitempty"`
	InsertStatsRetainedPayloadValueLogValues         int       `json:"insert_stats_retained_payload_value_log_values,omitempty"`
	InsertStatsRetainedPayloadValueLogBytes          int64     `json:"insert_stats_retained_payload_value_log_bytes,omitempty"`
	InsertStatsRetainedStreamValueLogPointerizeSec   float64   `json:"insert_stats_retained_stream_value_log_pointerize_seconds,omitempty"`
	InsertStatsRetainedStreamValueLogValues          int       `json:"insert_stats_retained_stream_value_log_values,omitempty"`
	InsertStatsRetainedStreamValueLogBytes           int64     `json:"insert_stats_retained_stream_value_log_bytes,omitempty"`
	InsertStatsPublishSec                            float64   `json:"insert_stats_publish_seconds,omitempty"`
	InsertStatsColumnPublishBuildColumnDeltaSec      float64   `json:"insert_stats_column_publish_build_column_delta_seconds,omitempty"`
	InsertStatsColumnPublishBuildSystemDeltaSec      float64   `json:"insert_stats_column_publish_build_system_delta_seconds,omitempty"`
	InsertStatsColumnPublishCommitSec                float64   `json:"insert_stats_column_publish_commit_seconds,omitempty"`
	InsertStatsColumnPublishDocumentExtractionSec    float64   `json:"insert_stats_column_publish_document_extraction_seconds,omitempty"`
	InsertStatsColumnPublishDeclaredColumnSec        float64   `json:"insert_stats_column_publish_declared_column_encoding_seconds,omitempty"`
	InsertStatsColumnPublishAssetPreparationSec      float64   `json:"insert_stats_column_publish_asset_preparation_seconds,omitempty"`
	InsertStatsColumnPublishRowAssetPrepareSec       float64   `json:"insert_stats_column_publish_row_asset_prepare_seconds,omitempty"`
	InsertStatsColumnPublishTypedColumnPrepareSec    float64   `json:"insert_stats_column_publish_typed_column_prepare_seconds,omitempty"`
	InsertStatsColumnPublishTypedDictionarySec       float64   `json:"insert_stats_column_publish_typed_column_dictionary_build_seconds,omitempty"`
	InsertStatsColumnPublishTypedRowsSec             float64   `json:"insert_stats_column_publish_typed_column_row_materialization_seconds,omitempty"`
	InsertStatsColumnPublishTypedPartSec             float64   `json:"insert_stats_column_publish_typed_column_part_build_seconds,omitempty"`
	InsertStatsColumnPublishTypedImageSec            float64   `json:"insert_stats_column_publish_typed_column_image_build_seconds,omitempty"`
	InsertStatsColumnPublishDictionaryPrepareSec     float64   `json:"insert_stats_column_publish_dictionary_sidecar_prepare_seconds,omitempty"`
	InsertStatsColumnPublishInt64PrepareSec          float64   `json:"insert_stats_column_publish_int64_sidecar_prepare_seconds,omitempty"`
	InsertStatsColumnPublishAggregateMetadataSec     float64   `json:"insert_stats_column_publish_aggregate_metadata_prepare_seconds,omitempty"`
	InsertStatsColumnPublishRowSidecarSharedBuildSec float64   `json:"insert_stats_column_publish_row_sidecar_shared_build_seconds,omitempty"`
	InsertStatsColumnPublishAssetAppendSec           float64   `json:"insert_stats_column_publish_asset_append_seconds,omitempty"`
	InsertStatsColumnPublishAssetAppendOpenSec       float64   `json:"insert_stats_column_publish_asset_append_open_seconds,omitempty"`
	InsertStatsColumnPublishAssetAppendWriteSec      float64   `json:"insert_stats_column_publish_asset_append_write_seconds,omitempty"`
	InsertStatsColumnPublishAssetAppendCloseSec      float64   `json:"insert_stats_column_publish_asset_append_close_seconds,omitempty"`
	InsertStatsColumnPublishAssetAppendFileSyncSec   float64   `json:"insert_stats_column_publish_asset_append_file_sync_seconds,omitempty"`
	InsertStatsColumnPublishAssetAppendFileCloseSec  float64   `json:"insert_stats_column_publish_asset_append_file_close_seconds,omitempty"`
	InsertStatsColumnPublishAssetAppendDirSyncSec    float64   `json:"insert_stats_column_publish_asset_append_dir_sync_seconds,omitempty"`
	InsertStatsColumnPublishAssetAppendCleanupSec    float64   `json:"insert_stats_column_publish_asset_append_cleanup_seconds,omitempty"`
	InsertStatsColumnPublishManifestEncodeSec        float64   `json:"insert_stats_column_publish_manifest_encode_seconds,omitempty"`
	InsertStatsColumnPublishAssetClosureSec          float64   `json:"insert_stats_column_publish_asset_closure_validation_seconds,omitempty"`
	InsertStatsColumnPublishRootDeltaSec             float64   `json:"insert_stats_column_publish_root_delta_construction_seconds,omitempty"`
	InsertStatsColumnPublishSystemDeltaSec           float64   `json:"insert_stats_column_publish_system_delta_construction_seconds,omitempty"`
	InsertStatsColumnPublishRootDeltaMaterializeSec  float64   `json:"insert_stats_column_publish_root_delta_materialization_seconds,omitempty"`
	InsertStatsColumnPublishRows                     int       `json:"insert_stats_column_publish_rows,omitempty"`
	InsertStatsColumnPublishPreparedAssets           int       `json:"insert_stats_column_publish_prepared_assets,omitempty"`
	InsertStatsColumnPublishRowAssetBytes            int64     `json:"insert_stats_column_publish_row_asset_bytes,omitempty"`
	InsertStatsColumnPublishRowAssetCount            int       `json:"insert_stats_column_publish_row_asset_count,omitempty"`
	InsertStatsColumnPublishTypedColumnBytes         int64     `json:"insert_stats_column_publish_typed_column_bytes,omitempty"`
	InsertStatsColumnPublishTypedColumnCount         int       `json:"insert_stats_column_publish_typed_column_count,omitempty"`
	InsertStatsColumnPublishDictionaryBytes          int64     `json:"insert_stats_column_publish_dictionary_sidecar_bytes,omitempty"`
	InsertStatsColumnPublishDictionaryCount          int       `json:"insert_stats_column_publish_dictionary_sidecar_count,omitempty"`
	InsertStatsColumnPublishInt64Bytes               int64     `json:"insert_stats_column_publish_int64_sidecar_bytes,omitempty"`
	InsertStatsColumnPublishInt64Count               int       `json:"insert_stats_column_publish_int64_sidecar_count,omitempty"`
	InsertStatsColumnPublishAggregateMetadataBytes   int64     `json:"insert_stats_column_publish_aggregate_metadata_bytes,omitempty"`
	InsertStatsColumnPublishAggregateMetadataCount   int       `json:"insert_stats_column_publish_aggregate_metadata_count,omitempty"`
	InsertStatsColumnPublishSharedAppendBytes        int64     `json:"insert_stats_column_publish_shared_asset_append_bytes,omitempty"`
	InsertStatsColumnPublishSharedAppendCount        int       `json:"insert_stats_column_publish_shared_asset_append_count,omitempty"`
	InsertStatsColumnPublishRequiredAssetBytes       int64     `json:"insert_stats_column_publish_required_asset_bytes,omitempty"`
	InsertStatsColumnPublishManifestBytes            int64     `json:"insert_stats_column_publish_manifest_bytes,omitempty"`
	CompactionSec                                    float64   `json:"compaction_seconds,omitempty"`
	Compacted                                        bool      `json:"compacted,omitempty"`
	RetainsJSON                                      *bool     `json:"retains_json_structure,omitempty"`
	ReconstructionValid                              *bool     `json:"reconstruction_valid,omitempty"`
	Source                                           string    `json:"source"`
}

type jsonBenchBaselineResult struct {
	System             string      `json:"system"`
	DatasetSize        int         `json:"dataset_size"`
	NumLoadedDocuments int         `json:"num_loaded_documents"`
	RequestedRows      int         `json:"requested_rows,omitempty"`
	TotalSize          int64       `json:"total_size"`
	DataSize           int64       `json:"data_size"`
	IndexSize          int64       `json:"index_size"`
	Result             [][]float64 `json:"result"`
}

func parseReportFlags(args []string) (reportConfig, error) {
	cfg := reportConfig{
		ResultsDir:           "results",
		DuckDBResultsDir:     "",
		ClickHouseResultsDir: "",
		Out:                  "results/report.md",
		JSONOut:              "results/report.json",
	}
	var duckDBScales string
	var clickHouseScales string
	fs := flag.NewFlagSet("report", flag.ContinueOnError)
	flagUsage(fs, "Aggregate TreeDB JSONBench cell results and explicitly supplied local baselines.")
	fs.StringVar(&cfg.ResultsDir, "results-dir", cfg.ResultsDir, "Directory containing TreeDB result JSON files")
	fs.StringVar(&cfg.DuckDBResultsDir, "duckdb-results-dir", cfg.DuckDBResultsDir, "DuckDB result JSON directory; empty disables DuckDB import")
	fs.StringVar(&duckDBScales, "duckdb-scales", "1m,10m", "Comma-separated DuckDB baseline scales to import, or all")
	fs.StringVar(&cfg.ClickHouseResultsDir, "clickhouse-results-dir", cfg.ClickHouseResultsDir, "JSONBench clickhouse/results directory; empty disables ClickHouse import")
	fs.StringVar(&clickHouseScales, "clickhouse-scales", "1m,10m", "Comma-separated ClickHouse baseline scales to import, or all")
	fs.StringVar(&cfg.Out, "out", cfg.Out, "Markdown report output path")
	fs.StringVar(&cfg.JSONOut, "json-out", cfg.JSONOut, "Machine-readable report JSON path")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	var err error
	cfg.DuckDBScales, err = parseScaleFilter(duckDBScales)
	if err != nil {
		return cfg, err
	}
	cfg.ClickHouseScales, err = parseScaleFilter(clickHouseScales)
	if err != nil {
		return cfg, err
	}
	cfg.ResultsDir, err = expandPath(cfg.ResultsDir)
	if err != nil {
		return cfg, err
	}
	if strings.TrimSpace(cfg.DuckDBResultsDir) != "" {
		cfg.DuckDBResultsDir, err = expandPath(cfg.DuckDBResultsDir)
		if err != nil {
			return cfg, err
		}
	}
	if strings.TrimSpace(cfg.ClickHouseResultsDir) != "" {
		cfg.ClickHouseResultsDir, err = expandPath(cfg.ClickHouseResultsDir)
		if err != nil {
			return cfg, err
		}
	}
	if strings.TrimSpace(cfg.Out) != "" {
		cfg.Out, err = expandPath(cfg.Out)
		if err != nil {
			return cfg, err
		}
	}
	if strings.TrimSpace(cfg.JSONOut) != "" {
		cfg.JSONOut, err = expandPath(cfg.JSONOut)
		if err != nil {
			return cfg, err
		}
	}
	return cfg, nil
}

func writeReport(cfg reportConfig) error {
	rows, err := collectReportRows(cfg)
	if err != nil {
		return err
	}
	sortReportRows(rows)
	doc := reportDocument{
		SchemaVersion: "jsonbench-treedb-report/v1",
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
		Rows:          rows,
	}
	if cfg.JSONOut != "" {
		if err := writeJSON(cfg.JSONOut, doc); err != nil {
			return err
		}
	}
	md := renderMarkdownReport(doc)
	if cfg.Out == "" || cfg.Out == "-" {
		_, err := os.Stdout.Write(md)
		return err
	}
	if err := os.MkdirAll(parentDir(cfg.Out), 0o755); err != nil {
		return err
	}
	return os.WriteFile(cfg.Out, md, 0o644)
}

func collectReportRows(cfg reportConfig) ([]reportRow, error) {
	var rows []reportRow
	treeRows, err := collectTreeDBRows(cfg.ResultsDir)
	if err != nil {
		return nil, err
	}
	rows = append(rows, treeRows...)
	if strings.TrimSpace(cfg.DuckDBResultsDir) != "" {
		duckRows, err := collectBaselineRows(cfg.DuckDBResultsDir, cfg.DuckDBScales, "DuckDB", "json-column-sql")
		if err != nil {
			return nil, err
		}
		rows = append(rows, duckRows...)
	}
	if strings.TrimSpace(cfg.ClickHouseResultsDir) != "" {
		clickRows, err := collectBaselineRows(cfg.ClickHouseResultsDir, cfg.ClickHouseScales, "ClickHouse", "json-column-sql")
		if err != nil {
			return nil, err
		}
		rows = append(rows, clickRows...)
	}
	return rows, nil
}

func collectTreeDBRows(dir string) ([]reportRow, error) {
	var rows []reportRow
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || filepath.Ext(path) != ".json" {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var probe struct {
			SchemaVersion string `json:"schema_version"`
		}
		if err := json.Unmarshal(raw, &probe); err != nil || probe.SchemaVersion != schemaVersion {
			return nil
		}
		var result runResult
		if err := json.Unmarshal(raw, &result); err != nil {
			return fmt.Errorf("parse %s: %w", path, err)
		}
		retainsJSON := result.RetainsJSON
		compactionEnabled := result.Compaction != nil && result.Compaction.Enabled
		compactionSec := 0.0
		if result.Compaction != nil {
			compactionSec = result.Compaction.WallSec
		}
		var reconstructionValid *bool
		if result.Reconstruction != nil {
			valid := result.Reconstruction.Valid
			reconstructionValid = &valid
		}
		columnAssetBytes := storageCategoryBytes(result.Storage, "column_asset_segments", "column_asset_indexes", "column_asset_metadata", "column_asset_quarantine")
		typedColumnPartBytes := int64(0)
		typedColumnSectionBytes := int64(0)
		aggregateMetadataSidecarBytes := int64(0)
		aggregateMetadataEmbeddedBytes := int64(0)
		aggregateMetadataRefs := 0
		if result.Storage.ColumnStorePhysical != nil {
			typedColumnPartBytes = result.Storage.ColumnStorePhysical.Totals.TypedColumnPartBytes
			typedColumnSectionBytes = result.Storage.ColumnStorePhysical.Totals.TypedColumnSections.TotalStoredBytes
			aggregateMetadataSidecarBytes = result.Storage.ColumnStorePhysical.Totals.AggregateMetadataBytes
			aggregateMetadataEmbeddedBytes = result.Storage.ColumnStorePhysical.Totals.TypedColumnSections.AggregateMetadataBytes
			aggregateMetadataRefs = result.Storage.ColumnStorePhysical.AggregateMetadataRefs
		}
		aggregateMetadataStorageBytes := aggregateMetadataSidecarBytes + aggregateMetadataEmbeddedBytes
		insertStats := result.Load.InsertStats
		for _, q := range result.Queries {
			queryMode := nonEmpty(q.QueryMode, result.QueryMode, inferQueryMode(result.StorageLayout))
			metadataMode := nonEmpty(q.MetadataMode, result.MetadataMode, inferMetadataMode(result.StorageLayout, q.Name))
			scanPath := reportRowMetadataDataScanPath(result.StorageLayout, q.Name, metadataMode)
			diagnostics := q.Diagnostics
			queryPath := diagnostics.QueryPath
			storageSource := reportRowStorageSource(result.StorageLayout)
			if strings.TrimSpace(diagnostics.StorageSource) != "" {
				storageSource = diagnostics.StorageSource
			}
			fallbackReason := reportRowFallbackReason(result.StorageLayout)
			if strings.TrimSpace(diagnostics.FallbackReason) != "" {
				fallbackReason = diagnostics.FallbackReason
			}
			if strings.TrimSpace(queryPath) != "" {
				scanPath = queryPath
			}
			rowsScanned := q.RowsScanned
			if strings.TrimSpace(queryPath) != "" || diagnostics.RowsScanned > 0 {
				rowsScanned = diagnostics.RowsScanned
			}
			metadataCostStorageBytes := int64(0)
			metadataCostStorageBasis := ""
			metadataCostInsertSec := 0.0
			metadataCostInsertBasis := ""
			if diagnostics.AggregateMetadataUsed {
				metadataCostStorageBytes = aggregateMetadataStorageBytes
				metadataCostStorageBasis = "active_manifest_aggregate_metadata_sidecars_plus_typed_column_embedded_sections"
				if aggregateMetadataStorageBytes == 0 {
					metadataCostStorageBasis = "aggregate_metadata_used_but_storage_bytes_not_reported"
				}
				if splitSec, splitBasis, ok := aggregateMetadataSplitInsertCost(result.Load); ok {
					metadataCostInsertSec = splitSec
					metadataCostInsertBasis = splitBasis
				} else {
					metadataCostInsertSec = result.Load.InsertSec
					metadataCostInsertBasis = metadataCostInsertBasisFullLoadUpperBound
				}
			}
			row := reportRow{
				System:                                result.System,
				Engine:                                result.Engine,
				Scale:                                 reportScaleLabel(result),
				RequestedRows:                         result.RequestedRows,
				DatasetSize:                           result.DatasetSize,
				RowCount:                              result.DatasetSize,
				InputRows:                             result.Load.InputRows,
				SkippedInvalidJSONRows:                result.Load.SkippedInvalidJSONRows,
				Format:                                result.Format,
				StorageLayout:                         result.StorageLayout,
				Projection:                            result.Projection,
				QueryMode:                             queryMode,
				MetadataMode:                          metadataMode,
				Profile:                               result.Profile,
				DataRoot:                              result.DataRoot,
				DataShape:                             result.DataShape,
				ExecutionMode:                         queryMode,
				QueryPath:                             queryPath,
				StorageSource:                         storageSource,
				FallbackReason:                        fallbackReason,
				MetadataDataScanPath:                  scanPath,
				SortLayout:                            reportRowSortLayout(result.StorageLayout, result.Projection),
				CompressionMode:                       reportRowCompressionMode(result),
				MutationMode:                          reportRowMutationMode(result),
				DocumentScanFallback:                  scanPath == "document_row_scan",
				RetainedPayloadPolicy:                 result.RetainedPayloadPolicy,
				RetainedPayloadEncoding:               result.RetainedPayloadEncoding,
				RetainedPayloadEncodingStatus:         result.RetainedPayloadEncodingStatus,
				ColumnReconstructionPolicy:            result.ColumnReconstructionPolicy,
				TypedColumnOwner:                      result.TypedColumnOwner,
				ReconstructionStatus:                  reportRowReconstructionStatus(result),
				Query:                                 q.Name,
				BestSec:                               q.BestSec,
				MedianSec:                             q.MedianSec,
				AttemptsSec:                           q.AttemptsSec,
				RowsScanned:                           rowsScanned,
				RowsMatched:                           diagnostics.RowsMatched,
				ReduceRows:                            diagnostics.ReduceRows,
				ResultGroups:                          diagnostics.ResultGroups,
				PredicateCount:                        diagnostics.PredicateCount,
				TopKLimit:                             diagnostics.TopKLimit,
				TopKCandidates:                        diagnostics.TopKCandidates,
				BoundedTopKUsed:                       diagnostics.BoundedTopKUsed,
				TimeOrderTopKUsed:                     diagnostics.TimeOrderTopKUsed,
				SortKeyMarkChecks:                     diagnostics.SortKeyMarkChecks,
				SortKeyMarkMatches:                    diagnostics.SortKeyMarkMatches,
				SortKeyMarkSkips:                      diagnostics.SortKeyMarkSkips,
				SortKeyMarkFallbackReason:             diagnostics.SortKeyMarkFallbackReason,
				SortedGroupedDistinctReady:            diagnostics.SortedGroupedDistinctReady,
				SortedGroupedDistinctUsed:             diagnostics.SortedGroupedDistinctUsed,
				SortedGroupedDistinctFallback:         diagnostics.SortedGroupedDistinctFallback,
				DenseGroupCountUsed:                   diagnostics.DenseGroupCountUsed,
				DenseGroupCountDistinctUsed:           diagnostics.DenseGroupCountDistinctUsed,
				DenseGroupHourCountUsed:               diagnostics.DenseGroupHourCountUsed,
				DenseInt64SpanUsed:                    diagnostics.DenseInt64SpanUsed,
				DictionaryCodeHits:                    diagnostics.DictionaryCodeHits,
				PredicateDictionaryCodeHits:           diagnostics.PredicateDictionaryCodeHits,
				Int64ValueHits:                        diagnostics.Int64ValueHits,
				DecodedPayloadBytes:                   diagnostics.DecodedPayloadBytes,
				DecodedMetadataBytes:                  diagnostics.DecodedMetadataBytes,
				PhysicalBytesScanned:                  diagnostics.PhysicalBytesScanned,
				MappedBytes:                           diagnostics.MappedBytes,
				HeapCopyBytes:                         diagnostics.HeapCopyBytes,
				RowMaterializations:                   diagnostics.RowMaterializations,
				DocumentMaterializations:              diagnostics.DocumentMaterializations,
				FallbackReads:                         diagnostics.FallbackReads,
				AggregateMetadataUsed:                 diagnostics.AggregateMetadataUsed,
				AggregateMetadataRefs:                 aggregateMetadataRefs,
				AggregateMetadataStorageBytes:         aggregateMetadataStorageBytes,
				AggregateMetadataSidecarBytes:         aggregateMetadataSidecarBytes,
				AggregateMetadataEmbeddedBytes:        aggregateMetadataEmbeddedBytes,
				MetadataCostStorageBytes:              metadataCostStorageBytes,
				MetadataCostStorageBasis:              metadataCostStorageBasis,
				MetadataCostInsertSec:                 metadataCostInsertSec,
				MetadataCostInsertBasis:               metadataCostInsertBasis,
				JSONReconstructionUsed:                diagnostics.JSONReconstructionUsed,
				TypedColumnOneShotCacheHit:            diagnostics.TypedColumnOneShotCacheHit,
				TypedColumnOneShotCacheMiss:           diagnostics.TypedColumnOneShotCacheMiss,
				TypedColumnOneShotCacheBuild:          diagnostics.TypedColumnOneShotCacheBuild,
				TypedColumnOneShotBuildNanos:          diagnostics.TypedColumnOneShotBuildNanos,
				TypedColumnPrepareWorkerCount:         diagnostics.TypedColumnPrepareWorkerCount,
				TypedColumnPreparePlanNanos:           diagnostics.TypedColumnPreparePlanNanos,
				TypedColumnPrepareRefsNanos:           diagnostics.TypedColumnPrepareRefsNanos,
				TypedColumnPreparePairingNanos:        diagnostics.TypedColumnPreparePairingNanos,
				TypedColumnPreparePartDecodeNanos:     diagnostics.TypedColumnPreparePartDecodeNanos,
				TypedColumnPreparePostPrepareNanos:    diagnostics.TypedColumnPreparePostPrepareNanos,
				TypedColumnPrepareSummaryNanos:        diagnostics.TypedColumnPrepareSummaryNanos,
				TypedColumnOneShotCacheStoreNanos:     diagnostics.TypedColumnOneShotCacheStoreNanos,
				TypedColumnPrepareReadImageNanos:      diagnostics.TypedColumnPrepareReadImageNanos,
				TypedColumnPrepareStateBuildNanos:     diagnostics.TypedColumnPrepareStateBuildNanos,
				TypedColumnPrepareDictionaryNanos:     diagnostics.TypedColumnPrepareDictionaryNanos,
				TypedColumnPreparePruningNanos:        diagnostics.TypedColumnPreparePruningNanos,
				TypedColumnPrepareSortKeyNanos:        diagnostics.TypedColumnPrepareSortKeyNanos,
				TypedColumnPrepareStatsNanos:          diagnostics.TypedColumnPrepareStatsNanos,
				TypedColumnPrepareRangeReadNanos:      diagnostics.TypedColumnPrepareRangeReadNanos,
				TypedColumnPrepareRangeReadBytes:      diagnostics.TypedColumnPrepareRangeReadBytes,
				TypedColumnPrepareAdapterNanos:        diagnostics.TypedColumnPrepareAdapterNanos,
				TypedColumnPrepareDenseGroupNanos:     diagnostics.TypedColumnPrepareDenseGroupNanos,
				TypedColumnPrepareDenseValueNanos:     diagnostics.TypedColumnPrepareDenseValueNanos,
				TypedColumnPrepareDensePredicateNanos: diagnostics.TypedColumnPrepareDensePredicateNanos,
				TypedColumnPrepareDensePreapplyNanos:  diagnostics.TypedColumnPrepareDensePreapplyNanos,
				TypedColumnPrepareQ2GroupRankNanos:    diagnostics.TypedColumnPrepareQ2GroupRankNanos,
				TypedColumnPrepareQ2DistinctRankNanos: diagnostics.TypedColumnPrepareQ2DistinctRankNanos,
				TypedColumnPrepareQ2LocalRankNanos:    diagnostics.TypedColumnPrepareQ2LocalRankNanos,
				PrepareSetupNanos:                     diagnostics.PrepareSetupNanos,
				RunNanos:                              diagnostics.RunNanos,
				ResultRenderNanos:                     diagnostics.ResultRenderNanos,
				HashNanos:                             diagnostics.HashNanos,
				RenderHashNanos:                       diagnostics.RenderHashNanos,
				AttemptWallNanos:                      diagnostics.AttemptWallNanos,
				TotalQueryNanos:                       diagnostics.TotalQueryNanos,
				PhysicalQueryCount:                    len(diagnostics.PhysicalQueries),
				StorageBytes:                          result.Storage.TotalBytes,
				StorageGrossBytes:                     result.Storage.GrossBytes,
				StorageExcludedBytes:                  result.Storage.ExcludedBytes,
				StorageDurableBytesWALExcluded:        reportStorageDurableBytesWALExcluded(result.Storage),
				StorageWALBytesExcludedFromDurable:    result.Storage.WALBytesExcludedFromDurable,
				StorageWALExcludedNote:                result.Storage.DurableStorageBytesWALExcludedNote,
				StorageColumnAssetBytes:               columnAssetBytes,
				StorageTypedColumnPartBytes:           typedColumnPartBytes,
				StorageTypedColumnSectionBytes:        typedColumnSectionBytes,
				StoragePrimaryIndexBytes:              storageCategoryBytes(result.Storage, "primary_index"),
				StorageLeafVLogBytes:                  storageCategoryBytes(result.Storage, "leaf_vlog"),
				StorageWALBytes:                       storageCategoryBytes(result.Storage, "wal"),
				StorageAccountingScope:                result.Storage.AccountingScope,
				StorageMeasurementPhase:               result.Storage.MeasurementPhase,
				LoadSec:                               result.Load.WallSec,
				InsertSec:                             result.Load.InsertSec,
				CompactionSec:                         compactionSec,
				Compacted:                             compactionEnabled,
				RetainsJSON:                           &retainsJSON,
				ReconstructionValid:                   reconstructionValid,
				Source:                                path,
			}
			applyQExprTypedScanEvidence(&row, q, diagnostics)
			applyReportRowInsertStats(&row, insertStats)
			rows = append(rows, row)
		}
		return nil
	})
	return rows, err
}

const (
	qexprExpressionKind = "sum(second_of_day_square)"
	qexprTypedCellBasis = "rows_scanned"
)

func applyQExprTypedScanEvidence(row *reportRow, query queryRun, diagnostics queryDiagnostics) {
	if row == nil || query.Name != "qexpr" {
		return
	}
	if diagnostics.QueryPath != "typed_column_int64_aggregate" || diagnostics.AggregateMetadataUsed {
		return
	}
	if !qexprUsesTypedColumnPath(diagnostics) {
		return
	}
	cells := qexprTypedCellsVisited(query, diagnostics)
	precomputed := false
	row.TypedCellsVisited = &cells
	row.TypedCellsBasis = qexprTypedCellBasis
	row.ExpressionKind = qexprExpressionKind
	row.PrecomputedExpressionUsed = &precomputed
}

func qexprUsesTypedColumnPath(diagnostics queryDiagnostics) bool {
	if diagnostics.StorageSource == "typed_column_part" {
		return true
	}
	for _, physical := range diagnostics.PhysicalQueries {
		if physical.StorageSource == "typed_column_part" {
			return true
		}
	}
	return false
}

func qexprTypedCellsVisited(query queryRun, diagnostics queryDiagnostics) int {
	for _, physical := range diagnostics.PhysicalQueries {
		if physical.Name == "second_of_day_square_sum" && physical.RowsScanned > 0 {
			return physical.RowsScanned
		}
	}
	if diagnostics.RowsScanned > 0 {
		return diagnostics.RowsScanned
	}
	return query.RowsScanned
}

func reportScaleLabel(result runResult) string {
	if result.RequestedRows > 0 && result.DatasetSize > 0 && result.RequestedRows != result.DatasetSize {
		if result.Load.InputRows == result.RequestedRows && result.Load.SkippedInvalidJSONRows > 0 {
			return scaleFromDatasetSize(result.RequestedRows)
		}
		return fmt.Sprintf("%d of %d requested rows", result.DatasetSize, result.RequestedRows)
	}
	if result.Scale == "subset" || result.Scale == "smoke" {
		return scaleFromDatasetSize(result.DatasetSize)
	}
	if strings.TrimSpace(result.ScaleLabel) != "" {
		return result.ScaleLabel
	}
	return result.Scale
}

func storageCategoryBytes(storage storageResult, categories ...string) int64 {
	if len(categories) == 0 || len(storage.Categories) == 0 {
		return 0
	}
	wanted := make(map[string]struct{}, len(categories))
	for _, category := range categories {
		wanted[category] = struct{}{}
	}
	var total int64
	for _, category := range storage.Categories {
		if _, ok := wanted[category.Category]; ok && category.Included {
			total += category.Bytes
		}
	}
	return total
}

func reportStorageDurableBytesWALExcluded(storage storageResult) int64 {
	return durableStorageBytesWALExcluded(storage.TotalBytes, storage.WALBytesExcludedFromDurable)
}

func reportRowExecutionMode(layout string) string {
	switch {
	case isPreparedColumnStoreLayout(layout):
		return queryModeHotPreparedRun
	case isColumnStoreLayout(layout):
		return queryModeOneShotEndToEnd
	case layout == storageLayoutRow:
		return queryModeOneShotEndToEnd
	default:
		return "unknown"
	}
}

func inferQueryMode(layout string) string {
	return reportRowExecutionMode(layout)
}

func inferMetadataMode(layout, query string) string {
	if columnStoreUsesAggregateMetadata(layout, query) {
		return metadataModeAutoAggregateMetadata
	}
	if isColumnStoreLayout(layout) {
		return metadataModeNoAggregateMetadata
	}
	return "not_applicable"
}

func reportRowStorageSource(layout string) string {
	switch {
	case isFullDataColumnStoreLayout(layout):
		return "typed_column_part"
	case isColumnStoreLayout(layout):
		return "typed_row_asset"
	case layout == storageLayoutRow:
		return "primary_document_btree"
	default:
		return "unknown"
	}
}

func reportRowFallbackReason(layout string) string {
	if isColumnStoreLayout(layout) {
		return "none"
	}
	if layout == storageLayoutRow {
		return "row_scan_baseline"
	}
	return "unknown"
}

func reportRowMetadataDataScanPath(layout, query, metadataMode string) string {
	switch {
	case metadataMode == metadataModeAutoAggregateMetadata && columnStoreUsesAggregateMetadata(layout, query):
		return "aggregate_metadata"
	case isColumnStoreLayout(layout):
		return "typed_column_data_scan"
	case layout == storageLayoutRow:
		return "document_row_scan"
	default:
		return "unknown"
	}
}

func reportRowSortLayout(layout, projection string) string {
	if !isColumnStoreLayout(layout) {
		return "not_applicable"
	}
	if isFullDataColumnStoreLayout(layout) {
		return "time_us"
	}
	switch projection {
	case "q3", "q4", "q4a", "q4b", "q5", "qexpr", "minimal":
		return "time_us"
	default:
		return "ingest_order_unsorted"
	}
}

func reportRowCompressionMode(result runResult) string {
	if !isColumnStoreLayout(result.StorageLayout) {
		return "not_applicable"
	}
	if result.Storage.ColumnStorePhysical == nil {
		return "not_reported"
	}
	details := result.Storage.ColumnStorePhysical.Totals.TypedColumnSections.CompressionDetail
	if len(details) == 0 {
		for _, part := range result.Storage.ColumnStorePhysical.TypedColumnParts {
			details = append(details, part.Image.CompressionDetail...)
		}
	}
	if len(details) == 0 {
		return "not_reported"
	}
	requested := make(map[string]int)
	actual := make(map[string]int)
	fallback := make(map[string]int)
	for _, detail := range details {
		requested[strings.TrimSpace(detail.RequestedCompression)] += detail.Blocks
		actual[strings.TrimSpace(detail.ActualCompression)] += detail.Blocks
		if reason := strings.TrimSpace(detail.FallbackReason); reason != "" {
			fallback[reason] += detail.Blocks
		}
	}
	mode := "requested=" + formatStringCountMap(requested) + "; actual=" + formatStringCountMap(actual)
	if len(fallback) > 0 {
		mode += "; fallback=" + formatStringCountMap(fallback)
	}
	return mode
}

func formatStringCountMap(counts map[string]int) string {
	if len(counts) == 0 {
		return "none"
	}
	keys := make([]string, 0, len(counts))
	for key := range counts {
		if key == "" {
			key = "unspecified"
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		lookup := key
		if key == "unspecified" {
			lookup = ""
		}
		parts = append(parts, fmt.Sprintf("%s:%d", key, counts[lookup]))
	}
	return strings.Join(parts, ",")
}

func reportRowMutationMode(result runResult) string {
	if result.Compaction != nil && result.Compaction.Enabled {
		return "insert_only_static_snapshot_compacted"
	}
	return "insert_only_static_snapshot"
}

func reportRowReconstructionStatus(result runResult) string {
	if result.Reconstruction != nil {
		if result.Reconstruction.Valid {
			return "valid"
		}
		return "invalid"
	}
	if isFullDataColumnStoreLayout(result.StorageLayout) {
		return "not_validated"
	}
	return "not_applicable"
}

func reportHasTreeDBInsertStats(rows []reportRow) bool {
	for _, row := range rows {
		if row.System == "TreeDB" && reportRowHasInsertStats(row) {
			return true
		}
	}
	return false
}

func reportHasTreeDBRetainedInsertStats(rows []reportRow) bool {
	for _, row := range rows {
		if row.System == "TreeDB" && reportRowHasRetainedInsertStats(row) {
			return true
		}
	}
	return false
}

func reportHasTreeDBColumnPublishInsertStats(rows []reportRow) bool {
	for _, row := range rows {
		if row.System == "TreeDB" && reportRowHasColumnPublishInsertStats(row) {
			return true
		}
	}
	return false
}

func reportRowHasInsertStats(row reportRow) bool {
	return reportRowHasRetainedInsertStats(row) || reportRowHasColumnPublishInsertStats(row)
}

func reportRowHasRetainedInsertStats(row reportRow) bool {
	return row.InsertStatsRetainedPayloadPrepareSec > 0 ||
		row.InsertStatsRetainedPayloadRows > 0 ||
		row.InsertStatsRetainedPayloadDeclaredRows > 0 ||
		row.InsertStatsRetainedPayloadSemanticStreamBlocks > 0 ||
		row.InsertStatsRetainedPayloadValueLogPointerizeSec > 0 ||
		row.InsertStatsRetainedPayloadValueLogValues > 0 ||
		row.InsertStatsRetainedPayloadValueLogBytes > 0 ||
		row.InsertStatsRetainedStreamValueLogPointerizeSec > 0 ||
		row.InsertStatsRetainedStreamValueLogValues > 0 ||
		row.InsertStatsRetainedStreamValueLogBytes > 0
}

func reportRowHasColumnPublishInsertStats(row reportRow) bool {
	return row.InsertStatsPublishSec > 0 ||
		row.InsertStatsColumnPublishBuildColumnDeltaSec > 0 ||
		row.InsertStatsColumnPublishBuildSystemDeltaSec > 0 ||
		row.InsertStatsColumnPublishCommitSec > 0 ||
		row.InsertStatsColumnPublishDocumentExtractionSec > 0 ||
		row.InsertStatsColumnPublishDeclaredColumnSec > 0 ||
		row.InsertStatsColumnPublishAssetPreparationSec > 0 ||
		row.InsertStatsColumnPublishRowAssetPrepareSec > 0 ||
		row.InsertStatsColumnPublishTypedColumnPrepareSec > 0 ||
		row.InsertStatsColumnPublishTypedDictionarySec > 0 ||
		row.InsertStatsColumnPublishTypedRowsSec > 0 ||
		row.InsertStatsColumnPublishTypedPartSec > 0 ||
		row.InsertStatsColumnPublishTypedImageSec > 0 ||
		row.InsertStatsColumnPublishDictionaryPrepareSec > 0 ||
		row.InsertStatsColumnPublishInt64PrepareSec > 0 ||
		row.InsertStatsColumnPublishAggregateMetadataSec > 0 ||
		row.InsertStatsColumnPublishRowSidecarSharedBuildSec > 0 ||
		row.InsertStatsColumnPublishAssetAppendSec > 0 ||
		row.InsertStatsColumnPublishAssetAppendOpenSec > 0 ||
		row.InsertStatsColumnPublishAssetAppendWriteSec > 0 ||
		row.InsertStatsColumnPublishAssetAppendCloseSec > 0 ||
		row.InsertStatsColumnPublishAssetAppendFileSyncSec > 0 ||
		row.InsertStatsColumnPublishAssetAppendFileCloseSec > 0 ||
		row.InsertStatsColumnPublishAssetAppendDirSyncSec > 0 ||
		row.InsertStatsColumnPublishAssetAppendCleanupSec > 0 ||
		row.InsertStatsColumnPublishManifestEncodeSec > 0 ||
		row.InsertStatsColumnPublishAssetClosureSec > 0 ||
		row.InsertStatsColumnPublishRootDeltaSec > 0 ||
		row.InsertStatsColumnPublishSystemDeltaSec > 0 ||
		row.InsertStatsColumnPublishRootDeltaMaterializeSec > 0 ||
		row.InsertStatsColumnPublishRows > 0 ||
		row.InsertStatsColumnPublishPreparedAssets > 0 ||
		row.InsertStatsColumnPublishRowAssetBytes > 0 ||
		row.InsertStatsColumnPublishRowAssetCount > 0 ||
		row.InsertStatsColumnPublishTypedColumnBytes > 0 ||
		row.InsertStatsColumnPublishTypedColumnCount > 0 ||
		row.InsertStatsColumnPublishDictionaryBytes > 0 ||
		row.InsertStatsColumnPublishDictionaryCount > 0 ||
		row.InsertStatsColumnPublishInt64Bytes > 0 ||
		row.InsertStatsColumnPublishInt64Count > 0 ||
		row.InsertStatsColumnPublishAggregateMetadataBytes > 0 ||
		row.InsertStatsColumnPublishAggregateMetadataCount > 0 ||
		row.InsertStatsColumnPublishSharedAppendBytes > 0 ||
		row.InsertStatsColumnPublishSharedAppendCount > 0 ||
		row.InsertStatsColumnPublishRequiredAssetBytes > 0 ||
		row.InsertStatsColumnPublishManifestBytes > 0
}

func applyReportRowInsertStats(row *reportRow, stats *insertStatsResult) {
	if row == nil || stats == nil {
		return
	}
	row.InsertStatsRetainedPayloadPrepareSec = stats.RetainedPayloadPrepareSec
	row.InsertStatsRetainedPayloadRows = stats.RetainedPayloadRows
	row.InsertStatsRetainedPayloadDeclaredRows = stats.RetainedPayloadDeclaredRows
	row.InsertStatsRetainedPayloadSemanticStreamBlocks = stats.RetainedPayloadSemanticStreamBlocks
	row.InsertStatsRetainedPayloadValueLogPointerizeSec = stats.RetainedPayloadValueLogPointerizeSec
	row.InsertStatsRetainedPayloadValueLogValues = stats.RetainedPayloadValueLogValues
	row.InsertStatsRetainedPayloadValueLogBytes = stats.RetainedPayloadValueLogBytes
	row.InsertStatsRetainedStreamValueLogPointerizeSec = stats.RetainedStreamValueLogPointerizeSec
	row.InsertStatsRetainedStreamValueLogValues = stats.RetainedStreamValueLogValues
	row.InsertStatsRetainedStreamValueLogBytes = stats.RetainedStreamValueLogBytes
	row.InsertStatsPublishSec = stats.PublishSec
	row.InsertStatsColumnPublishBuildColumnDeltaSec = stats.ColumnPublishBuildColumnDeltaSec
	row.InsertStatsColumnPublishBuildSystemDeltaSec = stats.ColumnPublishBuildSystemDeltaSec
	row.InsertStatsColumnPublishCommitSec = stats.ColumnPublishCommitSec
	row.InsertStatsColumnPublishDocumentExtractionSec = stats.ColumnPublishDocumentExtractionSec
	row.InsertStatsColumnPublishDeclaredColumnSec = stats.ColumnPublishDeclaredColumnSec
	row.InsertStatsColumnPublishAssetPreparationSec = stats.ColumnPublishAssetPreparationSec
	row.InsertStatsColumnPublishRowAssetPrepareSec = stats.ColumnPublishRowAssetPrepareSec
	row.InsertStatsColumnPublishTypedColumnPrepareSec = stats.ColumnPublishTypedColumnPrepareSec
	row.InsertStatsColumnPublishTypedDictionarySec = stats.ColumnPublishTypedDictionarySec
	row.InsertStatsColumnPublishTypedRowsSec = stats.ColumnPublishTypedRowsSec
	row.InsertStatsColumnPublishTypedPartSec = stats.ColumnPublishTypedPartSec
	row.InsertStatsColumnPublishTypedImageSec = stats.ColumnPublishTypedImageSec
	row.InsertStatsColumnPublishDictionaryPrepareSec = stats.ColumnPublishDictionaryPrepareSec
	row.InsertStatsColumnPublishInt64PrepareSec = stats.ColumnPublishInt64PrepareSec
	row.InsertStatsColumnPublishAggregateMetadataSec = stats.ColumnPublishAggregateMetadataSec
	row.InsertStatsColumnPublishRowSidecarSharedBuildSec = stats.ColumnPublishRowSidecarSharedBuildSec
	row.InsertStatsColumnPublishAssetAppendSec = stats.ColumnPublishAssetAppendSec
	row.InsertStatsColumnPublishAssetAppendOpenSec = stats.ColumnPublishAssetAppendOpenSec
	row.InsertStatsColumnPublishAssetAppendWriteSec = stats.ColumnPublishAssetAppendWriteSec
	row.InsertStatsColumnPublishAssetAppendCloseSec = stats.ColumnPublishAssetAppendCloseSec
	row.InsertStatsColumnPublishAssetAppendFileSyncSec = stats.ColumnPublishAssetAppendFileSyncSec
	row.InsertStatsColumnPublishAssetAppendFileCloseSec = stats.ColumnPublishAssetAppendFileCloseSec
	row.InsertStatsColumnPublishAssetAppendDirSyncSec = stats.ColumnPublishAssetAppendDirSyncSec
	row.InsertStatsColumnPublishAssetAppendCleanupSec = stats.ColumnPublishAssetAppendCleanupSec
	row.InsertStatsColumnPublishManifestEncodeSec = stats.ColumnPublishManifestEncodeSec
	row.InsertStatsColumnPublishAssetClosureSec = stats.ColumnPublishAssetClosureSec
	row.InsertStatsColumnPublishRootDeltaSec = stats.ColumnPublishRootDeltaSec
	row.InsertStatsColumnPublishSystemDeltaSec = stats.ColumnPublishSystemDeltaSec
	row.InsertStatsColumnPublishRootDeltaMaterializeSec = stats.ColumnPublishRootDeltaMaterializeSec
	row.InsertStatsColumnPublishRows = stats.ColumnPublishRows
	row.InsertStatsColumnPublishPreparedAssets = stats.ColumnPublishPreparedAssets
	row.InsertStatsColumnPublishRowAssetBytes = stats.ColumnPublishRowAssetBytes
	row.InsertStatsColumnPublishRowAssetCount = stats.ColumnPublishRowAssetCount
	row.InsertStatsColumnPublishTypedColumnBytes = stats.ColumnPublishTypedColumnBytes
	row.InsertStatsColumnPublishTypedColumnCount = stats.ColumnPublishTypedColumnCount
	row.InsertStatsColumnPublishDictionaryBytes = stats.ColumnPublishDictionaryBytes
	row.InsertStatsColumnPublishDictionaryCount = stats.ColumnPublishDictionaryCount
	row.InsertStatsColumnPublishInt64Bytes = stats.ColumnPublishInt64Bytes
	row.InsertStatsColumnPublishInt64Count = stats.ColumnPublishInt64Count
	row.InsertStatsColumnPublishAggregateMetadataBytes = stats.ColumnPublishAggregateMetadataBytes
	row.InsertStatsColumnPublishAggregateMetadataCount = stats.ColumnPublishAggregateMetadataCount
	row.InsertStatsColumnPublishSharedAppendBytes = stats.ColumnPublishSharedAppendBytes
	row.InsertStatsColumnPublishSharedAppendCount = stats.ColumnPublishSharedAppendCount
	row.InsertStatsColumnPublishRequiredAssetBytes = stats.ColumnPublishRequiredAssetBytes
	row.InsertStatsColumnPublishManifestBytes = stats.ColumnPublishManifestBytes
}

func reportRowInsertStatsKey(row reportRow) string {
	return strings.Join([]string{
		row.Source,
		row.Scale,
		row.StorageLayout,
		row.Projection,
		row.QueryMode,
		row.MetadataMode,
	}, "\x00")
}

func collectBaselineRows(dir string, scales map[string]struct{}, systemName, engine string) ([]reportRow, error) {
	if _, err := os.Stat(dir); err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var matches []string
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || filepath.Ext(path) != ".json" {
			return nil
		}
		matches = append(matches, path)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(matches)
	var rows []reportRow
	for _, path := range matches {
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var probe struct {
			System string `json:"system"`
		}
		if err := json.Unmarshal(raw, &probe); err != nil || probe.System == "" {
			continue
		}
		var result jsonBenchBaselineResult
		if err := json.Unmarshal(raw, &result); err != nil {
			return nil, fmt.Errorf("parse %s: %w", path, err)
		}
		if result.System == "" || len(result.Result) == 0 || result.System != systemName {
			continue
		}
		requestedRows := result.NumLoadedDocuments
		if result.RequestedRows > 0 {
			requestedRows = result.RequestedRows
		}
		scaleRows := result.DatasetSize
		if result.RequestedRows > 0 {
			scaleRows = result.RequestedRows
		}
		scale := scaleFromDatasetSize(scaleRows)
		if !scaleAllowed(scales, scale) {
			continue
		}
		for i, attempts := range result.Result {
			name := baselineQueryName(i, len(result.Result))
			best, median := bestMedian(attempts)
			retainsJSON := true
			rows = append(rows, reportRow{
				System:                  result.System,
				Engine:                  engine,
				Scale:                   scale,
				RequestedRows:           requestedRows,
				DatasetSize:             result.DatasetSize,
				RowCount:                result.DatasetSize,
				Format:                  "json",
				Projection:              "full",
				DataShape:               "full-json",
				ExecutionMode:           "baseline",
				StorageSource:           "external_json_engine",
				FallbackReason:          "not_applicable",
				MetadataDataScanPath:    "json_sql_scan",
				SortLayout:              "external_baseline",
				CompressionMode:         "engine_reported",
				MutationMode:            "external_baseline",
				ReconstructionStatus:    "not_reported",
				Query:                   name,
				BestSec:                 best,
				MedianSec:               median,
				AttemptsSec:             attempts,
				RowsScanned:             result.NumLoadedDocuments,
				StorageBytes:            result.TotalSize,
				StorageGrossBytes:       result.TotalSize,
				BaselineDataBytes:       result.DataSize,
				BaselineIndexBytes:      result.IndexSize,
				StorageAccountingScope:  systemName + "_reported_total_size",
				StorageMeasurementPhase: "baseline_artifact",
				RetainsJSON:             &retainsJSON,
				Source:                  path,
			})
		}
	}
	return rows, nil
}

func baselineQueryName(index, total int) string {
	if total == 6 && index == 5 {
		return "qexpr"
	}
	return "q" + strconv.Itoa(index+1)
}

func parseScaleFilter(raw string) (map[string]struct{}, error) {
	raw = strings.TrimSpace(strings.ToLower(raw))
	if raw == "" || raw == "all" {
		return nil, nil
	}
	out := make(map[string]struct{})
	for _, part := range strings.Split(raw, ",") {
		scale := strings.TrimSpace(part)
		if scale == "" {
			continue
		}
		out[scale] = struct{}{}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("empty scale filter %q", raw)
	}
	return out, nil
}

func scaleAllowed(filter map[string]struct{}, scale string) bool {
	if len(filter) == 0 {
		return true
	}
	_, ok := filter[scale]
	return ok
}

func renderMarkdownReport(doc reportDocument) []byte {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "# TreeDB JSONBench Collection Report\n\n")
	fmt.Fprintf(&buf, "Generated: `%s`\n\n", doc.GeneratedAt)
	if len(doc.Rows) == 0 {
		fmt.Fprintf(&buf, "No result rows found.\n")
		return buf.Bytes()
	}
	fmt.Fprintf(&buf, "## Query Runtime Matrix\n\n")
	fmt.Fprintf(&buf, "| rows/scale | system | shape | layout | query | best | loaded rows/s | scanned rows/s | median | attempts | requested | loaded | scanned | storage | load |\n")
	fmt.Fprintf(&buf, "|---|---|---|---|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|\n")
	for _, row := range doc.Rows {
		fmt.Fprintf(
			&buf,
			"| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %d | %s | %s |\n",
			row.Scale,
			row.System,
			reportRowDataShape(row),
			reportRowLayout(row),
			row.Query,
			formatSeconds(row.BestSec),
			formatRowsPerSecond(row.DatasetSize, row.BestSec),
			formatRowsPerSecond(row.RowsScanned, row.BestSec),
			formatSeconds(row.MedianSec),
			formatAttempts(row.AttemptsSec),
			formatCount(row.RequestedRows),
			formatCount(row.DatasetSize),
			row.RowsScanned,
			formatBytes(row.StorageBytes),
			formatSeconds(row.LoadSec),
		)
	}
	fmt.Fprintf(&buf, "\n## TreeDB Row Attribution Labels\n\n")
	fmt.Fprintf(&buf, "| rows/scale | layout | query | profile | mode | source | fallback | path | doc-scan fallback | sort | compression | mutation | retained payload | typed owner | rows | reconstruction | WAL-excluded durable | WAL excluded |\n")
	fmt.Fprintf(&buf, "|---|---|---:|---|---|---|---|---|---|---|---|---|---|---|---:|---|---:|---:|\n")
	for _, row := range doc.Rows {
		if row.System != "TreeDB" {
			continue
		}
		fmt.Fprintf(
			&buf,
			"| %s | %s | %s | %s | %s | %s | %s | %s | %t | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n",
			row.Scale,
			reportRowLayout(row),
			row.Query,
			row.Profile,
			row.ExecutionMode,
			row.StorageSource,
			row.FallbackReason,
			row.MetadataDataScanPath,
			row.DocumentScanFallback,
			row.SortLayout,
			row.CompressionMode,
			row.MutationMode,
			row.RetainedPayloadPolicy,
			row.TypedColumnOwner,
			formatCount(row.RowCount),
			row.ReconstructionStatus,
			formatBytes(row.StorageDurableBytesWALExcluded),
			formatBytes(row.StorageWALBytesExcludedFromDurable),
		)
	}
	if reportHasTreeDBRetainedInsertStats(doc.Rows) {
		fmt.Fprintf(&buf, "\n## TreeDB Insert Stats\n\n")
		fmt.Fprintf(&buf, "| rows/scale | layout | load | insert | retained prepare | retained rows | declared rows | stream blocks | primary vlog pointerize | primary vlog values | primary vlog bytes | stream vlog pointerize | stream vlog values | stream vlog bytes |\n")
		fmt.Fprintf(&buf, "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
		seen := make(map[string]struct{})
		for _, row := range doc.Rows {
			if row.System != "TreeDB" || !reportRowHasRetainedInsertStats(row) {
				continue
			}
			key := reportRowInsertStatsKey(row)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			fmt.Fprintf(
				&buf,
				"| %s | %s | %s | %s | %s | %d | %d | %d | %s | %d | %s | %s | %d | %s |\n",
				row.Scale,
				reportRowLayout(row),
				formatSeconds(row.LoadSec),
				formatSeconds(row.InsertSec),
				formatSeconds(row.InsertStatsRetainedPayloadPrepareSec),
				row.InsertStatsRetainedPayloadRows,
				row.InsertStatsRetainedPayloadDeclaredRows,
				row.InsertStatsRetainedPayloadSemanticStreamBlocks,
				formatSeconds(row.InsertStatsRetainedPayloadValueLogPointerizeSec),
				row.InsertStatsRetainedPayloadValueLogValues,
				formatBytes(row.InsertStatsRetainedPayloadValueLogBytes),
				formatSeconds(row.InsertStatsRetainedStreamValueLogPointerizeSec),
				row.InsertStatsRetainedStreamValueLogValues,
				formatBytes(row.InsertStatsRetainedStreamValueLogBytes),
			)
		}
	}
	if reportHasTreeDBColumnPublishInsertStats(doc.Rows) {
		fmt.Fprintf(&buf, "\n## TreeDB Column Publish Insert Stats\n\n")
		fmt.Fprintf(&buf, "| rows/scale | layout | load | insert | publish | build column delta | commit | asset prepare | row asset | typed column | typed dictionary | typed rows | typed part | typed image | dictionary | int64 | aggregate metadata | shared build | asset append | append open | append write | append close | file sync | file close | dir sync | rows | assets | row asset bytes | typed column bytes | dictionary bytes | int64 bytes | aggregate metadata bytes | shared append bytes | required asset bytes | manifest bytes |\n")
		fmt.Fprintf(&buf, "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
		seen := make(map[string]struct{})
		for _, row := range doc.Rows {
			if row.System != "TreeDB" || !reportRowHasColumnPublishInsertStats(row) {
				continue
			}
			key := reportRowInsertStatsKey(row)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			fmt.Fprintf(
				&buf,
				"| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %d | %d | %s | %s | %s | %s | %s | %s | %s | %s |\n",
				row.Scale,
				reportRowLayout(row),
				formatSeconds(row.LoadSec),
				formatSeconds(row.InsertSec),
				formatSeconds(row.InsertStatsPublishSec),
				formatSeconds(row.InsertStatsColumnPublishBuildColumnDeltaSec),
				formatSeconds(row.InsertStatsColumnPublishCommitSec),
				formatSeconds(row.InsertStatsColumnPublishAssetPreparationSec),
				formatSeconds(row.InsertStatsColumnPublishRowAssetPrepareSec),
				formatSeconds(row.InsertStatsColumnPublishTypedColumnPrepareSec),
				formatSeconds(row.InsertStatsColumnPublishTypedDictionarySec),
				formatSeconds(row.InsertStatsColumnPublishTypedRowsSec),
				formatSeconds(row.InsertStatsColumnPublishTypedPartSec),
				formatSeconds(row.InsertStatsColumnPublishTypedImageSec),
				formatSeconds(row.InsertStatsColumnPublishDictionaryPrepareSec),
				formatSeconds(row.InsertStatsColumnPublishInt64PrepareSec),
				formatSeconds(row.InsertStatsColumnPublishAggregateMetadataSec),
				formatSeconds(row.InsertStatsColumnPublishRowSidecarSharedBuildSec),
				formatSeconds(row.InsertStatsColumnPublishAssetAppendSec),
				formatSeconds(row.InsertStatsColumnPublishAssetAppendOpenSec),
				formatSeconds(row.InsertStatsColumnPublishAssetAppendWriteSec),
				formatSeconds(row.InsertStatsColumnPublishAssetAppendCloseSec),
				formatSeconds(row.InsertStatsColumnPublishAssetAppendFileSyncSec),
				formatSeconds(row.InsertStatsColumnPublishAssetAppendFileCloseSec),
				formatSeconds(row.InsertStatsColumnPublishAssetAppendDirSyncSec),
				row.InsertStatsColumnPublishRows,
				row.InsertStatsColumnPublishPreparedAssets,
				formatBytes(row.InsertStatsColumnPublishRowAssetBytes),
				formatBytes(row.InsertStatsColumnPublishTypedColumnBytes),
				formatBytes(row.InsertStatsColumnPublishDictionaryBytes),
				formatBytes(row.InsertStatsColumnPublishInt64Bytes),
				formatBytes(row.InsertStatsColumnPublishAggregateMetadataBytes),
				formatBytes(row.InsertStatsColumnPublishSharedAppendBytes),
				formatBytes(row.InsertStatsColumnPublishRequiredAssetBytes),
				formatBytes(row.InsertStatsColumnPublishManifestBytes),
			)
		}
	}
	fmt.Fprintf(&buf, "\n## TreeDB Query Diagnostics\n\n")
	fmt.Fprintf(&buf, "| rows/scale | layout | query | query mode | metadata mode | path | source | fallback | scanned | matched | reduced | groups | predicates | topK | topK candidates | aggregate metadata | bounded topK | time-order topK | mark checks | mark skips | sorted distinct | dense path | decoded payload | decoded metadata | physical bytes | row mats | doc mats | JSON reconstruction | prepare/setup | run | render/hash | total |\n")
	fmt.Fprintf(&buf, "|---|---|---:|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|---|---|---:|---:|---|---|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|\n")
	for _, row := range doc.Rows {
		if row.System != "TreeDB" {
			continue
		}
		fmt.Fprintf(
			&buf,
			"| %s | %s | %s | %s | %s | %s | %s | %s | %d | %d | %d | %d | %d | %d | %d | %t | %t | %t | %d | %d | %s | %s | %d | %d | %d | %d | %d | %t | %d | %d | %d | %d |\n",
			row.Scale,
			reportRowLayout(row),
			row.Query,
			row.QueryMode,
			row.MetadataMode,
			nonEmpty(row.QueryPath, row.MetadataDataScanPath),
			row.StorageSource,
			row.FallbackReason,
			row.RowsScanned,
			row.RowsMatched,
			row.ReduceRows,
			row.ResultGroups,
			row.PredicateCount,
			row.TopKLimit,
			row.TopKCandidates,
			row.AggregateMetadataUsed,
			row.BoundedTopKUsed,
			row.TimeOrderTopKUsed,
			row.SortKeyMarkChecks,
			row.SortKeyMarkSkips,
			formatDiagnosticBool(row.SortedGroupedDistinctReady, row.SortedGroupedDistinctUsed, row.SortedGroupedDistinctFallback),
			formatDensePath(row),
			row.DecodedPayloadBytes,
			row.DecodedMetadataBytes,
			row.PhysicalBytesScanned,
			row.RowMaterializations,
			row.DocumentMaterializations,
			row.JSONReconstructionUsed,
			row.PrepareSetupNanos,
			row.RunNanos,
			row.RenderHashNanos,
			row.TotalQueryNanos,
		)
	}
	if reportHasExpressionEvidence(doc.Rows) {
		fmt.Fprintf(&buf, "\n## TreeDB Expression Evidence\n\n")
		fmt.Fprintf(&buf, "| rows/scale | layout | query | expression | typed cells visited | basis | precomputed expression | aggregate metadata | path | source |\n")
		fmt.Fprintf(&buf, "|---|---|---:|---|---:|---|---|---|---|---|\n")
		for _, row := range doc.Rows {
			if row.System != "TreeDB" || !reportRowHasExpressionEvidence(row) {
				continue
			}
			fmt.Fprintf(
				&buf,
				"| %s | %s | %s | %s | %s | %s | %s | %t | %s | %s |\n",
				row.Scale,
				reportRowLayout(row),
				row.Query,
				row.ExpressionKind,
				formatOptionalInt(row.TypedCellsVisited),
				row.TypedCellsBasis,
				formatOptionalBool(row.PrecomputedExpressionUsed),
				row.AggregateMetadataUsed,
				nonEmpty(row.QueryPath, row.MetadataDataScanPath),
				row.StorageSource,
			)
		}
	}
	if reportHasTypedColumnSetupDiagnostics(doc.Rows) {
		fmt.Fprintf(&buf, "\n## TreeDB Typed Column Setup Diagnostics\n\n")
		fmt.Fprintf(&buf, "| rows/scale | layout | query | query mode | metadata mode | prepare/setup ns | one-shot build ns | prep workers | prep plan ns | prep refs ns | prep pair ns | prep decode ns | prep post ns | q2 group rank ns | q2 distinct rank ns | q2 local rank ns | prep summary ns | cache store ns | read image ns | state build ns | dictionary ns | pruning ns | sort key ns | stats ns | range read ns | range read B | adapter ns | dense group ns | dense value ns | dense predicate ns | dense preapply ns |\n")
		fmt.Fprintf(&buf, "|---|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
		for _, row := range doc.Rows {
			if row.System != "TreeDB" || !reportRowHasTypedColumnSetupDiagnostics(row) {
				continue
			}
			fmt.Fprintf(
				&buf,
				"| %s | %s | %s | %s | %s | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d |\n",
				row.Scale,
				reportRowLayout(row),
				row.Query,
				row.QueryMode,
				row.MetadataMode,
				row.PrepareSetupNanos,
				row.TypedColumnOneShotBuildNanos,
				row.TypedColumnPrepareWorkerCount,
				row.TypedColumnPreparePlanNanos,
				row.TypedColumnPrepareRefsNanos,
				row.TypedColumnPreparePairingNanos,
				row.TypedColumnPreparePartDecodeNanos,
				row.TypedColumnPreparePostPrepareNanos,
				row.TypedColumnPrepareQ2GroupRankNanos,
				row.TypedColumnPrepareQ2DistinctRankNanos,
				row.TypedColumnPrepareQ2LocalRankNanos,
				row.TypedColumnPrepareSummaryNanos,
				row.TypedColumnOneShotCacheStoreNanos,
				row.TypedColumnPrepareReadImageNanos,
				row.TypedColumnPrepareStateBuildNanos,
				row.TypedColumnPrepareDictionaryNanos,
				row.TypedColumnPreparePruningNanos,
				row.TypedColumnPrepareSortKeyNanos,
				row.TypedColumnPrepareStatsNanos,
				row.TypedColumnPrepareRangeReadNanos,
				row.TypedColumnPrepareRangeReadBytes,
				row.TypedColumnPrepareAdapterNanos,
				row.TypedColumnPrepareDenseGroupNanos,
				row.TypedColumnPrepareDenseValueNanos,
				row.TypedColumnPrepareDensePredicateNanos,
				row.TypedColumnPrepareDensePreapplyNanos,
			)
		}
	}
	fmt.Fprintf(&buf, "\n## Best Runtime By Query\n\n")
	best := bestByScaleQuery(doc.Rows)
	fmt.Fprintf(&buf, "| rows/scale | query | fastest system/layout | best | TreeDB best | DuckDB best | ClickHouse best | TreeDB / ClickHouse |\n")
	fmt.Fprintf(&buf, "|---|---:|---|---:|---:|---:|---:|---:|\n")
	keys := make([]string, 0, len(best))
	for key := range best {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		group := best[key]
		tree := group.treeBest()
		duck := group.duckBest()
		click := group.clickHouseBest()
		ratio := ""
		if tree != nil && click != nil && click.BestSec > 0 {
			ratio = fmt.Sprintf("%.2fx", tree.BestSec/click.BestSec)
		}
		fastest := group.fastest()
		fastestLabel := ""
		fastestBest := ""
		if fastest != nil {
			fastestLabel = fastest.System + " " + reportRowLayout(*fastest)
			fastestBest = formatSeconds(fastest.BestSec)
		}
		fmt.Fprintf(
			&buf,
			"| %s | %s | %s | %s | %s | %s | %s | %s |\n",
			group.Scale,
			group.Query,
			fastestLabel,
			fastestBest,
			formatOptionalRowSeconds(tree),
			formatOptionalRowSeconds(duck),
			formatOptionalRowSeconds(click),
			ratio,
		)
	}
	return buf.Bytes()
}

func reportRowLayout(row reportRow) string {
	layout := row.Format
	if row.Projection != "" {
		layout += "/" + row.Projection
	}
	if row.StorageLayout != "" && row.StorageLayout != storageLayoutRow {
		layout = row.StorageLayout + ":" + layout
	}
	if row.DataRoot != "" && row.DataRoot != "fast" {
		layout += "@" + row.DataRoot
	}
	if row.Compacted {
		layout += "+compacted"
	}
	if row.RetainedPayloadEncoding != "" && row.RetainedPayloadEncoding != "none" {
		layout += "+retained=" + row.RetainedPayloadEncoding
	}
	return layout
}

func reportRowDataShape(row reportRow) string {
	if strings.TrimSpace(row.DataShape) != "" {
		return row.DataShape
	}
	if row.RetainsJSON != nil && *row.RetainsJSON {
		return "full-json"
	}
	if isColumnStoreLayout(row.StorageLayout) {
		return "query-shaped-projection"
	}
	return ""
}

func nonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func formatDiagnosticBool(ready, used bool, fallback string) string {
	switch {
	case used:
		return "used"
	case ready:
		if strings.TrimSpace(fallback) != "" {
			return "ready/" + fallback
		}
		return "ready"
	case strings.TrimSpace(fallback) != "":
		return fallback
	default:
		return ""
	}
}

func formatDensePath(row reportRow) string {
	var paths []string
	if row.DenseGroupCountUsed {
		paths = append(paths, "group_count")
	}
	if row.DenseGroupCountDistinctUsed {
		paths = append(paths, "group_count_distinct")
	}
	if row.DenseGroupHourCountUsed {
		paths = append(paths, "group_hour_count")
	}
	if row.DenseInt64SpanUsed {
		paths = append(paths, "int64_span")
	}
	return strings.Join(paths, ",")
}

func reportHasExpressionEvidence(rows []reportRow) bool {
	for _, row := range rows {
		if row.System == "TreeDB" && reportRowHasExpressionEvidence(row) {
			return true
		}
	}
	return false
}

func reportRowHasExpressionEvidence(row reportRow) bool {
	return row.ExpressionKind != "" ||
		row.TypedCellsVisited != nil ||
		row.TypedCellsBasis != "" ||
		row.PrecomputedExpressionUsed != nil
}

func formatOptionalInt(value *int) string {
	if value == nil {
		return ""
	}
	return strconv.Itoa(*value)
}

func formatOptionalBool(value *bool) string {
	if value == nil {
		return ""
	}
	return strconv.FormatBool(*value)
}

func formatExpressionEvidence(row reportRow) string {
	if !reportRowHasExpressionEvidence(row) {
		return "n/a"
	}
	parts := make([]string, 0, 3)
	if row.ExpressionKind != "" {
		parts = append(parts, row.ExpressionKind)
	}
	if row.TypedCellsVisited != nil {
		cells := formatColumnSummaryCount(*row.TypedCellsVisited)
		if row.TypedCellsBasis != "" {
			cells += " typed cells (" + row.TypedCellsBasis + ")"
		} else {
			cells += " typed cells"
		}
		parts = append(parts, cells)
	}
	if row.PrecomputedExpressionUsed != nil {
		parts = append(parts, "precomputed_expression_used="+strconv.FormatBool(*row.PrecomputedExpressionUsed))
	}
	return strings.Join(parts, "; ")
}

func reportHasTypedColumnSetupDiagnostics(rows []reportRow) bool {
	for _, row := range rows {
		if row.System == "TreeDB" && reportRowHasTypedColumnSetupDiagnostics(row) {
			return true
		}
	}
	return false
}

func reportRowHasTypedColumnSetupDiagnostics(row reportRow) bool {
	return row.TypedColumnOneShotBuildNanos != 0 ||
		row.TypedColumnPrepareWorkerCount != 0 ||
		row.TypedColumnPreparePlanNanos != 0 ||
		row.TypedColumnPrepareRefsNanos != 0 ||
		row.TypedColumnPreparePairingNanos != 0 ||
		row.TypedColumnPreparePartDecodeNanos != 0 ||
		row.TypedColumnPreparePostPrepareNanos != 0 ||
		row.TypedColumnPrepareSummaryNanos != 0 ||
		row.TypedColumnOneShotCacheStoreNanos != 0 ||
		row.TypedColumnPrepareReadImageNanos != 0 ||
		row.TypedColumnPrepareStateBuildNanos != 0 ||
		row.TypedColumnPrepareDictionaryNanos != 0 ||
		row.TypedColumnPreparePruningNanos != 0 ||
		row.TypedColumnPrepareSortKeyNanos != 0 ||
		row.TypedColumnPrepareStatsNanos != 0 ||
		row.TypedColumnPrepareRangeReadNanos != 0 ||
		row.TypedColumnPrepareRangeReadBytes != 0 ||
		row.TypedColumnPrepareAdapterNanos != 0 ||
		row.TypedColumnPrepareDenseGroupNanos != 0 ||
		row.TypedColumnPrepareDenseValueNanos != 0 ||
		row.TypedColumnPrepareDensePredicateNanos != 0 ||
		row.TypedColumnPrepareDensePreapplyNanos != 0 ||
		row.TypedColumnPrepareQ2GroupRankNanos != 0 ||
		row.TypedColumnPrepareQ2DistinctRankNanos != 0 ||
		row.TypedColumnPrepareQ2LocalRankNanos != 0
}

type bestGroup struct {
	Scale string
	Query string
	Rows  []reportRow
}

func bestByScaleQuery(rows []reportRow) map[string]*bestGroup {
	out := make(map[string]*bestGroup)
	for _, row := range rows {
		key := row.Scale + "\x00" + row.Query
		group := out[key]
		if group == nil {
			group = &bestGroup{Scale: row.Scale, Query: row.Query}
			out[key] = group
		}
		group.Rows = append(group.Rows, row)
	}
	return out
}

func (g *bestGroup) fastest() *reportRow {
	return bestMatchingRow(g.Rows, func(reportRow) bool { return true })
}

func (g *bestGroup) treeBest() *reportRow {
	return bestMatchingRow(g.Rows, func(row reportRow) bool { return row.System == "TreeDB" })
}

func (g *bestGroup) duckBest() *reportRow {
	return bestMatchingRow(g.Rows, func(row reportRow) bool { return row.System == "DuckDB" })
}

func (g *bestGroup) clickHouseBest() *reportRow {
	return bestMatchingRow(g.Rows, func(row reportRow) bool { return row.System == "ClickHouse" })
}

func bestMatchingRow(rows []reportRow, match func(reportRow) bool) *reportRow {
	var best *reportRow
	for i := range rows {
		if !match(rows[i]) {
			continue
		}
		if best == nil || rows[i].BestSec < best.BestSec {
			candidate := rows[i]
			best = &candidate
		}
	}
	return best
}

func sortReportRows(rows []reportRow) {
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		if a.DatasetSize != b.DatasetSize {
			return a.DatasetSize < b.DatasetSize
		}
		if a.System != b.System {
			return a.System < b.System
		}
		if a.Format != b.Format {
			return a.Format < b.Format
		}
		if a.StorageLayout != b.StorageLayout {
			return a.StorageLayout < b.StorageLayout
		}
		if a.Projection != b.Projection {
			return a.Projection < b.Projection
		}
		return a.Query < b.Query
	})
}

func scaleFromDatasetSize(size int) string {
	switch size {
	case 1_000_000:
		return "1m"
	case 10_000_000:
		return "10m"
	case 100_000_000:
		return "100m"
	case 1_000_000_000:
		return "1000m"
	default:
		return fmt.Sprintf("%d rows", size)
	}
}

func formatSeconds(value float64) string {
	if value <= 0 || math.IsNaN(value) || math.IsInf(value, 0) {
		return ""
	}
	if value < 0.001 {
		return fmt.Sprintf("%.1fus", value*1e6)
	}
	if value < 1 {
		return fmt.Sprintf("%.4fs", value)
	}
	return fmt.Sprintf("%.3fs", value)
}

func formatOptionalRowSeconds(row *reportRow) string {
	if row == nil {
		return ""
	}
	return formatSeconds(row.BestSec)
}

func formatAttempts(values []float64) string {
	if len(values) == 0 {
		return ""
	}
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, formatSeconds(value))
	}
	return strings.Join(parts, ", ")
}

func formatRowsPerSecond(rows int, seconds float64) string {
	if rows <= 0 || seconds <= 0 || math.IsNaN(seconds) || math.IsInf(seconds, 0) {
		return ""
	}
	return formatCount(int(math.Round(float64(rows) / seconds)))
}

func formatCount(value int) string {
	if value <= 0 {
		return ""
	}
	return strconv.Itoa(value)
}

func formatBytes(value int64) string {
	if value <= 0 {
		return ""
	}
	units := []string{"B", "KiB", "MiB", "GiB", "TiB"}
	f := float64(value)
	unit := 0
	for f >= 1024 && unit < len(units)-1 {
		f /= 1024
		unit++
	}
	if unit == 0 {
		return fmt.Sprintf("%d B", value)
	}
	return fmt.Sprintf("%.2f %s", f, units[unit])
}
