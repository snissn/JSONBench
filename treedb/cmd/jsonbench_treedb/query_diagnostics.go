package main

import (
	"reflect"

	"github.com/snissn/gomap/TreeDB/collections"
)

type queryDiagnostics struct {
	QueryPath                                             string                    `json:"query_path,omitempty"`
	StorageSource                                         string                    `json:"storage_source,omitempty"`
	FallbackReason                                        string                    `json:"fallback_reason,omitempty"`
	RowsScanned                                           int                       `json:"rows_scanned"`
	RowsMatched                                           int                       `json:"rows_matched,omitempty"`
	ReduceRows                                            int                       `json:"reduce_rows,omitempty"`
	ResultRows                                            int                       `json:"result_rows,omitempty"`
	ResultGroups                                          int                       `json:"result_groups,omitempty"`
	PredicateCount                                        int                       `json:"predicate_count,omitempty"`
	PredicateColumns                                      []string                  `json:"predicate_columns,omitempty"`
	PredicateKinds                                        []string                  `json:"predicate_kinds,omitempty"`
	PredicateLiterals                                     int                       `json:"predicate_literals,omitempty"`
	TopKLimit                                             int                       `json:"topk_limit,omitempty"`
	TopKOrder                                             string                    `json:"topk_order,omitempty"`
	TopKCandidates                                        int                       `json:"topk_candidates,omitempty"`
	BoundedTopKUsed                                       bool                      `json:"bounded_topk_used,omitempty"`
	TimeOrderTopKUsed                                     bool                      `json:"time_order_topk_used,omitempty"`
	SortKeyPrefixPlanned                                  bool                      `json:"sort_key_prefix_planned,omitempty"`
	SortKeyPrefixColumns                                  []string                  `json:"sort_key_prefix_columns,omitempty"`
	SortKeyPrefixLiterals                                 int                       `json:"sort_key_prefix_literals,omitempty"`
	SortKeyMarkChecks                                     int                       `json:"sort_key_mark_checks,omitempty"`
	SortKeyMarkMatches                                    int                       `json:"sort_key_mark_matches,omitempty"`
	SortKeyMarkSkips                                      int                       `json:"sort_key_mark_skips,omitempty"`
	SortKeyMarkFallbackReason                             string                    `json:"sort_key_mark_fallback_reason,omitempty"`
	SortedGroupedDistinctReady                            bool                      `json:"sorted_grouped_distinct_ready,omitempty"`
	SortedGroupedDistinctUsed                             bool                      `json:"sorted_grouped_distinct_used,omitempty"`
	SortedGroupedDistinctFallback                         string                    `json:"sorted_grouped_distinct_fallback_reason,omitempty"`
	DenseGroupCountUsed                                   bool                      `json:"dense_group_count_used,omitempty"`
	DenseGroupCountDistinctUsed                           bool                      `json:"dense_group_count_distinct_used,omitempty"`
	DenseGroupHourCountUsed                               bool                      `json:"dense_group_hour_count_used,omitempty"`
	DenseInt64SpanUsed                                    bool                      `json:"dense_int64_span_used,omitempty"`
	DenseInt64SpanPredicateBlocksSkipped                  int                       `json:"dense_int64_span_predicate_blocks_skipped"`
	MetadataHits                                          int                       `json:"metadata_hits,omitempty"`
	MetadataEntries                                       int                       `json:"metadata_entries,omitempty"`
	MetadataMisses                                        int                       `json:"metadata_misses,omitempty"`
	DictionaryCodeHits                                    int                       `json:"dictionary_code_hits,omitempty"`
	PredicateDictionaryCodeHits                           int                       `json:"predicate_dictionary_code_hits,omitempty"`
	Int64ValueHits                                        int                       `json:"int64_value_hits,omitempty"`
	ScheduledGranules                                     int                       `json:"scheduled_granules,omitempty"`
	SkippedGranules                                       int                       `json:"skipped_granules,omitempty"`
	DecodedGranules                                       int                       `json:"decoded_granules,omitempty"`
	DecodedBlocks                                         int                       `json:"decoded_blocks,omitempty"`
	DirectReduceBlocks                                    int                       `json:"direct_reduce_blocks,omitempty"`
	TypedColumnPartSections                               int                       `json:"typed_column_part_sections,omitempty"`
	TypedColumnPartSectionBytes                           uint64                    `json:"typed_column_part_section_bytes,omitempty"`
	DecodedPayloadBytes                                   uint64                    `json:"decoded_payload_bytes,omitempty"`
	DecodedMetadataBytes                                  uint64                    `json:"decoded_metadata_bytes,omitempty"`
	PhysicalBytesScanned                                  int64                     `json:"physical_bytes_scanned,omitempty"`
	MappedBytes                                           uint64                    `json:"mapped_bytes,omitempty"`
	HeapCopyBytes                                         uint64                    `json:"heap_copy_bytes,omitempty"`
	RowMaterializations                                   int                       `json:"row_materializations,omitempty"`
	DocumentMaterializations                              int                       `json:"document_materializations,omitempty"`
	FallbackReads                                         int                       `json:"fallback_reads,omitempty"`
	VisibilityRows                                        int                       `json:"visibility_rows,omitempty"`
	ReconstructionRows                                    int                       `json:"reconstruction_rows,omitempty"`
	WorkerCount                                           int                       `json:"worker_count,omitempty"`
	TypedColumnPrepareWorkerCount                         int                       `json:"typed_column_prepare_worker_count,omitempty"`
	SegmentFileCacheHits                                  uint64                    `json:"segment_file_cache_hits,omitempty"`
	SegmentFileCacheMisses                                uint64                    `json:"segment_file_cache_misses,omitempty"`
	TypedColumnOneShotCacheHit                            bool                      `json:"typed_column_one_shot_cache_hit,omitempty"`
	TypedColumnOneShotCacheMiss                           bool                      `json:"typed_column_one_shot_cache_miss,omitempty"`
	TypedColumnOneShotCacheBuild                          bool                      `json:"typed_column_one_shot_cache_build,omitempty"`
	TypedColumnOneShotBuildNanos                          int64                     `json:"typed_column_one_shot_build_nanos,omitempty"`
	TypedColumnPreparePlanNanos                           int64                     `json:"typed_column_prepare_plan_nanos,omitempty"`
	TypedColumnPrepareRefsNanos                           int64                     `json:"typed_column_prepare_refs_nanos,omitempty"`
	TypedColumnPreparePairingNanos                        int64                     `json:"typed_column_prepare_pairing_nanos,omitempty"`
	TypedColumnPreparePartDecodeNanos                     int64                     `json:"typed_column_prepare_part_decode_nanos,omitempty"`
	TypedColumnPreparePostPrepareNanos                    int64                     `json:"typed_column_prepare_post_prepare_nanos,omitempty"`
	TypedColumnPrepareSummaryNanos                        int64                     `json:"typed_column_prepare_summary_nanos,omitempty"`
	TypedColumnOneShotCacheStoreNanos                     int64                     `json:"typed_column_one_shot_cache_store_nanos,omitempty"`
	TypedColumnPrepareReadImageNanos                      int64                     `json:"typed_column_prepare_read_image_nanos,omitempty"`
	TypedColumnPrepareStateBuildNanos                     int64                     `json:"typed_column_prepare_state_build_nanos,omitempty"`
	TypedColumnPrepareDictionaryNanos                     int64                     `json:"typed_column_prepare_dictionary_nanos,omitempty"`
	TypedColumnPreparePruningNanos                        int64                     `json:"typed_column_prepare_pruning_nanos,omitempty"`
	TypedColumnPrepareSortKeyNanos                        int64                     `json:"typed_column_prepare_sort_key_nanos,omitempty"`
	TypedColumnPrepareStatsNanos                          int64                     `json:"typed_column_prepare_stats_nanos,omitempty"`
	TypedColumnPrepareRangeReadNanos                      int64                     `json:"typed_column_prepare_range_read_nanos,omitempty"`
	TypedColumnPrepareRangeReadBytes                      int64                     `json:"typed_column_prepare_range_read_bytes,omitempty"`
	TypedColumnPrepareAdapterNanos                        int64                     `json:"typed_column_prepare_adapter_nanos,omitempty"`
	TypedColumnPrepareDenseGroupNanos                     int64                     `json:"typed_column_prepare_dense_group_nanos,omitempty"`
	TypedColumnPrepareDenseValueNanos                     int64                     `json:"typed_column_prepare_dense_value_nanos,omitempty"`
	TypedColumnPrepareDensePredicateNanos                 int64                     `json:"typed_column_prepare_dense_predicate_nanos,omitempty"`
	TypedColumnPrepareDensePreapplyNanos                  int64                     `json:"typed_column_prepare_dense_preapply_nanos,omitempty"`
	TypedColumnPrepareQ2GroupRankNanos                    int64                     `json:"typed_column_prepare_q2_group_rank_nanos,omitempty"`
	TypedColumnPrepareQ2DistinctRankNanos                 int64                     `json:"typed_column_prepare_q2_distinct_rank_nanos,omitempty"`
	TypedColumnPrepareQ2LocalRankNanos                    int64                     `json:"typed_column_prepare_q2_local_rank_nanos,omitempty"`
	TypedColumnPrepareQ2DenseGroupGlobalRankNanos         int64                     `json:"typed_column_prepare_q2_dense_group_global_rank_nanos,omitempty"`
	TypedColumnPrepareQ2DenseDistinctGlobalRankNanos      int64                     `json:"typed_column_prepare_q2_dense_distinct_global_rank_nanos,omitempty"`
	TypedColumnPrepareQ2DensePartLocalRankNanos           int64                     `json:"typed_column_prepare_q2_dense_part_local_rank_nanos,omitempty"`
	TypedColumnPrepareQ2DenseDistinctRankPlanNanos        int64                     `json:"typed_column_prepare_q2_dense_distinct_rank_plan_nanos,omitempty"`
	TypedColumnPrepareQ2DenseDistinctRankCollectRefsNanos int64                     `json:"typed_column_prepare_q2_dense_distinct_rank_collect_refs_nanos,omitempty"`
	TypedColumnPrepareQ2DenseDistinctRankBuildShardsNanos int64                     `json:"typed_column_prepare_q2_dense_distinct_rank_build_shards_nanos,omitempty"`
	TypedColumnPrepareQ2DenseDistinctRankShardCount       int                       `json:"typed_column_prepare_q2_dense_distinct_rank_shard_count,omitempty"`
	TypedColumnPrepareQ2DenseDistinctRankRefs             int                       `json:"typed_column_prepare_q2_dense_distinct_rank_refs,omitempty"`
	TypedColumnPrepareQ2DenseDistinctRankMaxShardRefs     int                       `json:"typed_column_prepare_q2_dense_distinct_rank_max_shard_refs,omitempty"`
	TypedColumnPrepareQ2DenseDistinctGlobalRanks          int                       `json:"typed_column_prepare_q2_dense_distinct_global_ranks,omitempty"`
	TypedColumnPrepareQ2GroupGlobalDictionaryRankNanos    int64                     `json:"typed_column_prepare_q2_group_global_dictionary_rank_nanos,omitempty"`
	TypedColumnPrepareQ2DistinctGlobalDictionaryRankNanos int64                     `json:"typed_column_prepare_q2_distinct_global_dictionary_rank_nanos,omitempty"`
	TypedColumnPrepareQ2GroupGlobalLocalRankNanos         int64                     `json:"typed_column_prepare_q2_group_global_local_rank_nanos,omitempty"`
	TypedColumnPrepareQ2DistinctGlobalLocalRankNanos      int64                     `json:"typed_column_prepare_q2_distinct_global_local_rank_nanos,omitempty"`
	TypedColumnPrepareQ2GroupGlobalCodeRemapNanos         int64                     `json:"typed_column_prepare_q2_group_global_code_remap_nanos,omitempty"`
	TypedColumnPrepareQ2DistinctGlobalCodeRemapNanos      int64                     `json:"typed_column_prepare_q2_distinct_global_code_remap_nanos,omitempty"`
	ColumnAssetReadIntegrity                              string                    `json:"column_asset_read_integrity,omitempty"`
	AggregateMetadataUsed                                 bool                      `json:"aggregate_metadata_used,omitempty"`
	JSONReconstructionUsed                                bool                      `json:"json_reconstruction_used,omitempty"`
	PrepareSetupNanos                                     int64                     `json:"prepare_setup_nanos,omitempty"`
	RunNanos                                              int64                     `json:"run_nanos,omitempty"`
	ScanNanos                                             int64                     `json:"scan_nanos,omitempty"`
	VisibilityNanos                                       int64                     `json:"visibility_nanos,omitempty"`
	ReduceNanos                                           int64                     `json:"reduce_nanos,omitempty"`
	ResultShapeNanos                                      int64                     `json:"result_shape_nanos,omitempty"`
	ReconstructionNanos                                   int64                     `json:"reconstruction_nanos,omitempty"`
	ResultRenderNanos                                     int64                     `json:"result_render_nanos,omitempty"`
	HashNanos                                             int64                     `json:"hash_nanos,omitempty"`
	RenderHashNanos                                       int64                     `json:"render_hash_nanos,omitempty"`
	AttemptWallNanos                                      int64                     `json:"attempt_wall_nanos,omitempty"`
	TotalQueryNanos                                       int64                     `json:"total_query_nanos,omitempty"`
	PhysicalQueries                                       []queryPhysicalDiagnostic `json:"physical_queries,omitempty"`
}

type queryPhysicalDiagnostic struct {
	Name                                                  string   `json:"name,omitempty"`
	StorageSource                                         string   `json:"storage_source,omitempty"`
	FallbackReason                                        string   `json:"fallback_reason,omitempty"`
	RowsScanned                                           int      `json:"rows_scanned"`
	RowsMatched                                           int      `json:"rows_matched,omitempty"`
	ReduceRows                                            int      `json:"reduce_rows,omitempty"`
	ResultGroups                                          int      `json:"result_groups,omitempty"`
	PredicateCount                                        int      `json:"predicate_count,omitempty"`
	PredicateColumns                                      []string `json:"predicate_columns,omitempty"`
	PredicateKinds                                        []string `json:"predicate_kinds,omitempty"`
	PredicateLiterals                                     int      `json:"predicate_literals,omitempty"`
	TopKLimit                                             int      `json:"topk_limit,omitempty"`
	TopKOrder                                             string   `json:"topk_order,omitempty"`
	TopKCandidates                                        int      `json:"topk_candidates,omitempty"`
	BoundedTopKUsed                                       bool     `json:"bounded_topk_used,omitempty"`
	TimeOrderTopKUsed                                     bool     `json:"time_order_topk_used,omitempty"`
	SortKeyPrefixPlanned                                  bool     `json:"sort_key_prefix_planned,omitempty"`
	SortKeyPrefixColumns                                  []string `json:"sort_key_prefix_columns,omitempty"`
	SortKeyPrefixLiterals                                 int      `json:"sort_key_prefix_literals,omitempty"`
	SortKeyMarkChecks                                     int      `json:"sort_key_mark_checks,omitempty"`
	SortKeyMarkMatches                                    int      `json:"sort_key_mark_matches,omitempty"`
	SortKeyMarkSkips                                      int      `json:"sort_key_mark_skips,omitempty"`
	SortKeyMarkFallbackReason                             string   `json:"sort_key_mark_fallback_reason,omitempty"`
	SortedGroupedDistinctReady                            bool     `json:"sorted_grouped_distinct_ready,omitempty"`
	SortedGroupedDistinctUsed                             bool     `json:"sorted_grouped_distinct_used,omitempty"`
	SortedGroupedDistinctFallback                         string   `json:"sorted_grouped_distinct_fallback_reason,omitempty"`
	DenseGroupCountUsed                                   bool     `json:"dense_group_count_used,omitempty"`
	DenseGroupCountDistinctUsed                           bool     `json:"dense_group_count_distinct_used,omitempty"`
	DenseGroupHourCountUsed                               bool     `json:"dense_group_hour_count_used,omitempty"`
	DenseInt64SpanUsed                                    bool     `json:"dense_int64_span_used,omitempty"`
	DenseInt64SpanPredicateBlocksSkipped                  int      `json:"dense_int64_span_predicate_blocks_skipped"`
	MetadataHits                                          int      `json:"metadata_hits,omitempty"`
	MetadataEntries                                       int      `json:"metadata_entries,omitempty"`
	MetadataMisses                                        int      `json:"metadata_misses,omitempty"`
	DictionaryCodeHits                                    int      `json:"dictionary_code_hits,omitempty"`
	PredicateDictionaryCodeHits                           int      `json:"predicate_dictionary_code_hits,omitempty"`
	Int64ValueHits                                        int      `json:"int64_value_hits,omitempty"`
	ScheduledGranules                                     int      `json:"scheduled_granules,omitempty"`
	SkippedGranules                                       int      `json:"skipped_granules,omitempty"`
	DecodedGranules                                       int      `json:"decoded_granules,omitempty"`
	DecodedBlocks                                         int      `json:"decoded_blocks,omitempty"`
	DirectReduceBlocks                                    int      `json:"direct_reduce_blocks,omitempty"`
	TypedColumnPartSections                               int      `json:"typed_column_part_sections,omitempty"`
	TypedColumnPartSectionBytes                           uint64   `json:"typed_column_part_section_bytes,omitempty"`
	DecodedPayloadBytes                                   uint64   `json:"decoded_payload_bytes,omitempty"`
	DecodedMetadataBytes                                  uint64   `json:"decoded_metadata_bytes,omitempty"`
	PhysicalBytesScanned                                  int64    `json:"physical_bytes_scanned,omitempty"`
	MappedBytes                                           uint64   `json:"mapped_bytes,omitempty"`
	HeapCopyBytes                                         uint64   `json:"heap_copy_bytes,omitempty"`
	RowMaterializations                                   int      `json:"row_materializations,omitempty"`
	DocumentMaterializations                              int      `json:"document_materializations,omitempty"`
	FallbackReads                                         int      `json:"fallback_reads,omitempty"`
	VisibilityRows                                        int      `json:"visibility_rows,omitempty"`
	ReconstructionRows                                    int      `json:"reconstruction_rows,omitempty"`
	WorkerCount                                           int      `json:"worker_count,omitempty"`
	TypedColumnPrepareWorkerCount                         int      `json:"typed_column_prepare_worker_count,omitempty"`
	SegmentFileCacheHits                                  uint64   `json:"segment_file_cache_hits,omitempty"`
	SegmentFileCacheMisses                                uint64   `json:"segment_file_cache_misses,omitempty"`
	TypedColumnOneShotCacheHit                            bool     `json:"typed_column_one_shot_cache_hit,omitempty"`
	TypedColumnOneShotCacheMiss                           bool     `json:"typed_column_one_shot_cache_miss,omitempty"`
	TypedColumnOneShotCacheBuild                          bool     `json:"typed_column_one_shot_cache_build,omitempty"`
	TypedColumnOneShotBuildNanos                          int64    `json:"typed_column_one_shot_build_nanos,omitempty"`
	TypedColumnPreparePlanNanos                           int64    `json:"typed_column_prepare_plan_nanos,omitempty"`
	TypedColumnPrepareRefsNanos                           int64    `json:"typed_column_prepare_refs_nanos,omitempty"`
	TypedColumnPreparePairingNanos                        int64    `json:"typed_column_prepare_pairing_nanos,omitempty"`
	TypedColumnPreparePartDecodeNanos                     int64    `json:"typed_column_prepare_part_decode_nanos,omitempty"`
	TypedColumnPreparePostPrepareNanos                    int64    `json:"typed_column_prepare_post_prepare_nanos,omitempty"`
	TypedColumnPrepareSummaryNanos                        int64    `json:"typed_column_prepare_summary_nanos,omitempty"`
	TypedColumnOneShotCacheStoreNanos                     int64    `json:"typed_column_one_shot_cache_store_nanos,omitempty"`
	TypedColumnPrepareReadImageNanos                      int64    `json:"typed_column_prepare_read_image_nanos,omitempty"`
	TypedColumnPrepareStateBuildNanos                     int64    `json:"typed_column_prepare_state_build_nanos,omitempty"`
	TypedColumnPrepareDictionaryNanos                     int64    `json:"typed_column_prepare_dictionary_nanos,omitempty"`
	TypedColumnPreparePruningNanos                        int64    `json:"typed_column_prepare_pruning_nanos,omitempty"`
	TypedColumnPrepareSortKeyNanos                        int64    `json:"typed_column_prepare_sort_key_nanos,omitempty"`
	TypedColumnPrepareStatsNanos                          int64    `json:"typed_column_prepare_stats_nanos,omitempty"`
	TypedColumnPrepareRangeReadNanos                      int64    `json:"typed_column_prepare_range_read_nanos,omitempty"`
	TypedColumnPrepareRangeReadBytes                      int64    `json:"typed_column_prepare_range_read_bytes,omitempty"`
	TypedColumnPrepareAdapterNanos                        int64    `json:"typed_column_prepare_adapter_nanos,omitempty"`
	TypedColumnPrepareDenseGroupNanos                     int64    `json:"typed_column_prepare_dense_group_nanos,omitempty"`
	TypedColumnPrepareDenseValueNanos                     int64    `json:"typed_column_prepare_dense_value_nanos,omitempty"`
	TypedColumnPrepareDensePredicateNanos                 int64    `json:"typed_column_prepare_dense_predicate_nanos,omitempty"`
	TypedColumnPrepareDensePreapplyNanos                  int64    `json:"typed_column_prepare_dense_preapply_nanos,omitempty"`
	TypedColumnPrepareQ2GroupRankNanos                    int64    `json:"typed_column_prepare_q2_group_rank_nanos,omitempty"`
	TypedColumnPrepareQ2DistinctRankNanos                 int64    `json:"typed_column_prepare_q2_distinct_rank_nanos,omitempty"`
	TypedColumnPrepareQ2LocalRankNanos                    int64    `json:"typed_column_prepare_q2_local_rank_nanos,omitempty"`
	TypedColumnPrepareQ2DenseGroupGlobalRankNanos         int64    `json:"typed_column_prepare_q2_dense_group_global_rank_nanos,omitempty"`
	TypedColumnPrepareQ2DenseDistinctGlobalRankNanos      int64    `json:"typed_column_prepare_q2_dense_distinct_global_rank_nanos,omitempty"`
	TypedColumnPrepareQ2DensePartLocalRankNanos           int64    `json:"typed_column_prepare_q2_dense_part_local_rank_nanos,omitempty"`
	TypedColumnPrepareQ2DenseDistinctRankPlanNanos        int64    `json:"typed_column_prepare_q2_dense_distinct_rank_plan_nanos,omitempty"`
	TypedColumnPrepareQ2DenseDistinctRankCollectRefsNanos int64    `json:"typed_column_prepare_q2_dense_distinct_rank_collect_refs_nanos,omitempty"`
	TypedColumnPrepareQ2DenseDistinctRankBuildShardsNanos int64    `json:"typed_column_prepare_q2_dense_distinct_rank_build_shards_nanos,omitempty"`
	TypedColumnPrepareQ2DenseDistinctRankShardCount       int      `json:"typed_column_prepare_q2_dense_distinct_rank_shard_count,omitempty"`
	TypedColumnPrepareQ2DenseDistinctRankRefs             int      `json:"typed_column_prepare_q2_dense_distinct_rank_refs,omitempty"`
	TypedColumnPrepareQ2DenseDistinctRankMaxShardRefs     int      `json:"typed_column_prepare_q2_dense_distinct_rank_max_shard_refs,omitempty"`
	TypedColumnPrepareQ2DenseDistinctGlobalRanks          int      `json:"typed_column_prepare_q2_dense_distinct_global_ranks,omitempty"`
	TypedColumnPrepareQ2GroupGlobalDictionaryRankNanos    int64    `json:"typed_column_prepare_q2_group_global_dictionary_rank_nanos,omitempty"`
	TypedColumnPrepareQ2DistinctGlobalDictionaryRankNanos int64    `json:"typed_column_prepare_q2_distinct_global_dictionary_rank_nanos,omitempty"`
	TypedColumnPrepareQ2GroupGlobalLocalRankNanos         int64    `json:"typed_column_prepare_q2_group_global_local_rank_nanos,omitempty"`
	TypedColumnPrepareQ2DistinctGlobalLocalRankNanos      int64    `json:"typed_column_prepare_q2_distinct_global_local_rank_nanos,omitempty"`
	TypedColumnPrepareQ2GroupGlobalCodeRemapNanos         int64    `json:"typed_column_prepare_q2_group_global_code_remap_nanos,omitempty"`
	TypedColumnPrepareQ2DistinctGlobalCodeRemapNanos      int64    `json:"typed_column_prepare_q2_distinct_global_code_remap_nanos,omitempty"`
	ColumnAssetReadIntegrity                              string   `json:"column_asset_read_integrity,omitempty"`
	ScanNanos                                             int64    `json:"scan_nanos,omitempty"`
	VisibilityNanos                                       int64    `json:"visibility_nanos,omitempty"`
	ReduceNanos                                           int64    `json:"reduce_nanos,omitempty"`
	ResultShapeNanos                                      int64    `json:"result_shape_nanos,omitempty"`
	ReconstructionNanos                                   int64    `json:"reconstruction_nanos,omitempty"`
	FallbackRowsUsed                                      bool     `json:"fallback_rows_used,omitempty"`
}

type namedColumnPhysicalResult struct {
	Name         string
	Result       collections.ColumnPhysicalQueryResult
	FallbackRows int
}

type namedTypedInt64AggregateResult struct {
	Name         string
	Result       collections.TypedColumnInt64PredicateAggregateResult
	FallbackRows int
}

func rowScanQueryDiagnostics(rowsScanned, rowsMatched, resultRows int, renderNanos int64) queryDiagnostics {
	return queryDiagnostics{
		QueryPath:                "document_row_scan",
		StorageSource:            "primary_document_btree",
		FallbackReason:           "row_scan_baseline",
		RowsScanned:              rowsScanned,
		RowsMatched:              rowsMatched,
		ReduceRows:               rowsMatched,
		ResultRows:               resultRows,
		ResultGroups:             resultRows,
		RowMaterializations:      rowsScanned,
		DocumentMaterializations: rowsScanned,
		ResultRenderNanos:        renderNanos,
	}
}

func columnQueryDiagnostics(resultRows int, renderNanos int64, inputs ...namedColumnPhysicalResult) queryDiagnostics {
	out := queryDiagnostics{
		QueryPath:         "column_physical",
		ResultRows:        resultRows,
		ResultGroups:      resultRows,
		ResultRenderNanos: renderNanos,
	}
	for _, input := range inputs {
		phys := physicalQueryDiagnostic(input)
		out.PhysicalQueries = append(out.PhysicalQueries, phys)
		out.StorageSource = mergeDiagnosticString(out.StorageSource, phys.StorageSource)
		out.FallbackReason = mergeDiagnosticString(out.FallbackReason, phys.FallbackReason)
		out.RowsScanned = maxInt(out.RowsScanned, phys.RowsScanned)
		out.RowsMatched = maxInt(out.RowsMatched, phys.RowsMatched)
		out.ReduceRows = maxInt(out.ReduceRows, phys.ReduceRows)
		out.ResultGroups = maxInt(out.ResultGroups, phys.ResultGroups)
		out.PredicateCount = maxInt(out.PredicateCount, phys.PredicateCount)
		out.PredicateColumns = mergeStringSlices(out.PredicateColumns, phys.PredicateColumns)
		out.PredicateKinds = mergeStringSlices(out.PredicateKinds, phys.PredicateKinds)
		out.PredicateLiterals += phys.PredicateLiterals
		out.TopKLimit = maxInt(out.TopKLimit, phys.TopKLimit)
		out.TopKOrder = mergeDiagnosticString(out.TopKOrder, phys.TopKOrder)
		out.TopKCandidates = maxInt(out.TopKCandidates, phys.TopKCandidates)
		out.BoundedTopKUsed = out.BoundedTopKUsed || phys.BoundedTopKUsed
		out.TimeOrderTopKUsed = out.TimeOrderTopKUsed || phys.TimeOrderTopKUsed
		out.SortKeyPrefixPlanned = out.SortKeyPrefixPlanned || phys.SortKeyPrefixPlanned
		out.SortKeyPrefixColumns = mergeStringSlices(out.SortKeyPrefixColumns, phys.SortKeyPrefixColumns)
		out.SortKeyPrefixLiterals += phys.SortKeyPrefixLiterals
		out.SortKeyMarkChecks += phys.SortKeyMarkChecks
		out.SortKeyMarkMatches += phys.SortKeyMarkMatches
		out.SortKeyMarkSkips += phys.SortKeyMarkSkips
		out.SortKeyMarkFallbackReason = mergeDiagnosticString(out.SortKeyMarkFallbackReason, phys.SortKeyMarkFallbackReason)
		out.SortedGroupedDistinctReady = out.SortedGroupedDistinctReady || phys.SortedGroupedDistinctReady
		out.SortedGroupedDistinctUsed = out.SortedGroupedDistinctUsed || phys.SortedGroupedDistinctUsed
		out.SortedGroupedDistinctFallback = mergeDiagnosticString(out.SortedGroupedDistinctFallback, phys.SortedGroupedDistinctFallback)
		out.DenseGroupCountUsed = out.DenseGroupCountUsed || phys.DenseGroupCountUsed
		out.DenseGroupCountDistinctUsed = out.DenseGroupCountDistinctUsed || phys.DenseGroupCountDistinctUsed
		out.DenseGroupHourCountUsed = out.DenseGroupHourCountUsed || phys.DenseGroupHourCountUsed
		out.DenseInt64SpanUsed = out.DenseInt64SpanUsed || phys.DenseInt64SpanUsed
		out.DenseInt64SpanPredicateBlocksSkipped += phys.DenseInt64SpanPredicateBlocksSkipped
		out.MetadataHits += phys.MetadataHits
		out.MetadataEntries += phys.MetadataEntries
		out.MetadataMisses += phys.MetadataMisses
		out.DictionaryCodeHits += phys.DictionaryCodeHits
		out.PredicateDictionaryCodeHits += phys.PredicateDictionaryCodeHits
		out.Int64ValueHits += phys.Int64ValueHits
		out.ScheduledGranules += phys.ScheduledGranules
		out.SkippedGranules += phys.SkippedGranules
		out.DecodedGranules += phys.DecodedGranules
		out.DecodedBlocks += phys.DecodedBlocks
		out.DirectReduceBlocks += phys.DirectReduceBlocks
		out.TypedColumnPartSections += phys.TypedColumnPartSections
		out.TypedColumnPartSectionBytes += phys.TypedColumnPartSectionBytes
		out.DecodedPayloadBytes += phys.DecodedPayloadBytes
		out.DecodedMetadataBytes += phys.DecodedMetadataBytes
		out.PhysicalBytesScanned += phys.PhysicalBytesScanned
		out.MappedBytes += phys.MappedBytes
		out.HeapCopyBytes += phys.HeapCopyBytes
		out.RowMaterializations += phys.RowMaterializations
		out.DocumentMaterializations += phys.DocumentMaterializations
		out.FallbackReads += phys.FallbackReads
		out.VisibilityRows = maxInt(out.VisibilityRows, phys.VisibilityRows)
		out.ReconstructionRows += phys.ReconstructionRows
		out.WorkerCount = maxInt(out.WorkerCount, phys.WorkerCount)
		out.TypedColumnPrepareWorkerCount = maxInt(out.TypedColumnPrepareWorkerCount, phys.TypedColumnPrepareWorkerCount)
		out.SegmentFileCacheHits += phys.SegmentFileCacheHits
		out.SegmentFileCacheMisses += phys.SegmentFileCacheMisses
		out.TypedColumnOneShotCacheHit = out.TypedColumnOneShotCacheHit || phys.TypedColumnOneShotCacheHit
		out.TypedColumnOneShotCacheMiss = out.TypedColumnOneShotCacheMiss || phys.TypedColumnOneShotCacheMiss
		out.TypedColumnOneShotCacheBuild = out.TypedColumnOneShotCacheBuild || phys.TypedColumnOneShotCacheBuild
		out.TypedColumnOneShotBuildNanos += phys.TypedColumnOneShotBuildNanos
		out.TypedColumnPreparePlanNanos += phys.TypedColumnPreparePlanNanos
		out.TypedColumnPrepareRefsNanos += phys.TypedColumnPrepareRefsNanos
		out.TypedColumnPreparePairingNanos += phys.TypedColumnPreparePairingNanos
		out.TypedColumnPreparePartDecodeNanos += phys.TypedColumnPreparePartDecodeNanos
		out.TypedColumnPreparePostPrepareNanos += phys.TypedColumnPreparePostPrepareNanos
		out.TypedColumnPrepareSummaryNanos += phys.TypedColumnPrepareSummaryNanos
		out.TypedColumnOneShotCacheStoreNanos += phys.TypedColumnOneShotCacheStoreNanos
		out.TypedColumnPrepareReadImageNanos += phys.TypedColumnPrepareReadImageNanos
		out.TypedColumnPrepareStateBuildNanos += phys.TypedColumnPrepareStateBuildNanos
		out.TypedColumnPrepareDictionaryNanos += phys.TypedColumnPrepareDictionaryNanos
		out.TypedColumnPreparePruningNanos += phys.TypedColumnPreparePruningNanos
		out.TypedColumnPrepareSortKeyNanos += phys.TypedColumnPrepareSortKeyNanos
		out.TypedColumnPrepareStatsNanos += phys.TypedColumnPrepareStatsNanos
		out.TypedColumnPrepareRangeReadNanos += phys.TypedColumnPrepareRangeReadNanos
		out.TypedColumnPrepareRangeReadBytes += phys.TypedColumnPrepareRangeReadBytes
		out.TypedColumnPrepareAdapterNanos += phys.TypedColumnPrepareAdapterNanos
		out.TypedColumnPrepareDenseGroupNanos += phys.TypedColumnPrepareDenseGroupNanos
		out.TypedColumnPrepareDenseValueNanos += phys.TypedColumnPrepareDenseValueNanos
		out.TypedColumnPrepareDensePredicateNanos += phys.TypedColumnPrepareDensePredicateNanos
		out.TypedColumnPrepareDensePreapplyNanos += phys.TypedColumnPrepareDensePreapplyNanos
		out.TypedColumnPrepareQ2GroupRankNanos += phys.TypedColumnPrepareQ2GroupRankNanos
		out.TypedColumnPrepareQ2DistinctRankNanos += phys.TypedColumnPrepareQ2DistinctRankNanos
		out.TypedColumnPrepareQ2LocalRankNanos += phys.TypedColumnPrepareQ2LocalRankNanos
		out.TypedColumnPrepareQ2DenseGroupGlobalRankNanos += phys.TypedColumnPrepareQ2DenseGroupGlobalRankNanos
		out.TypedColumnPrepareQ2DenseDistinctGlobalRankNanos += phys.TypedColumnPrepareQ2DenseDistinctGlobalRankNanos
		out.TypedColumnPrepareQ2DensePartLocalRankNanos += phys.TypedColumnPrepareQ2DensePartLocalRankNanos
		out.TypedColumnPrepareQ2DenseDistinctRankPlanNanos += phys.TypedColumnPrepareQ2DenseDistinctRankPlanNanos
		out.TypedColumnPrepareQ2DenseDistinctRankCollectRefsNanos += phys.TypedColumnPrepareQ2DenseDistinctRankCollectRefsNanos
		out.TypedColumnPrepareQ2DenseDistinctRankBuildShardsNanos += phys.TypedColumnPrepareQ2DenseDistinctRankBuildShardsNanos
		out.TypedColumnPrepareQ2DenseDistinctRankShardCount = maxInt(out.TypedColumnPrepareQ2DenseDistinctRankShardCount, phys.TypedColumnPrepareQ2DenseDistinctRankShardCount)
		out.TypedColumnPrepareQ2DenseDistinctRankRefs += phys.TypedColumnPrepareQ2DenseDistinctRankRefs
		out.TypedColumnPrepareQ2DenseDistinctRankMaxShardRefs = maxInt(out.TypedColumnPrepareQ2DenseDistinctRankMaxShardRefs, phys.TypedColumnPrepareQ2DenseDistinctRankMaxShardRefs)
		out.TypedColumnPrepareQ2DenseDistinctGlobalRanks = maxInt(out.TypedColumnPrepareQ2DenseDistinctGlobalRanks, phys.TypedColumnPrepareQ2DenseDistinctGlobalRanks)
		out.TypedColumnPrepareQ2GroupGlobalDictionaryRankNanos += phys.TypedColumnPrepareQ2GroupGlobalDictionaryRankNanos
		out.TypedColumnPrepareQ2DistinctGlobalDictionaryRankNanos += phys.TypedColumnPrepareQ2DistinctGlobalDictionaryRankNanos
		out.TypedColumnPrepareQ2GroupGlobalLocalRankNanos += phys.TypedColumnPrepareQ2GroupGlobalLocalRankNanos
		out.TypedColumnPrepareQ2DistinctGlobalLocalRankNanos += phys.TypedColumnPrepareQ2DistinctGlobalLocalRankNanos
		out.TypedColumnPrepareQ2GroupGlobalCodeRemapNanos += phys.TypedColumnPrepareQ2GroupGlobalCodeRemapNanos
		out.TypedColumnPrepareQ2DistinctGlobalCodeRemapNanos += phys.TypedColumnPrepareQ2DistinctGlobalCodeRemapNanos
		out.ColumnAssetReadIntegrity = mergeDiagnosticString(out.ColumnAssetReadIntegrity, phys.ColumnAssetReadIntegrity)
		out.ScanNanos += phys.ScanNanos
		out.VisibilityNanos += phys.VisibilityNanos
		out.ReduceNanos += phys.ReduceNanos
		out.ResultShapeNanos += phys.ResultShapeNanos
		out.ReconstructionNanos += phys.ReconstructionNanos
	}
	return out
}

func typedInt64AggregateQueryDiagnostics(resultRows int, renderNanos int64, inputs ...namedTypedInt64AggregateResult) queryDiagnostics {
	out := queryDiagnostics{
		QueryPath:         "typed_column_int64_aggregate",
		ResultRows:        resultRows,
		ResultGroups:      resultRows,
		ResultRenderNanos: renderNanos,
	}
	for _, input := range inputs {
		phys := typedInt64AggregatePhysicalDiagnostic(input)
		out.PhysicalQueries = append(out.PhysicalQueries, phys)
		out.StorageSource = mergeDiagnosticString(out.StorageSource, phys.StorageSource)
		out.FallbackReason = mergeDiagnosticString(out.FallbackReason, phys.FallbackReason)
		out.RowsScanned = maxInt(out.RowsScanned, phys.RowsScanned)
		out.RowsMatched = maxInt(out.RowsMatched, phys.RowsMatched)
		out.ReduceRows = maxInt(out.ReduceRows, phys.ReduceRows)
		out.ResultGroups = maxInt(out.ResultGroups, phys.ResultGroups)
		out.DecodedBlocks += phys.DecodedBlocks
		out.DirectReduceBlocks += phys.DirectReduceBlocks
		out.TypedColumnPartSections += phys.TypedColumnPartSections
		out.TypedColumnPartSectionBytes += phys.TypedColumnPartSectionBytes
		out.DecodedPayloadBytes += phys.DecodedPayloadBytes
		out.DecodedMetadataBytes += phys.DecodedMetadataBytes
		out.PhysicalBytesScanned += phys.PhysicalBytesScanned
		out.MappedBytes += phys.MappedBytes
		out.HeapCopyBytes += phys.HeapCopyBytes
		out.RowMaterializations += phys.RowMaterializations
		out.DocumentMaterializations += phys.DocumentMaterializations
		out.FallbackReads += phys.FallbackReads
		out.SegmentFileCacheHits += phys.SegmentFileCacheHits
		out.SegmentFileCacheMisses += phys.SegmentFileCacheMisses
		out.ColumnAssetReadIntegrity = mergeDiagnosticString(out.ColumnAssetReadIntegrity, phys.ColumnAssetReadIntegrity)
		out.ScanNanos += phys.ScanNanos
	}
	return out
}

func physicalQueryDiagnostic(input namedColumnPhysicalResult) queryPhysicalDiagnostic {
	d := input.Result.Diagnostics
	rowsScanned := d.RowsScanned
	fallbackRowsUsed := false
	if rowsScanned == 0 && d.MetadataHits == 0 && d.DecodedMetadataBytes == 0 && input.FallbackRows > 0 {
		rowsScanned = input.FallbackRows
		fallbackRowsUsed = true
	}
	return queryPhysicalDiagnostic{
		Name:                                               input.Name,
		StorageSource:                                      string(d.StorageSource),
		FallbackReason:                                     string(d.FallbackReason),
		RowsScanned:                                        rowsScanned,
		RowsMatched:                                        d.RowsMatched,
		ReduceRows:                                         d.ReduceRows,
		ResultGroups:                                       d.ResultGroups,
		PredicateCount:                                     d.PredicateCount,
		PredicateColumns:                                   append([]string(nil), d.PredicateColumns...),
		PredicateKinds:                                     append([]string(nil), d.PredicateKinds...),
		PredicateLiterals:                                  d.PredicateLiterals,
		TopKLimit:                                          d.TopKLimit,
		TopKOrder:                                          d.TopKOrder,
		TopKCandidates:                                     d.TopKCandidates,
		BoundedTopKUsed:                                    d.TopKLimit > 0 && (d.TopKCandidates > 0 || d.TimeOrderTopKUsed),
		TimeOrderTopKUsed:                                  d.TimeOrderTopKUsed,
		SortKeyPrefixPlanned:                               d.SortKeyPrefixPlanned,
		SortKeyPrefixColumns:                               append([]string(nil), d.SortKeyPrefixColumns...),
		SortKeyPrefixLiterals:                              d.SortKeyPrefixLiterals,
		SortKeyMarkChecks:                                  d.SortKeyMarkChecks,
		SortKeyMarkMatches:                                 d.SortKeyMarkMatches,
		SortKeyMarkSkips:                                   d.SortKeyMarkSkips,
		SortKeyMarkFallbackReason:                          d.SortKeyMarkFallbackReason,
		SortedGroupedDistinctReady:                         d.SortedGroupedDistinctReady,
		SortedGroupedDistinctUsed:                          d.SortedGroupedDistinctUsed,
		SortedGroupedDistinctFallback:                      d.SortedGroupedDistinctFallbackReason,
		DenseGroupCountUsed:                                d.DenseGroupCountUsed,
		DenseGroupCountDistinctUsed:                        d.DenseGroupCountDistinctUsed,
		DenseGroupHourCountUsed:                            d.DenseGroupHourCountUsed,
		DenseInt64SpanUsed:                                 d.DenseInt64SpanUsed,
		DenseInt64SpanPredicateBlocksSkipped:               optionalColumnPhysicalDiagnosticInt(d, "DenseInt64SpanPredicateBlocksSkipped"),
		MetadataHits:                                       d.MetadataHits,
		MetadataEntries:                                    d.MetadataEntries,
		MetadataMisses:                                     d.MetadataMisses,
		DictionaryCodeHits:                                 d.DictionaryCodeHits,
		PredicateDictionaryCodeHits:                        d.PredicateDictionaryCodeHits,
		Int64ValueHits:                                     d.Int64ValueHits,
		ScheduledGranules:                                  d.ScheduledGranules,
		SkippedGranules:                                    d.SkippedGranules,
		DecodedGranules:                                    d.DecodedGranules,
		DecodedBlocks:                                      d.DecodedBlocks,
		DirectReduceBlocks:                                 d.DirectReduceBlocks,
		TypedColumnPartSections:                            d.TypedColumnPartSections,
		TypedColumnPartSectionBytes:                        d.TypedColumnPartSectionBytes,
		DecodedPayloadBytes:                                d.DecodedPayloadBytes,
		DecodedMetadataBytes:                               d.DecodedMetadataBytes,
		PhysicalBytesScanned:                               d.PhysicalBytesScanned,
		MappedBytes:                                        d.MappedBytes,
		HeapCopyBytes:                                      d.HeapCopyBytes,
		RowMaterializations:                                d.RowMaterializations,
		DocumentMaterializations:                           d.DocumentMaterializations,
		FallbackReads:                                      d.FallbackReads,
		VisibilityRows:                                     d.VisibilityRows,
		ReconstructionRows:                                 d.ReconstructionRows,
		WorkerCount:                                        d.WorkerCount,
		TypedColumnPrepareWorkerCount:                      optionalColumnPhysicalDiagnosticInt(d, "TypedColumnPrepareWorkerCount"),
		SegmentFileCacheHits:                               d.SegmentFileCacheHits,
		SegmentFileCacheMisses:                             d.SegmentFileCacheMisses,
		TypedColumnOneShotCacheHit:                         d.TypedColumnOneShotCacheHit,
		TypedColumnOneShotCacheMiss:                        d.TypedColumnOneShotCacheMiss,
		TypedColumnOneShotCacheBuild:                       d.TypedColumnOneShotCacheBuild,
		TypedColumnOneShotBuildNanos:                       d.TypedColumnOneShotBuildNanos,
		TypedColumnPreparePlanNanos:                        optionalColumnPhysicalDiagnosticInt64(d, "TypedColumnPreparePlanNanos"),
		TypedColumnPrepareRefsNanos:                        optionalColumnPhysicalDiagnosticInt64(d, "TypedColumnPrepareRefsNanos"),
		TypedColumnPreparePairingNanos:                     optionalColumnPhysicalDiagnosticInt64(d, "TypedColumnPreparePairingNanos"),
		TypedColumnPreparePartDecodeNanos:                  optionalColumnPhysicalDiagnosticInt64(d, "TypedColumnPreparePartDecodeNanos"),
		TypedColumnPreparePostPrepareNanos:                 optionalColumnPhysicalDiagnosticInt64(d, "TypedColumnPreparePostPrepareNanos"),
		TypedColumnPrepareSummaryNanos:                     optionalColumnPhysicalDiagnosticInt64(d, "TypedColumnPrepareSummaryNanos"),
		TypedColumnOneShotCacheStoreNanos:                  optionalColumnPhysicalDiagnosticInt64(d, "TypedColumnOneShotCacheStoreNanos"),
		TypedColumnPrepareReadImageNanos:                   optionalColumnPhysicalDiagnosticInt64(d, "TypedColumnPrepareReadImageNanos"),
		TypedColumnPrepareStateBuildNanos:                  optionalColumnPhysicalDiagnosticInt64(d, "TypedColumnPrepareStateBuildNanos"),
		TypedColumnPrepareDictionaryNanos:                  optionalColumnPhysicalDiagnosticInt64(d, "TypedColumnPrepareDictionaryNanos"),
		TypedColumnPreparePruningNanos:                     optionalColumnPhysicalDiagnosticInt64(d, "TypedColumnPreparePruningNanos"),
		TypedColumnPrepareSortKeyNanos:                     optionalColumnPhysicalDiagnosticInt64(d, "TypedColumnPrepareSortKeyNanos"),
		TypedColumnPrepareStatsNanos:                       optionalColumnPhysicalDiagnosticInt64(d, "TypedColumnPrepareStatsNanos"),
		TypedColumnPrepareRangeReadNanos:                   optionalColumnPhysicalDiagnosticInt64(d, "TypedColumnPrepareRangeReadNanos"),
		TypedColumnPrepareRangeReadBytes:                   optionalColumnPhysicalDiagnosticInt64(d, "TypedColumnPrepareRangeReadBytes"),
		TypedColumnPrepareAdapterNanos:                     optionalColumnPhysicalDiagnosticInt64(d, "TypedColumnPrepareAdapterNanos"),
		TypedColumnPrepareDenseGroupNanos:                  optionalColumnPhysicalDiagnosticInt64(d, "TypedColumnPrepareDenseGroupNanos"),
		TypedColumnPrepareDenseValueNanos:                  optionalColumnPhysicalDiagnosticInt64(d, "TypedColumnPrepareDenseValueNanos"),
		TypedColumnPrepareDensePredicateNanos:              optionalColumnPhysicalDiagnosticInt64(d, "TypedColumnPrepareDensePredicateNanos"),
		TypedColumnPrepareDensePreapplyNanos:               optionalColumnPhysicalDiagnosticInt64(d, "TypedColumnPrepareDensePreapplyNanos"),
		TypedColumnPrepareQ2GroupRankNanos:                 optionalColumnPhysicalDiagnosticInt64(d, "TypedColumnPrepareQ2GroupRankNanos"),
		TypedColumnPrepareQ2DistinctRankNanos:              optionalColumnPhysicalDiagnosticInt64(d, "TypedColumnPrepareQ2DistinctRankNanos"),
		TypedColumnPrepareQ2LocalRankNanos:                 optionalColumnPhysicalDiagnosticInt64(d, "TypedColumnPrepareQ2LocalRankNanos"),
		TypedColumnPrepareQ2DenseGroupGlobalRankNanos:      optionalColumnPhysicalDiagnosticInt64(d, "TypedColumnPrepareQ2DenseGroupGlobalRankNanos"),
		TypedColumnPrepareQ2DenseDistinctGlobalRankNanos:   optionalColumnPhysicalDiagnosticInt64(d, "TypedColumnPrepareQ2DenseDistinctGlobalRankNanos"),
		TypedColumnPrepareQ2DensePartLocalRankNanos:        optionalColumnPhysicalDiagnosticInt64(d, "TypedColumnPrepareQ2DensePartLocalRankNanos"),
		TypedColumnPrepareQ2GroupGlobalDictionaryRankNanos: optionalColumnPhysicalDiagnosticInt64(d, "TypedColumnPrepareQ2GroupGlobalDictionaryRankNanos"),
		TypedColumnPrepareQ2DistinctGlobalDictionaryRankNanos: optionalColumnPhysicalDiagnosticInt64(d, "TypedColumnPrepareQ2DistinctGlobalDictionaryRankNanos"),
		TypedColumnPrepareQ2GroupGlobalLocalRankNanos:         optionalColumnPhysicalDiagnosticInt64(d, "TypedColumnPrepareQ2GroupGlobalLocalRankNanos"),
		TypedColumnPrepareQ2DistinctGlobalLocalRankNanos:      optionalColumnPhysicalDiagnosticInt64(d, "TypedColumnPrepareQ2DistinctGlobalLocalRankNanos"),
		TypedColumnPrepareQ2GroupGlobalCodeRemapNanos:         optionalColumnPhysicalDiagnosticInt64(d, "TypedColumnPrepareQ2GroupGlobalCodeRemapNanos"),
		TypedColumnPrepareQ2DistinctGlobalCodeRemapNanos:      optionalColumnPhysicalDiagnosticInt64(d, "TypedColumnPrepareQ2DistinctGlobalCodeRemapNanos"),
		ColumnAssetReadIntegrity:                              d.ColumnAssetReadIntegrity,
		ScanNanos:                                             d.ScanNanos,
		VisibilityNanos:                                       d.VisibilityNanos,
		ReduceNanos:                                           d.ReduceNanos,
		ResultShapeNanos:                                      d.ResultShapeNanos,
		ReconstructionNanos:                                   d.ReconstructionNanos,
		FallbackRowsUsed:                                      fallbackRowsUsed,

		TypedColumnPrepareQ2DenseDistinctRankPlanNanos:        optionalColumnPhysicalDiagnosticInt64(d, "TypedColumnPrepareQ2DenseDistinctRankPlanNanos"),
		TypedColumnPrepareQ2DenseDistinctRankCollectRefsNanos: optionalColumnPhysicalDiagnosticInt64(d, "TypedColumnPrepareQ2DenseDistinctRankCollectRefsNanos"),
		TypedColumnPrepareQ2DenseDistinctRankBuildShardsNanos: optionalColumnPhysicalDiagnosticInt64(d, "TypedColumnPrepareQ2DenseDistinctRankBuildShardsNanos"),
		TypedColumnPrepareQ2DenseDistinctRankShardCount:       optionalColumnPhysicalDiagnosticInt(d, "TypedColumnPrepareQ2DenseDistinctRankShardCount"),
		TypedColumnPrepareQ2DenseDistinctRankRefs:             optionalColumnPhysicalDiagnosticInt(d, "TypedColumnPrepareQ2DenseDistinctRankRefs"),
		TypedColumnPrepareQ2DenseDistinctRankMaxShardRefs:     optionalColumnPhysicalDiagnosticInt(d, "TypedColumnPrepareQ2DenseDistinctRankMaxShardRefs"),
		TypedColumnPrepareQ2DenseDistinctGlobalRanks:          optionalColumnPhysicalDiagnosticInt(d, "TypedColumnPrepareQ2DenseDistinctGlobalRanks"),
	}
}

func columnPhysicalRunnerPrepareDiagnostic(name string, runner *collections.ColumnPhysicalQueryRunner) (queryPhysicalDiagnostic, bool) {
	if runner == nil {
		return queryPhysicalDiagnostic{}, false
	}
	method := reflect.ValueOf(runner).MethodByName("PrepareDiagnostics")
	if !method.IsValid() || method.Type().NumIn() != 0 || method.Type().NumOut() != 1 {
		return queryPhysicalDiagnostic{}, false
	}
	values := method.Call(nil)
	if len(values) != 1 {
		return queryPhysicalDiagnostic{}, false
	}
	diagnostics, ok := values[0].Interface().(collections.ColumnPhysicalQueryDiagnostics)
	if !ok {
		return queryPhysicalDiagnostic{}, false
	}
	phys := physicalQueryDiagnostic(namedColumnPhysicalResult{
		Name: name,
		Result: collections.ColumnPhysicalQueryResult{
			Diagnostics: diagnostics,
		},
	})
	return phys, columnPhysicalPrepareDiagnosticsHasData(phys)
}

func columnPhysicalPrepareDiagnosticsHasData(phys queryPhysicalDiagnostic) bool {
	return phys.DenseInt64SpanPredicateBlocksSkipped > 0 ||
		phys.TypedColumnPrepareWorkerCount > 0 ||
		phys.TypedColumnPreparePlanNanos > 0 ||
		phys.TypedColumnPrepareRefsNanos > 0 ||
		phys.TypedColumnPreparePairingNanos > 0 ||
		phys.TypedColumnPreparePartDecodeNanos > 0 ||
		phys.TypedColumnPreparePostPrepareNanos > 0 ||
		phys.TypedColumnPrepareSummaryNanos > 0 ||
		phys.TypedColumnPrepareReadImageNanos > 0 ||
		phys.TypedColumnPrepareStateBuildNanos > 0 ||
		phys.TypedColumnPrepareDictionaryNanos > 0 ||
		phys.TypedColumnPreparePruningNanos > 0 ||
		phys.TypedColumnPrepareSortKeyNanos > 0 ||
		phys.TypedColumnPrepareStatsNanos > 0 ||
		phys.TypedColumnPrepareRangeReadNanos > 0 ||
		phys.TypedColumnPrepareRangeReadBytes > 0 ||
		phys.TypedColumnPrepareAdapterNanos > 0 ||
		phys.TypedColumnPrepareDenseGroupNanos > 0 ||
		phys.TypedColumnPrepareDenseValueNanos > 0 ||
		phys.TypedColumnPrepareDensePredicateNanos > 0 ||
		phys.TypedColumnPrepareDensePreapplyNanos > 0 ||
		phys.TypedColumnPrepareQ2GroupRankNanos > 0 ||
		phys.TypedColumnPrepareQ2DistinctRankNanos > 0 ||
		phys.TypedColumnPrepareQ2LocalRankNanos > 0 ||
		phys.TypedColumnPrepareQ2DenseGroupGlobalRankNanos > 0 ||
		phys.TypedColumnPrepareQ2DenseDistinctGlobalRankNanos > 0 ||
		phys.TypedColumnPrepareQ2DensePartLocalRankNanos > 0 ||
		phys.TypedColumnPrepareQ2DenseDistinctRankPlanNanos > 0 ||
		phys.TypedColumnPrepareQ2DenseDistinctRankCollectRefsNanos > 0 ||
		phys.TypedColumnPrepareQ2DenseDistinctRankBuildShardsNanos > 0 ||
		phys.TypedColumnPrepareQ2DenseDistinctRankShardCount > 0 ||
		phys.TypedColumnPrepareQ2DenseDistinctRankRefs > 0 ||
		phys.TypedColumnPrepareQ2DenseDistinctRankMaxShardRefs > 0 ||
		phys.TypedColumnPrepareQ2DenseDistinctGlobalRanks > 0 ||
		phys.TypedColumnPrepareQ2GroupGlobalDictionaryRankNanos > 0 ||
		phys.TypedColumnPrepareQ2DistinctGlobalDictionaryRankNanos > 0 ||
		phys.TypedColumnPrepareQ2GroupGlobalLocalRankNanos > 0 ||
		phys.TypedColumnPrepareQ2DistinctGlobalLocalRankNanos > 0 ||
		phys.TypedColumnPrepareQ2GroupGlobalCodeRemapNanos > 0 ||
		phys.TypedColumnPrepareQ2DistinctGlobalCodeRemapNanos > 0
}

func mergeColumnPhysicalPrepareDiagnostics(diag *queryDiagnostics, phys queryPhysicalDiagnostic) {
	mergeColumnPhysicalPrepareDiagnosticFields(diag, phys)
	for i := range diag.PhysicalQueries {
		if diag.PhysicalQueries[i].Name == phys.Name {
			mergeQueryPhysicalPrepareDiagnosticFields(&diag.PhysicalQueries[i], phys)
			return
		}
	}
	diag.PhysicalQueries = append(diag.PhysicalQueries, phys)
}

func mergeColumnPhysicalPrepareDiagnosticFields(diag *queryDiagnostics, phys queryPhysicalDiagnostic) {
	diag.DenseInt64SpanPredicateBlocksSkipped += phys.DenseInt64SpanPredicateBlocksSkipped
	diag.TypedColumnPrepareWorkerCount = maxInt(diag.TypedColumnPrepareWorkerCount, phys.TypedColumnPrepareWorkerCount)
	diag.TypedColumnPreparePlanNanos += phys.TypedColumnPreparePlanNanos
	diag.TypedColumnPrepareRefsNanos += phys.TypedColumnPrepareRefsNanos
	diag.TypedColumnPreparePairingNanos += phys.TypedColumnPreparePairingNanos
	diag.TypedColumnPreparePartDecodeNanos += phys.TypedColumnPreparePartDecodeNanos
	diag.TypedColumnPreparePostPrepareNanos += phys.TypedColumnPreparePostPrepareNanos
	diag.TypedColumnPrepareSummaryNanos += phys.TypedColumnPrepareSummaryNanos
	diag.TypedColumnPrepareReadImageNanos += phys.TypedColumnPrepareReadImageNanos
	diag.TypedColumnPrepareStateBuildNanos += phys.TypedColumnPrepareStateBuildNanos
	diag.TypedColumnPrepareDictionaryNanos += phys.TypedColumnPrepareDictionaryNanos
	diag.TypedColumnPreparePruningNanos += phys.TypedColumnPreparePruningNanos
	diag.TypedColumnPrepareSortKeyNanos += phys.TypedColumnPrepareSortKeyNanos
	diag.TypedColumnPrepareStatsNanos += phys.TypedColumnPrepareStatsNanos
	diag.TypedColumnPrepareRangeReadNanos += phys.TypedColumnPrepareRangeReadNanos
	diag.TypedColumnPrepareRangeReadBytes += phys.TypedColumnPrepareRangeReadBytes
	diag.TypedColumnPrepareAdapterNanos += phys.TypedColumnPrepareAdapterNanos
	diag.TypedColumnPrepareDenseGroupNanos += phys.TypedColumnPrepareDenseGroupNanos
	diag.TypedColumnPrepareDenseValueNanos += phys.TypedColumnPrepareDenseValueNanos
	diag.TypedColumnPrepareDensePredicateNanos += phys.TypedColumnPrepareDensePredicateNanos
	diag.TypedColumnPrepareDensePreapplyNanos += phys.TypedColumnPrepareDensePreapplyNanos
	diag.TypedColumnPrepareQ2GroupRankNanos += phys.TypedColumnPrepareQ2GroupRankNanos
	diag.TypedColumnPrepareQ2DistinctRankNanos += phys.TypedColumnPrepareQ2DistinctRankNanos
	diag.TypedColumnPrepareQ2LocalRankNanos += phys.TypedColumnPrepareQ2LocalRankNanos
	diag.TypedColumnPrepareQ2DenseGroupGlobalRankNanos += phys.TypedColumnPrepareQ2DenseGroupGlobalRankNanos
	diag.TypedColumnPrepareQ2DenseDistinctGlobalRankNanos += phys.TypedColumnPrepareQ2DenseDistinctGlobalRankNanos
	diag.TypedColumnPrepareQ2DensePartLocalRankNanos += phys.TypedColumnPrepareQ2DensePartLocalRankNanos
	diag.TypedColumnPrepareQ2DenseDistinctRankPlanNanos += phys.TypedColumnPrepareQ2DenseDistinctRankPlanNanos
	diag.TypedColumnPrepareQ2DenseDistinctRankCollectRefsNanos += phys.TypedColumnPrepareQ2DenseDistinctRankCollectRefsNanos
	diag.TypedColumnPrepareQ2DenseDistinctRankBuildShardsNanos += phys.TypedColumnPrepareQ2DenseDistinctRankBuildShardsNanos
	diag.TypedColumnPrepareQ2DenseDistinctRankShardCount = maxInt(diag.TypedColumnPrepareQ2DenseDistinctRankShardCount, phys.TypedColumnPrepareQ2DenseDistinctRankShardCount)
	diag.TypedColumnPrepareQ2DenseDistinctRankRefs += phys.TypedColumnPrepareQ2DenseDistinctRankRefs
	diag.TypedColumnPrepareQ2DenseDistinctRankMaxShardRefs = maxInt(diag.TypedColumnPrepareQ2DenseDistinctRankMaxShardRefs, phys.TypedColumnPrepareQ2DenseDistinctRankMaxShardRefs)
	diag.TypedColumnPrepareQ2DenseDistinctGlobalRanks = maxInt(diag.TypedColumnPrepareQ2DenseDistinctGlobalRanks, phys.TypedColumnPrepareQ2DenseDistinctGlobalRanks)
	diag.TypedColumnPrepareQ2GroupGlobalDictionaryRankNanos += phys.TypedColumnPrepareQ2GroupGlobalDictionaryRankNanos
	diag.TypedColumnPrepareQ2DistinctGlobalDictionaryRankNanos += phys.TypedColumnPrepareQ2DistinctGlobalDictionaryRankNanos
	diag.TypedColumnPrepareQ2GroupGlobalLocalRankNanos += phys.TypedColumnPrepareQ2GroupGlobalLocalRankNanos
	diag.TypedColumnPrepareQ2DistinctGlobalLocalRankNanos += phys.TypedColumnPrepareQ2DistinctGlobalLocalRankNanos
	diag.TypedColumnPrepareQ2GroupGlobalCodeRemapNanos += phys.TypedColumnPrepareQ2GroupGlobalCodeRemapNanos
	diag.TypedColumnPrepareQ2DistinctGlobalCodeRemapNanos += phys.TypedColumnPrepareQ2DistinctGlobalCodeRemapNanos
}

func mergeQueryPhysicalPrepareDiagnosticFields(dst *queryPhysicalDiagnostic, src queryPhysicalDiagnostic) {
	dst.DenseInt64SpanPredicateBlocksSkipped += src.DenseInt64SpanPredicateBlocksSkipped
	dst.TypedColumnPrepareWorkerCount = maxInt(dst.TypedColumnPrepareWorkerCount, src.TypedColumnPrepareWorkerCount)
	dst.TypedColumnPreparePlanNanos += src.TypedColumnPreparePlanNanos
	dst.TypedColumnPrepareRefsNanos += src.TypedColumnPrepareRefsNanos
	dst.TypedColumnPreparePairingNanos += src.TypedColumnPreparePairingNanos
	dst.TypedColumnPreparePartDecodeNanos += src.TypedColumnPreparePartDecodeNanos
	dst.TypedColumnPreparePostPrepareNanos += src.TypedColumnPreparePostPrepareNanos
	dst.TypedColumnPrepareSummaryNanos += src.TypedColumnPrepareSummaryNanos
	dst.TypedColumnPrepareReadImageNanos += src.TypedColumnPrepareReadImageNanos
	dst.TypedColumnPrepareStateBuildNanos += src.TypedColumnPrepareStateBuildNanos
	dst.TypedColumnPrepareDictionaryNanos += src.TypedColumnPrepareDictionaryNanos
	dst.TypedColumnPreparePruningNanos += src.TypedColumnPreparePruningNanos
	dst.TypedColumnPrepareSortKeyNanos += src.TypedColumnPrepareSortKeyNanos
	dst.TypedColumnPrepareStatsNanos += src.TypedColumnPrepareStatsNanos
	dst.TypedColumnPrepareRangeReadNanos += src.TypedColumnPrepareRangeReadNanos
	dst.TypedColumnPrepareRangeReadBytes += src.TypedColumnPrepareRangeReadBytes
	dst.TypedColumnPrepareAdapterNanos += src.TypedColumnPrepareAdapterNanos
	dst.TypedColumnPrepareDenseGroupNanos += src.TypedColumnPrepareDenseGroupNanos
	dst.TypedColumnPrepareDenseValueNanos += src.TypedColumnPrepareDenseValueNanos
	dst.TypedColumnPrepareDensePredicateNanos += src.TypedColumnPrepareDensePredicateNanos
	dst.TypedColumnPrepareDensePreapplyNanos += src.TypedColumnPrepareDensePreapplyNanos
	dst.TypedColumnPrepareQ2GroupRankNanos += src.TypedColumnPrepareQ2GroupRankNanos
	dst.TypedColumnPrepareQ2DistinctRankNanos += src.TypedColumnPrepareQ2DistinctRankNanos
	dst.TypedColumnPrepareQ2LocalRankNanos += src.TypedColumnPrepareQ2LocalRankNanos
	dst.TypedColumnPrepareQ2DenseGroupGlobalRankNanos += src.TypedColumnPrepareQ2DenseGroupGlobalRankNanos
	dst.TypedColumnPrepareQ2DenseDistinctGlobalRankNanos += src.TypedColumnPrepareQ2DenseDistinctGlobalRankNanos
	dst.TypedColumnPrepareQ2DensePartLocalRankNanos += src.TypedColumnPrepareQ2DensePartLocalRankNanos
	dst.TypedColumnPrepareQ2DenseDistinctRankPlanNanos += src.TypedColumnPrepareQ2DenseDistinctRankPlanNanos
	dst.TypedColumnPrepareQ2DenseDistinctRankCollectRefsNanos += src.TypedColumnPrepareQ2DenseDistinctRankCollectRefsNanos
	dst.TypedColumnPrepareQ2DenseDistinctRankBuildShardsNanos += src.TypedColumnPrepareQ2DenseDistinctRankBuildShardsNanos
	dst.TypedColumnPrepareQ2DenseDistinctRankShardCount = maxInt(dst.TypedColumnPrepareQ2DenseDistinctRankShardCount, src.TypedColumnPrepareQ2DenseDistinctRankShardCount)
	dst.TypedColumnPrepareQ2DenseDistinctRankRefs += src.TypedColumnPrepareQ2DenseDistinctRankRefs
	dst.TypedColumnPrepareQ2DenseDistinctRankMaxShardRefs = maxInt(dst.TypedColumnPrepareQ2DenseDistinctRankMaxShardRefs, src.TypedColumnPrepareQ2DenseDistinctRankMaxShardRefs)
	dst.TypedColumnPrepareQ2DenseDistinctGlobalRanks = maxInt(dst.TypedColumnPrepareQ2DenseDistinctGlobalRanks, src.TypedColumnPrepareQ2DenseDistinctGlobalRanks)
	dst.TypedColumnPrepareQ2GroupGlobalDictionaryRankNanos += src.TypedColumnPrepareQ2GroupGlobalDictionaryRankNanos
	dst.TypedColumnPrepareQ2DistinctGlobalDictionaryRankNanos += src.TypedColumnPrepareQ2DistinctGlobalDictionaryRankNanos
	dst.TypedColumnPrepareQ2GroupGlobalLocalRankNanos += src.TypedColumnPrepareQ2GroupGlobalLocalRankNanos
	dst.TypedColumnPrepareQ2DistinctGlobalLocalRankNanos += src.TypedColumnPrepareQ2DistinctGlobalLocalRankNanos
	dst.TypedColumnPrepareQ2GroupGlobalCodeRemapNanos += src.TypedColumnPrepareQ2GroupGlobalCodeRemapNanos
	dst.TypedColumnPrepareQ2DistinctGlobalCodeRemapNanos += src.TypedColumnPrepareQ2DistinctGlobalCodeRemapNanos
}

func optionalColumnPhysicalDiagnosticInt(d collections.ColumnPhysicalQueryDiagnostics, name string) int {
	field := reflect.ValueOf(d).FieldByName(name)
	if !field.IsValid() {
		return 0
	}
	switch field.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return int(field.Int())
	default:
		return 0
	}
}

func optionalColumnPhysicalDiagnosticInt64(d collections.ColumnPhysicalQueryDiagnostics, name string) int64 {
	field := reflect.ValueOf(d).FieldByName(name)
	if !field.IsValid() || field.Kind() != reflect.Int64 {
		return 0
	}
	return field.Int()
}

func typedInt64AggregatePhysicalDiagnostic(input namedTypedInt64AggregateResult) queryPhysicalDiagnostic {
	d := input.Result.Diagnostics
	rowsScanned := d.RowsScanned
	fallbackRowsUsed := false
	if rowsScanned == 0 && input.FallbackRows > 0 {
		rowsScanned = input.FallbackRows
		fallbackRowsUsed = true
	}
	rowsMatched := d.RowsMatched
	if rowsMatched == 0 && input.Result.Count > 0 {
		rowsMatched = int(input.Result.Count)
	}
	storageSource := "typed_column_part"
	fallbackReason := "none"
	if d.Fallback {
		storageSource = "primary_document_btree"
		fallbackReason = d.FallbackReason
		if fallbackReason == "" {
			fallbackReason = "document_fallback"
		}
	} else if d.FallbackReason != "" {
		fallbackReason = d.FallbackReason
	}
	physicalBytesScanned := d.PhysicalBytesScanned
	if physicalBytesScanned == 0 {
		physicalBytesScanned = int64(d.SectionBytesRead + d.RangeBytesRead + d.FullAssetBytes)
	}
	return queryPhysicalDiagnostic{
		Name:                        input.Name,
		StorageSource:               storageSource,
		FallbackReason:              fallbackReason,
		RowsScanned:                 rowsScanned,
		RowsMatched:                 rowsMatched,
		ReduceRows:                  int(input.Result.Count),
		ResultGroups:                1,
		DecodedBlocks:               d.BlocksDecoded,
		DirectReduceBlocks:          d.KernelBlocks + d.StatsBlocks,
		TypedColumnPartSections:     d.DirectTypedColumnAssetReads,
		TypedColumnPartSectionBytes: d.SectionBytesRead,
		DecodedPayloadBytes:         d.DecodedHeapCopyBytes,
		DecodedMetadataBytes:        d.DecodedMetadataBytes,
		PhysicalBytesScanned:        physicalBytesScanned,
		MappedBytes:                 d.MappedBytes,
		HeapCopyBytes:               d.HeapCopyBytes,
		RowMaterializations:         d.RowMaterializations,
		DocumentMaterializations:    d.DocumentMaterializations,
		FallbackReads:               d.FallbackReads,
		SegmentFileCacheHits:        d.SegmentFileCacheHits,
		SegmentFileCacheMisses:      d.SegmentFileCacheMisses,
		ColumnAssetReadIntegrity:    d.ColumnAssetReadIntegrity,
		ScanNanos:                   d.ScanNanos,
		FallbackRowsUsed:            fallbackRowsUsed,
	}
}

func typedInt64AggregateRowsScanned(fallback int, result collections.TypedColumnInt64PredicateAggregateResult) int {
	if result.Diagnostics.RowsScanned > 0 {
		return result.Diagnostics.RowsScanned
	}
	if fallback > 0 {
		return fallback
	}
	return int(result.Count)
}

func mergeDiagnosticString(existing, next string) string {
	if next == "" {
		return existing
	}
	if existing == "" {
		return next
	}
	if existing == next {
		return existing
	}
	return "mixed"
}

func mergeStringSlices(existing, next []string) []string {
	if len(next) == 0 {
		return existing
	}
	seen := make(map[string]struct{}, len(existing)+len(next))
	out := make([]string, 0, len(existing)+len(next))
	for _, value := range existing {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	for _, value := range next {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func maxInt(a, b int) int {
	if b > a {
		return b
	}
	return a
}
