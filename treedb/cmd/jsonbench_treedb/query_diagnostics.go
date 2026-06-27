package main

import "github.com/snissn/gomap/TreeDB/collections"

type queryDiagnostics struct {
	QueryPath                     string                    `json:"query_path,omitempty"`
	StorageSource                 string                    `json:"storage_source,omitempty"`
	FallbackReason                string                    `json:"fallback_reason,omitempty"`
	RowsScanned                   int                       `json:"rows_scanned"`
	RowsMatched                   int                       `json:"rows_matched,omitempty"`
	ReduceRows                    int                       `json:"reduce_rows,omitempty"`
	ResultRows                    int                       `json:"result_rows,omitempty"`
	ResultGroups                  int                       `json:"result_groups,omitempty"`
	PredicateCount                int                       `json:"predicate_count,omitempty"`
	PredicateColumns              []string                  `json:"predicate_columns,omitempty"`
	PredicateKinds                []string                  `json:"predicate_kinds,omitempty"`
	PredicateLiterals             int                       `json:"predicate_literals,omitempty"`
	TopKLimit                     int                       `json:"topk_limit,omitempty"`
	TopKOrder                     string                    `json:"topk_order,omitempty"`
	TopKCandidates                int                       `json:"topk_candidates,omitempty"`
	BoundedTopKUsed               bool                      `json:"bounded_topk_used,omitempty"`
	TimeOrderTopKUsed             bool                      `json:"time_order_topk_used,omitempty"`
	SortKeyPrefixPlanned          bool                      `json:"sort_key_prefix_planned,omitempty"`
	SortKeyPrefixColumns          []string                  `json:"sort_key_prefix_columns,omitempty"`
	SortKeyPrefixLiterals         int                       `json:"sort_key_prefix_literals,omitempty"`
	SortKeyMarkChecks             int                       `json:"sort_key_mark_checks,omitempty"`
	SortKeyMarkMatches            int                       `json:"sort_key_mark_matches,omitempty"`
	SortKeyMarkSkips              int                       `json:"sort_key_mark_skips,omitempty"`
	SortKeyMarkFallbackReason     string                    `json:"sort_key_mark_fallback_reason,omitempty"`
	SortedGroupedDistinctReady    bool                      `json:"sorted_grouped_distinct_ready,omitempty"`
	SortedGroupedDistinctUsed     bool                      `json:"sorted_grouped_distinct_used,omitempty"`
	SortedGroupedDistinctFallback string                    `json:"sorted_grouped_distinct_fallback_reason,omitempty"`
	DenseGroupCountUsed           bool                      `json:"dense_group_count_used,omitempty"`
	DenseGroupCountDistinctUsed   bool                      `json:"dense_group_count_distinct_used,omitempty"`
	DenseGroupHourCountUsed       bool                      `json:"dense_group_hour_count_used,omitempty"`
	DenseInt64SpanUsed            bool                      `json:"dense_int64_span_used,omitempty"`
	MetadataHits                  int                       `json:"metadata_hits,omitempty"`
	MetadataEntries               int                       `json:"metadata_entries,omitempty"`
	MetadataMisses                int                       `json:"metadata_misses,omitempty"`
	DictionaryCodeHits            int                       `json:"dictionary_code_hits,omitempty"`
	PredicateDictionaryCodeHits   int                       `json:"predicate_dictionary_code_hits,omitempty"`
	Int64ValueHits                int                       `json:"int64_value_hits,omitempty"`
	ScheduledGranules             int                       `json:"scheduled_granules,omitempty"`
	SkippedGranules               int                       `json:"skipped_granules,omitempty"`
	DecodedGranules               int                       `json:"decoded_granules,omitempty"`
	DecodedBlocks                 int                       `json:"decoded_blocks,omitempty"`
	DirectReduceBlocks            int                       `json:"direct_reduce_blocks,omitempty"`
	TypedColumnPartSections       int                       `json:"typed_column_part_sections,omitempty"`
	TypedColumnPartSectionBytes   uint64                    `json:"typed_column_part_section_bytes,omitempty"`
	DecodedPayloadBytes           uint64                    `json:"decoded_payload_bytes,omitempty"`
	DecodedMetadataBytes          uint64                    `json:"decoded_metadata_bytes,omitempty"`
	PhysicalBytesScanned          int64                     `json:"physical_bytes_scanned,omitempty"`
	MappedBytes                   uint64                    `json:"mapped_bytes,omitempty"`
	HeapCopyBytes                 uint64                    `json:"heap_copy_bytes,omitempty"`
	RowMaterializations           int                       `json:"row_materializations,omitempty"`
	DocumentMaterializations      int                       `json:"document_materializations,omitempty"`
	FallbackReads                 int                       `json:"fallback_reads,omitempty"`
	VisibilityRows                int                       `json:"visibility_rows,omitempty"`
	ReconstructionRows            int                       `json:"reconstruction_rows,omitempty"`
	WorkerCount                   int                       `json:"worker_count,omitempty"`
	SegmentFileCacheHits          uint64                    `json:"segment_file_cache_hits,omitempty"`
	SegmentFileCacheMisses        uint64                    `json:"segment_file_cache_misses,omitempty"`
	TypedColumnOneShotCacheHit    bool                      `json:"typed_column_one_shot_cache_hit,omitempty"`
	TypedColumnOneShotCacheMiss   bool                      `json:"typed_column_one_shot_cache_miss,omitempty"`
	TypedColumnOneShotCacheBuild  bool                      `json:"typed_column_one_shot_cache_build,omitempty"`
	TypedColumnOneShotBuildNanos  int64                     `json:"typed_column_one_shot_build_nanos,omitempty"`
	ColumnAssetReadIntegrity      string                    `json:"column_asset_read_integrity,omitempty"`
	AggregateMetadataUsed         bool                      `json:"aggregate_metadata_used,omitempty"`
	JSONReconstructionUsed        bool                      `json:"json_reconstruction_used,omitempty"`
	PrepareSetupNanos             int64                     `json:"prepare_setup_nanos,omitempty"`
	RunNanos                      int64                     `json:"run_nanos,omitempty"`
	ScanNanos                     int64                     `json:"scan_nanos,omitempty"`
	VisibilityNanos               int64                     `json:"visibility_nanos,omitempty"`
	ReduceNanos                   int64                     `json:"reduce_nanos,omitempty"`
	ResultShapeNanos              int64                     `json:"result_shape_nanos,omitempty"`
	ReconstructionNanos           int64                     `json:"reconstruction_nanos,omitempty"`
	ResultRenderNanos             int64                     `json:"result_render_nanos,omitempty"`
	HashNanos                     int64                     `json:"hash_nanos,omitempty"`
	RenderHashNanos               int64                     `json:"render_hash_nanos,omitempty"`
	AttemptWallNanos              int64                     `json:"attempt_wall_nanos,omitempty"`
	TotalQueryNanos               int64                     `json:"total_query_nanos,omitempty"`
	PhysicalQueries               []queryPhysicalDiagnostic `json:"physical_queries,omitempty"`
}

type queryPhysicalDiagnostic struct {
	Name                          string   `json:"name,omitempty"`
	StorageSource                 string   `json:"storage_source,omitempty"`
	FallbackReason                string   `json:"fallback_reason,omitempty"`
	RowsScanned                   int      `json:"rows_scanned"`
	RowsMatched                   int      `json:"rows_matched,omitempty"`
	ReduceRows                    int      `json:"reduce_rows,omitempty"`
	ResultGroups                  int      `json:"result_groups,omitempty"`
	PredicateCount                int      `json:"predicate_count,omitempty"`
	PredicateColumns              []string `json:"predicate_columns,omitempty"`
	PredicateKinds                []string `json:"predicate_kinds,omitempty"`
	PredicateLiterals             int      `json:"predicate_literals,omitempty"`
	TopKLimit                     int      `json:"topk_limit,omitempty"`
	TopKOrder                     string   `json:"topk_order,omitempty"`
	TopKCandidates                int      `json:"topk_candidates,omitempty"`
	BoundedTopKUsed               bool     `json:"bounded_topk_used,omitempty"`
	TimeOrderTopKUsed             bool     `json:"time_order_topk_used,omitempty"`
	SortKeyPrefixPlanned          bool     `json:"sort_key_prefix_planned,omitempty"`
	SortKeyPrefixColumns          []string `json:"sort_key_prefix_columns,omitempty"`
	SortKeyPrefixLiterals         int      `json:"sort_key_prefix_literals,omitempty"`
	SortKeyMarkChecks             int      `json:"sort_key_mark_checks,omitempty"`
	SortKeyMarkMatches            int      `json:"sort_key_mark_matches,omitempty"`
	SortKeyMarkSkips              int      `json:"sort_key_mark_skips,omitempty"`
	SortKeyMarkFallbackReason     string   `json:"sort_key_mark_fallback_reason,omitempty"`
	SortedGroupedDistinctReady    bool     `json:"sorted_grouped_distinct_ready,omitempty"`
	SortedGroupedDistinctUsed     bool     `json:"sorted_grouped_distinct_used,omitempty"`
	SortedGroupedDistinctFallback string   `json:"sorted_grouped_distinct_fallback_reason,omitempty"`
	DenseGroupCountUsed           bool     `json:"dense_group_count_used,omitempty"`
	DenseGroupCountDistinctUsed   bool     `json:"dense_group_count_distinct_used,omitempty"`
	DenseGroupHourCountUsed       bool     `json:"dense_group_hour_count_used,omitempty"`
	DenseInt64SpanUsed            bool     `json:"dense_int64_span_used,omitempty"`
	MetadataHits                  int      `json:"metadata_hits,omitempty"`
	MetadataEntries               int      `json:"metadata_entries,omitempty"`
	MetadataMisses                int      `json:"metadata_misses,omitempty"`
	DictionaryCodeHits            int      `json:"dictionary_code_hits,omitempty"`
	PredicateDictionaryCodeHits   int      `json:"predicate_dictionary_code_hits,omitempty"`
	Int64ValueHits                int      `json:"int64_value_hits,omitempty"`
	ScheduledGranules             int      `json:"scheduled_granules,omitempty"`
	SkippedGranules               int      `json:"skipped_granules,omitempty"`
	DecodedGranules               int      `json:"decoded_granules,omitempty"`
	DecodedBlocks                 int      `json:"decoded_blocks,omitempty"`
	DirectReduceBlocks            int      `json:"direct_reduce_blocks,omitempty"`
	TypedColumnPartSections       int      `json:"typed_column_part_sections,omitempty"`
	TypedColumnPartSectionBytes   uint64   `json:"typed_column_part_section_bytes,omitempty"`
	DecodedPayloadBytes           uint64   `json:"decoded_payload_bytes,omitempty"`
	DecodedMetadataBytes          uint64   `json:"decoded_metadata_bytes,omitempty"`
	PhysicalBytesScanned          int64    `json:"physical_bytes_scanned,omitempty"`
	MappedBytes                   uint64   `json:"mapped_bytes,omitempty"`
	HeapCopyBytes                 uint64   `json:"heap_copy_bytes,omitempty"`
	RowMaterializations           int      `json:"row_materializations,omitempty"`
	DocumentMaterializations      int      `json:"document_materializations,omitempty"`
	FallbackReads                 int      `json:"fallback_reads,omitempty"`
	VisibilityRows                int      `json:"visibility_rows,omitempty"`
	ReconstructionRows            int      `json:"reconstruction_rows,omitempty"`
	WorkerCount                   int      `json:"worker_count,omitempty"`
	SegmentFileCacheHits          uint64   `json:"segment_file_cache_hits,omitempty"`
	SegmentFileCacheMisses        uint64   `json:"segment_file_cache_misses,omitempty"`
	TypedColumnOneShotCacheHit    bool     `json:"typed_column_one_shot_cache_hit,omitempty"`
	TypedColumnOneShotCacheMiss   bool     `json:"typed_column_one_shot_cache_miss,omitempty"`
	TypedColumnOneShotCacheBuild  bool     `json:"typed_column_one_shot_cache_build,omitempty"`
	TypedColumnOneShotBuildNanos  int64    `json:"typed_column_one_shot_build_nanos,omitempty"`
	ColumnAssetReadIntegrity      string   `json:"column_asset_read_integrity,omitempty"`
	ScanNanos                     int64    `json:"scan_nanos,omitempty"`
	VisibilityNanos               int64    `json:"visibility_nanos,omitempty"`
	ReduceNanos                   int64    `json:"reduce_nanos,omitempty"`
	ResultShapeNanos              int64    `json:"result_shape_nanos,omitempty"`
	ReconstructionNanos           int64    `json:"reconstruction_nanos,omitempty"`
	FallbackRowsUsed              bool     `json:"fallback_rows_used,omitempty"`
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
		out.SegmentFileCacheHits += phys.SegmentFileCacheHits
		out.SegmentFileCacheMisses += phys.SegmentFileCacheMisses
		out.TypedColumnOneShotCacheHit = out.TypedColumnOneShotCacheHit || phys.TypedColumnOneShotCacheHit
		out.TypedColumnOneShotCacheMiss = out.TypedColumnOneShotCacheMiss || phys.TypedColumnOneShotCacheMiss
		out.TypedColumnOneShotCacheBuild = out.TypedColumnOneShotCacheBuild || phys.TypedColumnOneShotCacheBuild
		out.TypedColumnOneShotBuildNanos += phys.TypedColumnOneShotBuildNanos
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
		Name:                          input.Name,
		StorageSource:                 string(d.StorageSource),
		FallbackReason:                string(d.FallbackReason),
		RowsScanned:                   rowsScanned,
		RowsMatched:                   d.RowsMatched,
		ReduceRows:                    d.ReduceRows,
		ResultGroups:                  d.ResultGroups,
		PredicateCount:                d.PredicateCount,
		PredicateColumns:              append([]string(nil), d.PredicateColumns...),
		PredicateKinds:                append([]string(nil), d.PredicateKinds...),
		PredicateLiterals:             d.PredicateLiterals,
		TopKLimit:                     d.TopKLimit,
		TopKOrder:                     d.TopKOrder,
		TopKCandidates:                d.TopKCandidates,
		BoundedTopKUsed:               d.TopKLimit > 0 && (d.TopKCandidates > 0 || d.TimeOrderTopKUsed),
		TimeOrderTopKUsed:             d.TimeOrderTopKUsed,
		SortKeyPrefixPlanned:          d.SortKeyPrefixPlanned,
		SortKeyPrefixColumns:          append([]string(nil), d.SortKeyPrefixColumns...),
		SortKeyPrefixLiterals:         d.SortKeyPrefixLiterals,
		SortKeyMarkChecks:             d.SortKeyMarkChecks,
		SortKeyMarkMatches:            d.SortKeyMarkMatches,
		SortKeyMarkSkips:              d.SortKeyMarkSkips,
		SortKeyMarkFallbackReason:     d.SortKeyMarkFallbackReason,
		SortedGroupedDistinctReady:    d.SortedGroupedDistinctReady,
		SortedGroupedDistinctUsed:     d.SortedGroupedDistinctUsed,
		SortedGroupedDistinctFallback: d.SortedGroupedDistinctFallbackReason,
		DenseGroupCountUsed:           d.DenseGroupCountUsed,
		DenseGroupCountDistinctUsed:   d.DenseGroupCountDistinctUsed,
		DenseGroupHourCountUsed:       d.DenseGroupHourCountUsed,
		DenseInt64SpanUsed:            d.DenseInt64SpanUsed,
		MetadataHits:                  d.MetadataHits,
		MetadataEntries:               d.MetadataEntries,
		MetadataMisses:                d.MetadataMisses,
		DictionaryCodeHits:            d.DictionaryCodeHits,
		PredicateDictionaryCodeHits:   d.PredicateDictionaryCodeHits,
		Int64ValueHits:                d.Int64ValueHits,
		ScheduledGranules:             d.ScheduledGranules,
		SkippedGranules:               d.SkippedGranules,
		DecodedGranules:               d.DecodedGranules,
		DecodedBlocks:                 d.DecodedBlocks,
		DirectReduceBlocks:            d.DirectReduceBlocks,
		TypedColumnPartSections:       d.TypedColumnPartSections,
		TypedColumnPartSectionBytes:   d.TypedColumnPartSectionBytes,
		DecodedPayloadBytes:           d.DecodedPayloadBytes,
		DecodedMetadataBytes:          d.DecodedMetadataBytes,
		PhysicalBytesScanned:          d.PhysicalBytesScanned,
		MappedBytes:                   d.MappedBytes,
		HeapCopyBytes:                 d.HeapCopyBytes,
		RowMaterializations:           d.RowMaterializations,
		DocumentMaterializations:      d.DocumentMaterializations,
		FallbackReads:                 d.FallbackReads,
		VisibilityRows:                d.VisibilityRows,
		ReconstructionRows:            d.ReconstructionRows,
		WorkerCount:                   d.WorkerCount,
		SegmentFileCacheHits:          d.SegmentFileCacheHits,
		SegmentFileCacheMisses:        d.SegmentFileCacheMisses,
		TypedColumnOneShotCacheHit:    d.TypedColumnOneShotCacheHit,
		TypedColumnOneShotCacheMiss:   d.TypedColumnOneShotCacheMiss,
		TypedColumnOneShotCacheBuild:  d.TypedColumnOneShotCacheBuild,
		TypedColumnOneShotBuildNanos:  d.TypedColumnOneShotBuildNanos,
		ColumnAssetReadIntegrity:      d.ColumnAssetReadIntegrity,
		ScanNanos:                     d.ScanNanos,
		VisibilityNanos:               d.VisibilityNanos,
		ReduceNanos:                   d.ReduceNanos,
		ResultShapeNanos:              d.ResultShapeNanos,
		ReconstructionNanos:           d.ReconstructionNanos,
		FallbackRowsUsed:              fallbackRowsUsed,
	}
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
