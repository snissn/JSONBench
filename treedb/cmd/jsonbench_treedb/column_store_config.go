package main

import (
	"fmt"
	"strings"

	"github.com/snissn/gomap/TreeDB/collections"
)

const (
	storageLayoutRow                         = "row"
	storageLayoutColumnStore                 = "column-store"
	storageLayoutColumnStorePrepared         = "column-store-prepared"
	storageLayoutColumnStorePreparedMetadata = "column-store-prepared-metadata"
	storageLayoutColumnStoreFull             = "column-store-full"
	storageLayoutColumnStoreFullPrepared     = "column-store-full-prepared"

	queryModeOneShotEndToEnd     = "one_shot_end_to_end"
	queryModeFirstTouchAfterOpen = "first_touch_after_open"
	queryModeHotPreparedRun      = "hot_prepared_run"

	metadataModeAutoAggregateMetadata = "auto_aggregate_metadata"
	metadataModeNoAggregateMetadata   = "no_aggregate_metadata"

	columnStoreQ1AggregateMetadataName   = "event_count"
	columnStoreQ3AggregateMetadataName   = "feed_event_hour_count"
	columnStoreQ5AggregateMetadataName   = "post_time_us_minmax"
	columnStoreTopKAggregateMetadataName = "min_time_us"
)

func normalizeStorageLayout(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", storageLayoutRow, "rows", "row-store", "rowstore":
		return storageLayoutRow, nil
	case storageLayoutColumnStore, "column", "columns", "column_store", "columnstore":
		return storageLayoutColumnStore, nil
	case storageLayoutColumnStorePrepared, "column_store_prepared", "column-store-prepared-scan", "column_store_prepared_scan":
		return storageLayoutColumnStorePrepared, nil
	// Keep the dashed constants above as canonical output names; underscore and
	// shorter aggmeta aliases are accepted only for interactive shell convenience.
	case storageLayoutColumnStorePreparedMetadata, "column_store_prepared_metadata", "column-store-prepared-aggmeta", "column_store_prepared_aggmeta", "column-store-aggmeta", "column_store_aggmeta":
		return storageLayoutColumnStorePreparedMetadata, nil
	case storageLayoutColumnStoreFull, "column_store_full", "column-store-retained", "column_store_retained", "column-store-full-direct", "column_store_full_direct":
		return storageLayoutColumnStoreFull, nil
	case storageLayoutColumnStoreFullPrepared, "column_store_full_prepared", "column-store-retained-prepared", "column_store_retained_prepared", "column-store-full-prepared-scan", "column_store_full_prepared_scan":
		return storageLayoutColumnStoreFullPrepared, nil
	default:
		return "", fmt.Errorf("unsupported -storage-layout %q", raw)
	}
}

func normalizeQueryMode(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", queryModeOneShotEndToEnd, "one-shot", "one_shot", "oneshot", "one-shot-end-to-end":
		return queryModeOneShotEndToEnd, nil
	case queryModeFirstTouchAfterOpen, "first-touch", "first_touch", "first-touch-after-open":
		return queryModeFirstTouchAfterOpen, nil
	case queryModeHotPreparedRun, "hot-prepared", "hot_prepared", "prepared", "prepared_run":
		return queryModeHotPreparedRun, nil
	default:
		return "", fmt.Errorf("unsupported -query-mode %q", raw)
	}
}

func normalizeMetadataMode(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", metadataModeAutoAggregateMetadata, "auto", "aggregate_metadata", "aggmeta":
		return metadataModeAutoAggregateMetadata, nil
	case metadataModeNoAggregateMetadata, "no-aggregate-metadata", "no_aggmeta", "no-aggmeta", "scan":
		return metadataModeNoAggregateMetadata, nil
	default:
		return "", fmt.Errorf("unsupported -metadata-mode %q", raw)
	}
}

func validateStorageLayoutConfig(cfg runConfig) error {
	if !isColumnStoreLayout(cfg.StorageLayout) {
		return nil
	}
	if cfg.Format != "json" {
		return fmt.Errorf("-storage-layout %s currently supports -format json only", cfg.StorageLayout)
	}
	if isFullDataColumnStoreLayout(cfg.StorageLayout) {
		if cfg.Projection != "full" {
			return fmt.Errorf("-storage-layout %s requires -projection full, got %q", cfg.StorageLayout, cfg.Projection)
		}
		return nil
	}
	if cfg.Projection == "full" {
		return fmt.Errorf("-storage-layout %s requires a query projection (q1..q5 plus q4a/q4b/qexpr), not full", cfg.StorageLayout)
	}
	if len(cfg.Queries) != 1 {
		return fmt.Errorf("-storage-layout %s uses query-shaped column fixtures; pass exactly one -queries value", cfg.StorageLayout)
	}
	if cfg.Projection != cfg.Queries[0] {
		return fmt.Errorf("-storage-layout %s requires -projection to match -queries (got projection %q, query %q)", cfg.StorageLayout, cfg.Projection, cfg.Queries[0])
	}
	return nil
}

func isColumnStoreLayout(layout string) bool {
	switch layout {
	case storageLayoutColumnStore, storageLayoutColumnStorePrepared, storageLayoutColumnStorePreparedMetadata, storageLayoutColumnStoreFull, storageLayoutColumnStoreFullPrepared:
		return true
	default:
		return false
	}
}

func isFullDataColumnStoreLayout(layout string) bool {
	return layout == storageLayoutColumnStoreFull || layout == storageLayoutColumnStoreFullPrepared
}

func isPreparedColumnStoreLayout(layout string) bool {
	return layout == storageLayoutColumnStorePrepared || layout == storageLayoutColumnStorePreparedMetadata || layout == storageLayoutColumnStoreFullPrepared
}

func columnStoreUsesAggregateMetadata(layout, query string) bool {
	_, ok := columnStoreAggregateMetadataNameForQuery(layout, query)
	return ok
}

func columnStoreRequestUsesAggregateMetadata(cfg runConfig, query string) bool {
	if cfg.MetadataMode == metadataModeNoAggregateMetadata {
		return false
	}
	return columnStoreUsesAggregateMetadata(cfg.StorageLayout, query)
}

func columnStoreAggregateMetadataNameForQuery(layout, query string) (string, bool) {
	switch layout {
	case storageLayoutColumnStorePreparedMetadata:
		switch query {
		case "q4", "q4a", "q4b", "q5":
			return columnStoreTopKAggregateMetadataName, true
		default:
			return "", false
		}
	case storageLayoutColumnStoreFullPrepared:
		switch query {
		case "q1":
			return columnStoreQ1AggregateMetadataName, true
		case "q3":
			return columnStoreQ3AggregateMetadataName, true
		case "q5":
			return columnStoreQ5AggregateMetadataName, true
		default:
			return "", false
		}
	default:
		return "", false
	}
}

func columnStoreRequestsBoundedTopK(layout, query string) bool {
	switch query {
	case "q4", "q4a", "q4b", "q5":
		return layout == storageLayoutColumnStorePreparedMetadata || layout == storageLayoutColumnStoreFullPrepared
	default:
		return false
	}
}

func treeDBEngineName(cfg runConfig) string {
	switch cfg.StorageLayout {
	case storageLayoutColumnStore:
		return "treedb-collections-column-store-direct-go"
	case storageLayoutColumnStorePrepared:
		return "treedb-collections-column-store-prepared-scan-go"
	case storageLayoutColumnStorePreparedMetadata:
		return "treedb-collections-column-store-prepared-metadata-direct-go"
	case storageLayoutColumnStoreFull:
		return "treedb-collections-column-store-full-direct-go"
	case storageLayoutColumnStoreFullPrepared:
		return "treedb-collections-column-store-full-prepared-metadata-go"
	default:
		return "treedb-collections-direct-go"
	}
}

func runNotes(cfg runConfig) []string {
	if !isColumnStoreLayout(cfg.StorageLayout) {
		return nil
	}
	var inputNotes []string
	if cfg.AllowErrors {
		inputNotes = append(inputNotes, "allow-errors skips malformed JSON input rows before TreeDB insertion to match ClickHouse JSONAsObject allow-errors comparison runs.")
	}
	if isFullDataColumnStoreLayout(cfg.StorageLayout) {
		notes := []string{
			fmt.Sprintf("storage_layout=%s stores full source JSON by retaining non-declared fields and declaring JSONBench hot-path columns in TreeDB typed column parts.", cfg.StorageLayout),
			fmt.Sprintf("storage_layout=%s reconstructs documents from retained JSON plus typed columns; query timing uses physical column queries, not document reconstruction.", cfg.StorageLayout),
			fmt.Sprintf("storage_layout=%s forces TreeDB durable command-WAL mode because current column-store publication requires it.", cfg.StorageLayout),
			"full-data column-store cells do not use load-time sentinel masking; q2/q3/q4/q4a/q4b/q5 predicates are evaluated as real physical predicates or predicate-qualified aggregate metadata; qexpr scans/evaluates typed time_us cells.",
		}
		notes = append(notes, inputNotes...)
		if cfg.StorageLayout == storageLayoutColumnStoreFullPrepared {
			notes = append(notes, "column-store-full-prepared declares typed-column hot-path assets and aggregate metadata. one_shot_end_to_end and first_touch_after_open use the direct physical query APIs; hot_prepared_run prepares exact physical runners outside timed attempts. q1/q3/q5 request aggregate metadata when metadata_mode allows it, q4/q4a/q4b keep bounded physical TopK over typed-column part sections, and qexpr scans a typed int64 expression aggregate.")
		}
		return notes
	}
	notes := []string{
		fmt.Sprintf("storage_layout=%s stores declared projection fields in TreeDB physical column row assets with retained_payload=none.", cfg.StorageLayout),
		fmt.Sprintf("storage_layout=%s forces TreeDB durable command-WAL mode because current column-store publication requires it.", cfg.StorageLayout),
		"q3/q4/q4a/q4b/q5 column-store cells use physical dictionary predicates when supported; q4/q4a/q4b/q5 aggregate-metadata cells still use load-time sentinel masking for metadata semantics; qexpr scans/evaluates typed time_us cells; q2 remains sentinel-masked.",
	}
	notes = append(notes, inputNotes...)
	if cfg.StorageLayout == storageLayoutColumnStorePrepared {
		notes = append(notes, "column-store-prepared declares prepared storage assets and scans base column rows; one_shot_end_to_end and first_touch_after_open use direct physical query APIs, while hot_prepared_run prepares exact physical runners outside timed attempts. It does not declare aggregate metadata.")
	}
	if cfg.StorageLayout == storageLayoutColumnStorePreparedMetadata {
		notes = append(notes, "column-store-prepared-metadata declares prepared storage assets; one_shot_end_to_end and first_touch_after_open use direct physical query APIs, while hot_prepared_run prepares exact physical runners outside timed attempts. Only q4/q4a/q4b/q5 declare and use aggregate metadata named min_time_us.")
	}
	for _, q := range cfg.Queries {
		if q == "q3" {
			notes = append(notes, "q3 uses TreeDB's physical grouped-hour reducer over dictionary and int64 column sidecars.")
			break
		}
	}
	return notes
}

func columnStoreConfigForProjection(projection, storageLayout, retainedPayloadEncoding string) (*collections.ColumnStoreConfig, error) {
	fields, err := projectionFields(projection)
	if err != nil {
		return nil, err
	}
	retainedEncoding, hasRetainedEncoding, err := columnStoreRetainedPayloadEncodingOverride(retainedPayloadEncoding)
	if err != nil {
		return nil, err
	}
	fullData := isFullDataColumnStoreLayout(storageLayout)
	if fullData {
		fields = []string{"event", "did", "kind", "operation", "time_us"}
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("column-store layout requires a query projection, not %q", projection)
	}
	cfg := &collections.ColumnStoreConfig{
		Enabled:         true,
		RetainedPayload: collections.ColumnRetainedPayloadNone,
		Reconstruction:  collections.ColumnReconstructionRetainedPayloadAndColumns,
		ProfileSupport:  collections.ColumnStoreProfileBenchmarkRelaxed,
		Columns:         make([]collections.ColumnStoreColumn, 0, len(fields)),
	}
	if fullData {
		cfg.RetainedPayload = collections.ColumnRetainedPayloadNonColumn
		if hasRetainedEncoding {
			cfg.RetainedPayloadEncoding = retainedEncoding
		}
	}
	for _, field := range fields {
		col, err := columnStoreColumnForField(field, fullData)
		if err != nil {
			return nil, err
		}
		if projection == "qexpr" && field == "time_us" {
			col.Owner = collections.TypedStorageOwnerColumnPart
		}
		cfg.Columns = append(cfg.Columns, col)
		if field == "time_us" {
			cfg.SortKey = []collections.ColumnSortKey{{Column: "time_us"}}
		}
	}
	if fullData && storageLayout == storageLayoutColumnStoreFullPrepared {
		cfg.AggregateMetadata = columnStoreFullPreparedAggregateMetadata()
	} else if name, ok := columnStoreAggregateMetadataNameForQuery(storageLayout, projection); ok {
		cfg.AggregateMetadata = []collections.ColumnAggregateMetadata{{
			Name:        name,
			Column:      "time_us",
			GroupColumn: "did",
			Kind:        collections.ColumnAggregateMin,
		}}
	}
	return cfg, nil
}

func columnStoreFullPreparedAggregateMetadata() []collections.ColumnAggregateMetadata {
	return []collections.ColumnAggregateMetadata{
		{
			Name:        columnStoreQ1AggregateMetadataName,
			GroupColumn: "event",
			Kind:        collections.ColumnAggregateCount,
		},
		{
			Name:        columnStoreQ3AggregateMetadataName,
			Column:      "time_us",
			GroupColumn: "event",
			Kind:        collections.ColumnAggregateGroupHourCount,
			Predicates:  columnStoreQ3Predicates(),
		},
		{
			Name:        columnStoreQ5AggregateMetadataName,
			Column:      "time_us",
			GroupColumn: "did",
			Kind:        collections.ColumnAggregateMin,
			Predicates:  columnStorePostPredicates(),
		},
	}
}

func columnStoreRetainedPayloadEncodingOverride(raw string) (collections.ColumnRetainedPayloadEncoding, bool, error) {
	normalized := strings.ToLower(strings.TrimSpace(raw))
	switch normalized {
	case "", "default":
		return "", false, nil
	case "json":
		return collections.ColumnRetainedPayloadEncodingJSON, true, nil
	case "template-v1", "template_v1", "templatev1":
		return collections.ColumnRetainedPayloadEncodingTemplateV1, true, nil
	case "semantic-stream-v1", "semantic_stream_v1", "semanticstreamv1":
		return collections.ColumnRetainedPayloadEncodingSemanticStreamV1, true, nil
	default:
		return "", false, fmt.Errorf("unsupported retained payload encoding %q", raw)
	}
}

func columnStoreColumnForField(field string, fullData bool) (collections.ColumnStoreColumn, error) {
	col := collections.ColumnStoreColumn{
		Name:  field,
		Path:  field,
		Owner: collections.TypedStorageOwnerRowAsset,
	}
	if fullData {
		col.Owner = collections.TypedStorageOwnerColumnPart
		switch field {
		case "event":
			col.Path = "commit.collection"
		case "operation":
			col.Path = "commit.operation"
		}
	}
	switch field {
	case "event", "did", "kind", "operation":
		col.ValueType = collections.ColumnStoreValueString
		col.Dictionary = true
		col.Nullable = fullData
		return col, nil
	case "time_us":
		col.ValueType = collections.ColumnStoreValueInt64
		return col, nil
	default:
		return collections.ColumnStoreColumn{}, fmt.Errorf("unsupported column-store projection field %q", field)
	}
}

func applyColumnStoreQueryMask(fields *extractedFields, projection, storageLayout string) {
	if fields == nil {
		return
	}
	switch projection {
	case "q2":
		if fields.Kind != "commit" || fields.Operation != "create" {
			fields.Event = ""
			fields.DID = ""
		}
	case "q4", "q4a", "q4b", "q5":
		if columnStoreUsesAggregateMetadata(storageLayout, projection) && (fields.Kind != "commit" || fields.Operation != "create" || fields.Event != "app.bsky.feed.post") {
			fields.DID = ""
		}
	}
}
