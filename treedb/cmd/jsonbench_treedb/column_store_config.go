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

	columnStoreAggregateMetadataName = "min_time_us"
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
		return fmt.Errorf("-storage-layout %s requires a query projection (q1..q5), not full", cfg.StorageLayout)
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
	if layout != storageLayoutColumnStorePreparedMetadata {
		return false
	}
	switch query {
	case "q4", "q5":
		return true
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
		return "treedb-collections-column-store-full-prepared-scan-go"
	default:
		return "treedb-collections-direct-go"
	}
}

func runNotes(cfg runConfig) []string {
	if !isColumnStoreLayout(cfg.StorageLayout) {
		return nil
	}
	if isFullDataColumnStoreLayout(cfg.StorageLayout) {
		notes := []string{
			fmt.Sprintf("storage_layout=%s stores full source JSON by retaining non-declared fields and declaring JSONBench hot-path columns in TreeDB typed column parts.", cfg.StorageLayout),
			fmt.Sprintf("storage_layout=%s reconstructs documents from retained JSON plus typed columns; query timing uses physical column queries, not document reconstruction.", cfg.StorageLayout),
			fmt.Sprintf("storage_layout=%s forces TreeDB durable command-WAL mode because current column-store publication requires it.", cfg.StorageLayout),
			"full-data column-store cells do not use load-time sentinel masking; q2/q4/q5 predicates are evaluated as real physical predicates.",
		}
		if cfg.StorageLayout == storageLayoutColumnStoreFullPrepared {
			notes = append(notes, "column-store-full-prepared prepares physical query runners outside timed attempts and scans typed-column part sections.")
		}
		return notes
	}
	notes := []string{
		fmt.Sprintf("storage_layout=%s stores declared projection fields in TreeDB physical column row assets with retained_payload=none.", cfg.StorageLayout),
		fmt.Sprintf("storage_layout=%s forces TreeDB durable command-WAL mode because current column-store publication requires it.", cfg.StorageLayout),
		"q3/q4/q5 column-store cells use physical dictionary predicates when supported; q4/q5 aggregate-metadata cells still use load-time sentinel masking for metadata semantics; q2 remains sentinel-masked.",
	}
	if cfg.StorageLayout == storageLayoutColumnStorePrepared {
		notes = append(notes, "column-store-prepared prepares physical query runners outside timed attempts and scans base column rows; it does not declare aggregate metadata.")
	}
	if cfg.StorageLayout == storageLayoutColumnStorePreparedMetadata {
		notes = append(notes, "column-store-prepared-metadata prepares physical query runners outside timed attempts; only q4/q5 declare and use aggregate metadata named min_time_us.")
	}
	for _, q := range cfg.Queries {
		if q == "q3" {
			notes = append(notes, "q3 uses TreeDB's physical grouped-hour reducer over dictionary and int64 column sidecars.")
			break
		}
	}
	return notes
}

func columnStoreConfigForProjection(projection, storageLayout string) (*collections.ColumnStoreConfig, error) {
	fields, err := projectionFields(projection)
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
	}
	for _, field := range fields {
		col, err := columnStoreColumnForField(field, fullData)
		if err != nil {
			return nil, err
		}
		cfg.Columns = append(cfg.Columns, col)
		if field == "time_us" {
			cfg.SortKey = []collections.ColumnSortKey{{Column: "time_us"}}
		}
	}
	if columnStoreUsesAggregateMetadata(storageLayout, projection) {
		cfg.AggregateMetadata = []collections.ColumnAggregateMetadata{{
			Name:        columnStoreAggregateMetadataName,
			Column:      "time_us",
			GroupColumn: "did",
			Kind:        collections.ColumnAggregateMin,
		}}
	}
	return cfg, nil
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
	case "q4", "q5":
		if columnStoreUsesAggregateMetadata(storageLayout, projection) && (fields.Kind != "commit" || fields.Operation != "create" || fields.Event != "app.bsky.feed.post") {
			fields.DID = ""
		}
	}
}
