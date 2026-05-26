package main

import (
	"fmt"
	"strings"

	"github.com/snissn/gomap/TreeDB/collections"
)

const (
	storageLayoutRow                         = "row"
	storageLayoutColumnStore                 = "column-store"
	storageLayoutColumnStorePreparedMetadata = "column-store-prepared-metadata"

	columnStoreAggregateMetadataName = "min_time_us"
)

func normalizeStorageLayout(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", storageLayoutRow, "rows", "row-store", "rowstore":
		return storageLayoutRow, nil
	case storageLayoutColumnStore, "column", "columns", "column_store", "columnstore":
		return storageLayoutColumnStore, nil
	// Keep the dashed constants above as canonical output names; underscore and
	// shorter aggmeta aliases are accepted only for interactive shell convenience.
	case storageLayoutColumnStorePreparedMetadata, "column_store_prepared_metadata", "column-store-prepared-aggmeta", "column_store_prepared_aggmeta", "column-store-aggmeta", "column_store_aggmeta":
		return storageLayoutColumnStorePreparedMetadata, nil
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
	case storageLayoutColumnStore, storageLayoutColumnStorePreparedMetadata:
		return true
	default:
		return false
	}
}

func isPreparedColumnStoreLayout(layout string) bool {
	return layout == storageLayoutColumnStorePreparedMetadata
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
	case storageLayoutColumnStorePreparedMetadata:
		return "treedb-collections-column-store-prepared-metadata-direct-go"
	default:
		return "treedb-collections-direct-go"
	}
}

func runNotes(cfg runConfig) []string {
	if !isColumnStoreLayout(cfg.StorageLayout) {
		return nil
	}
	notes := []string{
		fmt.Sprintf("storage_layout=%s stores declared projection fields in TreeDB physical column row assets with retained_payload=none.", cfg.StorageLayout),
		fmt.Sprintf("storage_layout=%s forces TreeDB durable command-WAL mode because current column-store publication requires it.", cfg.StorageLayout),
		"q2/q4/q5 column-store cells use query-specific sentinel masking during load because the current physical column reducers do not expose separate filter predicates.",
	}
	if isPreparedColumnStoreLayout(cfg.StorageLayout) {
		notes = append(notes, "column-store-prepared-metadata prepares physical query runners outside timed attempts; q4/q5 declare and use aggregate metadata named min_time_us.")
	}
	for _, q := range cfg.Queries {
		if q == "q3" {
			notes = append(notes, "q3 uses the existing JSON materialization scan over the column-store fixture; the current physical column reducers do not expose an event+hour grouped reducer.")
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
	if len(fields) == 0 {
		return nil, fmt.Errorf("column-store layout requires a query projection, not %q", projection)
	}
	cfg := &collections.ColumnStoreConfig{
		Enabled:         true,
		RetainedPayload: collections.ColumnRetainedPayloadNone,
		ProfileSupport:  collections.ColumnStoreProfileBenchmarkRelaxed,
		Columns:         make([]collections.ColumnStoreColumn, 0, len(fields)),
	}
	for _, field := range fields {
		col, err := columnStoreColumnForField(field)
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

func columnStoreColumnForField(field string) (collections.ColumnStoreColumn, error) {
	col := collections.ColumnStoreColumn{
		Name:  field,
		Path:  field,
		Owner: collections.TypedStorageOwnerRowAsset,
	}
	switch field {
	case "event", "did", "kind", "operation":
		col.ValueType = collections.ColumnStoreValueString
		col.Dictionary = true
		return col, nil
	case "time_us":
		col.ValueType = collections.ColumnStoreValueInt64
		return col, nil
	default:
		return collections.ColumnStoreColumn{}, fmt.Errorf("unsupported column-store projection field %q", field)
	}
}

func applyColumnStoreQueryMask(fields *extractedFields, projection string) {
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
		if fields.Kind != "commit" || fields.Operation != "create" || fields.Event != "app.bsky.feed.post" {
			fields.DID = ""
		}
	}
}
