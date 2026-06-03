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
	System                             string    `json:"system"`
	Engine                             string    `json:"engine,omitempty"`
	Scale                              string    `json:"scale"`
	RequestedRows                      int       `json:"requested_rows,omitempty"`
	DatasetSize                        int       `json:"dataset_size"`
	RowCount                           int       `json:"row_count"`
	Format                             string    `json:"format,omitempty"`
	StorageLayout                      string    `json:"storage_layout,omitempty"`
	Projection                         string    `json:"projection,omitempty"`
	Profile                            string    `json:"profile,omitempty"`
	DataRoot                           string    `json:"data_root,omitempty"`
	DataShape                          string    `json:"data_shape,omitempty"`
	ExecutionMode                      string    `json:"execution_mode,omitempty"`
	StorageSource                      string    `json:"storage_source,omitempty"`
	FallbackReason                     string    `json:"fallback_reason,omitempty"`
	MetadataDataScanPath               string    `json:"metadata_data_scan_path,omitempty"`
	SortLayout                         string    `json:"sort_layout,omitempty"`
	CompressionMode                    string    `json:"compression_mode,omitempty"`
	MutationMode                       string    `json:"mutation_mode,omitempty"`
	DocumentScanFallback               bool      `json:"document_scan_fallback"`
	RetainedPayloadPolicy              string    `json:"retained_payload_policy,omitempty"`
	ColumnReconstructionPolicy         string    `json:"column_reconstruction_policy,omitempty"`
	TypedColumnOwner                   string    `json:"typed_column_owner,omitempty"`
	ReconstructionStatus               string    `json:"reconstruction_status,omitempty"`
	Query                              string    `json:"query"`
	BestSec                            float64   `json:"best_seconds"`
	MedianSec                          float64   `json:"median_seconds"`
	AttemptsSec                        []float64 `json:"attempts_seconds"`
	RowsScanned                        int       `json:"rows_scanned,omitempty"`
	StorageBytes                       int64     `json:"storage_bytes,omitempty"`
	StorageGrossBytes                  int64     `json:"storage_gross_bytes,omitempty"`
	StorageExcludedBytes               int64     `json:"storage_excluded_bytes,omitempty"`
	StorageDurableBytesWALExcluded     int64     `json:"storage_durable_bytes_wal_excluded"`
	StorageWALBytesExcludedFromDurable int64     `json:"storage_wal_bytes_excluded_from_durable_storage"`
	StorageWALExcludedNote             string    `json:"storage_durable_bytes_wal_excluded_note,omitempty"`
	StorageColumnAssetBytes            int64     `json:"storage_column_asset_bytes,omitempty"`
	StorageTypedColumnPartBytes        int64     `json:"storage_typed_column_part_bytes,omitempty"`
	StorageTypedColumnSectionBytes     int64     `json:"storage_typed_column_section_bytes,omitempty"`
	StoragePrimaryIndexBytes           int64     `json:"storage_primary_index_bytes,omitempty"`
	StorageLeafVLogBytes               int64     `json:"storage_leaf_vlog_bytes,omitempty"`
	StorageWALBytes                    int64     `json:"storage_wal_bytes,omitempty"`
	BaselineDataBytes                  int64     `json:"baseline_data_bytes,omitempty"`
	BaselineIndexBytes                 int64     `json:"baseline_index_bytes,omitempty"`
	StorageAccountingScope             string    `json:"storage_accounting_scope,omitempty"`
	StorageMeasurementPhase            string    `json:"storage_measurement_phase,omitempty"`
	LoadSec                            float64   `json:"load_seconds,omitempty"`
	CompactionSec                      float64   `json:"compaction_seconds,omitempty"`
	Compacted                          bool      `json:"compacted,omitempty"`
	RetainsJSON                        *bool     `json:"retains_json_structure,omitempty"`
	ReconstructionValid                *bool     `json:"reconstruction_valid,omitempty"`
	Source                             string    `json:"source"`
}

type jsonBenchBaselineResult struct {
	System             string      `json:"system"`
	DatasetSize        int         `json:"dataset_size"`
	NumLoadedDocuments int         `json:"num_loaded_documents"`
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
		if result.Storage.ColumnStorePhysical != nil {
			typedColumnPartBytes = result.Storage.ColumnStorePhysical.Totals.TypedColumnPartBytes
			typedColumnSectionBytes = result.Storage.ColumnStorePhysical.Totals.TypedColumnSections.TotalStoredBytes
		}
		for _, q := range result.Queries {
			rows = append(rows, reportRow{
				System:                             result.System,
				Engine:                             result.Engine,
				Scale:                              reportScaleLabel(result),
				RequestedRows:                      result.RequestedRows,
				DatasetSize:                        result.DatasetSize,
				RowCount:                           result.DatasetSize,
				Format:                             result.Format,
				StorageLayout:                      result.StorageLayout,
				Projection:                         result.Projection,
				Profile:                            result.Profile,
				DataRoot:                           result.DataRoot,
				DataShape:                          result.DataShape,
				ExecutionMode:                      reportRowExecutionMode(result.StorageLayout),
				StorageSource:                      reportRowStorageSource(result.StorageLayout),
				FallbackReason:                     reportRowFallbackReason(result.StorageLayout),
				MetadataDataScanPath:               reportRowMetadataDataScanPath(result.StorageLayout, q.Name),
				SortLayout:                         reportRowSortLayout(result.StorageLayout, result.Projection),
				CompressionMode:                    reportRowCompressionMode(result),
				MutationMode:                       reportRowMutationMode(result),
				DocumentScanFallback:               false,
				RetainedPayloadPolicy:              result.RetainedPayloadPolicy,
				ColumnReconstructionPolicy:         result.ColumnReconstructionPolicy,
				TypedColumnOwner:                   result.TypedColumnOwner,
				ReconstructionStatus:               reportRowReconstructionStatus(result),
				Query:                              q.Name,
				BestSec:                            q.BestSec,
				MedianSec:                          q.MedianSec,
				AttemptsSec:                        q.AttemptsSec,
				RowsScanned:                        q.RowsScanned,
				StorageBytes:                       result.Storage.TotalBytes,
				StorageGrossBytes:                  result.Storage.GrossBytes,
				StorageExcludedBytes:               result.Storage.ExcludedBytes,
				StorageDurableBytesWALExcluded:     result.Storage.DurableStorageBytesWALExcluded,
				StorageWALBytesExcludedFromDurable: result.Storage.WALBytesExcludedFromDurable,
				StorageWALExcludedNote:             result.Storage.DurableStorageBytesWALExcludedNote,
				StorageColumnAssetBytes:            columnAssetBytes,
				StorageTypedColumnPartBytes:        typedColumnPartBytes,
				StorageTypedColumnSectionBytes:     typedColumnSectionBytes,
				StoragePrimaryIndexBytes:           storageCategoryBytes(result.Storage, "primary_index"),
				StorageLeafVLogBytes:               storageCategoryBytes(result.Storage, "leaf_vlog"),
				StorageWALBytes:                    storageCategoryBytes(result.Storage, "wal"),
				StorageAccountingScope:             result.Storage.AccountingScope,
				StorageMeasurementPhase:            result.Storage.MeasurementPhase,
				LoadSec:                            result.Load.WallSec,
				CompactionSec:                      compactionSec,
				Compacted:                          compactionEnabled,
				RetainsJSON:                        &retainsJSON,
				ReconstructionValid:                reconstructionValid,
				Source:                             path,
			})
		}
		return nil
	})
	return rows, err
}

func reportScaleLabel(result runResult) string {
	if result.RequestedRows > 0 && result.DatasetSize > 0 && result.RequestedRows != result.DatasetSize {
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

func reportRowExecutionMode(layout string) string {
	switch {
	case isPreparedColumnStoreLayout(layout):
		return "prepared"
	case isColumnStoreLayout(layout):
		return "direct"
	case layout == storageLayoutRow:
		return "row_scan"
	default:
		return "unknown"
	}
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

func reportRowMetadataDataScanPath(layout, query string) string {
	switch {
	case columnStoreUsesAggregateMetadata(layout, query):
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
	case "q3", "q4", "q5", "minimal":
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
		scale := scaleFromDatasetSize(result.DatasetSize)
		if !scaleAllowed(scales, scale) {
			continue
		}
		for i, attempts := range result.Result {
			name := "q" + strconv.Itoa(i+1)
			best, median := bestMedian(attempts)
			retainsJSON := true
			rows = append(rows, reportRow{
				System:                  result.System,
				Engine:                  engine,
				Scale:                   scale,
				RequestedRows:           result.NumLoadedDocuments,
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
	fmt.Fprintf(&buf, "| rows/scale | layout | query | profile | mode | source | fallback | path | sort | compression | mutation | retained payload | typed owner | rows | reconstruction | WAL-excluded durable | WAL excluded |\n")
	fmt.Fprintf(&buf, "|---|---|---:|---|---|---|---|---|---|---|---|---|---|---:|---|---:|---:|\n")
	for _, row := range doc.Rows {
		if row.System != "TreeDB" {
			continue
		}
		fmt.Fprintf(
			&buf,
			"| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n",
			row.Scale,
			reportRowLayout(row),
			row.Query,
			row.Profile,
			row.ExecutionMode,
			row.StorageSource,
			row.FallbackReason,
			row.MetadataDataScanPath,
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
