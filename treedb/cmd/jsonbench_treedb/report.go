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
	System        string    `json:"system"`
	Engine        string    `json:"engine,omitempty"`
	Scale         string    `json:"scale"`
	RequestedRows int       `json:"requested_rows,omitempty"`
	DatasetSize   int       `json:"dataset_size"`
	Format        string    `json:"format,omitempty"`
	Projection    string    `json:"projection,omitempty"`
	DataRoot      string    `json:"data_root,omitempty"`
	Query         string    `json:"query"`
	BestSec       float64   `json:"best_seconds"`
	MedianSec     float64   `json:"median_seconds"`
	AttemptsSec   []float64 `json:"attempts_seconds"`
	RowsScanned   int       `json:"rows_scanned,omitempty"`
	StorageBytes  int64     `json:"storage_bytes,omitempty"`
	LoadSec       float64   `json:"load_seconds,omitempty"`
	CompactionSec float64   `json:"compaction_seconds,omitempty"`
	Compacted     bool      `json:"compacted,omitempty"`
	RetainsJSON   *bool     `json:"retains_json_structure,omitempty"`
	Source        string    `json:"source"`
}

type jsonBenchBaselineResult struct {
	System             string      `json:"system"`
	DatasetSize        int         `json:"dataset_size"`
	NumLoadedDocuments int         `json:"num_loaded_documents"`
	TotalSize          int64       `json:"total_size"`
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
		for _, q := range result.Queries {
			rows = append(rows, reportRow{
				System:        result.System,
				Engine:        result.Engine,
				Scale:         reportScaleLabel(result),
				RequestedRows: result.RequestedRows,
				DatasetSize:   result.DatasetSize,
				Format:        result.Format,
				Projection:    result.Projection,
				DataRoot:      result.DataRoot,
				Query:         q.Name,
				BestSec:       q.BestSec,
				MedianSec:     q.MedianSec,
				AttemptsSec:   q.AttemptsSec,
				RowsScanned:   q.RowsScanned,
				StorageBytes:  result.Storage.TotalBytes,
				LoadSec:       result.Load.WallSec,
				CompactionSec: compactionSec,
				Compacted:     compactionEnabled,
				RetainsJSON:   &retainsJSON,
				Source:        path,
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
			rows = append(rows, reportRow{
				System:        result.System,
				Engine:        engine,
				Scale:         scale,
				RequestedRows: result.NumLoadedDocuments,
				DatasetSize:   result.DatasetSize,
				Format:        "json",
				Projection:    "full",
				Query:         name,
				BestSec:       best,
				MedianSec:     median,
				AttemptsSec:   attempts,
				RowsScanned:   result.NumLoadedDocuments,
				StorageBytes:  result.TotalSize,
				Source:        path,
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
	fmt.Fprintf(&buf, "| rows/scale | system | layout | query | best | median | attempts | requested | loaded | storage | load |\n")
	fmt.Fprintf(&buf, "|---|---|---|---:|---:|---:|---|---:|---:|---:|---:|\n")
	for _, row := range doc.Rows {
		fmt.Fprintf(
			&buf,
			"| %s | %s | %s | %s | %s | %s | %s | %s | %d | %s | %s |\n",
			row.Scale,
			row.System,
			reportRowLayout(row),
			row.Query,
			formatSeconds(row.BestSec),
			formatSeconds(row.MedianSec),
			formatAttempts(row.AttemptsSec),
			formatCount(row.RequestedRows),
			row.RowsScanned,
			formatBytes(row.StorageBytes),
			formatSeconds(row.LoadSec),
		)
	}
	fmt.Fprintf(&buf, "\n## Best Runtime By Query\n\n")
	best := bestByScaleQuery(doc.Rows)
	fmt.Fprintf(&buf, "| rows/scale | query | fastest system/layout | best | TreeDB best | DuckDB best | TreeDB / DuckDB |\n")
	fmt.Fprintf(&buf, "|---|---:|---|---:|---:|---:|---:|\n")
	keys := make([]string, 0, len(best))
	for key := range best {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		group := best[key]
		tree := group.treeBest()
		duck := group.duckBest()
		ratio := ""
		if tree != nil && duck != nil && duck.BestSec > 0 {
			ratio = fmt.Sprintf("%.2fx", tree.BestSec/duck.BestSec)
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
			"| %s | %s | %s | %s | %s | %s | %s |\n",
			group.Scale,
			group.Query,
			fastestLabel,
			fastestBest,
			formatOptionalRowSeconds(tree),
			formatOptionalRowSeconds(duck),
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
	if row.DataRoot != "" && row.DataRoot != "fast" {
		layout += "@" + row.DataRoot
	}
	if row.Compacted {
		layout += "+compacted"
	}
	return layout
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
