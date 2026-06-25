package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReportScaleLabelUsesRequestedInputRowsForAllowErrors(t *testing.T) {
	result := runResult{
		Scale:         "10m",
		ScaleLabel:    "9999994 of 10000000 requested rows",
		RequestedRows: 10_000_000,
		DatasetSize:   9_999_994,
		Load: loadResult{
			InputRows:              10_000_000,
			SkippedInvalidJSONRows: 6,
		},
	}
	if got, want := reportScaleLabel(result), "10m"; got != want {
		t.Fatalf("reportScaleLabel allow-errors=%q want %q", got, want)
	}

	partial := result
	partial.Load.InputRows = 9_999_994
	if got, want := reportScaleLabel(partial), "9999994 of 10000000 requested rows"; got != want {
		t.Fatalf("reportScaleLabel partial=%q want %q", got, want)
	}
}

func TestCollectBaselineRowsUsesRequestedRowsForScale(t *testing.T) {
	dir := t.TempDir()
	resultPath := filepath.Join(dir, "result.json")
	const clickHouseResult = `{
  "system": "ClickHouse",
  "dataset_size": 9999994,
  "num_loaded_documents": 9999994,
  "requested_rows": 10000000,
  "total_size": 1234,
  "data_size": 1000,
  "index_size": 234,
  "result": [[0.014]]
}`
	if err := os.WriteFile(resultPath, []byte(clickHouseResult), 0o644); err != nil {
		t.Fatalf("write ClickHouse result: %v", err)
	}

	rows, err := collectBaselineRows(dir, nil, "ClickHouse", "json-column-sql")
	if err != nil {
		t.Fatalf("collectBaselineRows: %v", err)
	}
	if got, want := len(rows), 1; got != want {
		t.Fatalf("rows=%d want %d", got, want)
	}
	if got, want := rows[0].Scale, "10m"; got != want {
		t.Fatalf("scale=%q want %q", got, want)
	}
	if got, want := rows[0].RequestedRows, 10_000_000; got != want {
		t.Fatalf("requested_rows=%d want %d", got, want)
	}
	if got, want := rows[0].DatasetSize, 9_999_994; got != want {
		t.Fatalf("dataset_size=%d want %d", got, want)
	}
}

func TestCollectBaselineRowsPreservesLegacyDatasetScale(t *testing.T) {
	dir := t.TempDir()
	resultPath := filepath.Join(dir, "result.json")
	const clickHouseResult = `{
  "system": "ClickHouse",
  "dataset_size": 10000000,
  "num_loaded_documents": 9999994,
  "total_size": 1234,
  "data_size": 1000,
  "index_size": 234,
  "result": [[0.014]]
}`
	if err := os.WriteFile(resultPath, []byte(clickHouseResult), 0o644); err != nil {
		t.Fatalf("write legacy ClickHouse result: %v", err)
	}

	rows, err := collectBaselineRows(dir, map[string]struct{}{"10m": {}}, "ClickHouse", "json-column-sql")
	if err != nil {
		t.Fatalf("collectBaselineRows: %v", err)
	}
	if got, want := len(rows), 1; got != want {
		t.Fatalf("rows=%d want %d", got, want)
	}
	if got, want := rows[0].Scale, "10m"; got != want {
		t.Fatalf("scale=%q want %q", got, want)
	}
	if got, want := rows[0].RequestedRows, 9_999_994; got != want {
		t.Fatalf("requested_rows=%d want legacy loaded count %d", got, want)
	}
	if got, want := rows[0].DatasetSize, 10_000_000; got != want {
		t.Fatalf("dataset_size=%d want %d", got, want)
	}
}
