package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestPreferredScriptUsesFullDataStorageHeadline(t *testing.T) {
	if testing.Short() {
		t.Skip("preferred script smoke test shells out to go run")
	}
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skipf("bash not found: %v", err)
	}
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skipf("python3 not found: %v", err)
	}

	tmp := t.TempDir()
	outDir := filepath.Join(tmp, "preferred")
	clickHouseResult := filepath.Join(tmp, "clickhouse", "result.json")
	if err := os.MkdirAll(filepath.Dir(clickHouseResult), 0o755); err != nil {
		t.Fatalf("mkdir clickhouse stub: %v", err)
	}
	const clickHouseStub = `{"system":"ClickHouse","dataset_size":6,"num_loaded_documents":6,"total_size":1234,"data_size":1000,"index_size":234,"load_seconds":0.001,"result":[[0.001],[0.001],[0.001],[0.001],[0.001],[0.001]]}`
	if err := os.WriteFile(clickHouseResult, []byte(clickHouseStub), 0o644); err != nil {
		t.Fatalf("write clickhouse stub: %v", err)
	}

	cmd := exec.Command("bash", "./run_preferred_columnstore_clickhouse_compare.sh")
	cmd.Dir = filepath.Join("..", "..")
	cmd.Env = append(os.Environ(),
		"DATA_DIR=./testdata/bluesky",
		"ROWS=6",
		"TRIES=1",
		"RUN_CLICKHOUSE=0",
		"CLICKHOUSE_RESULT="+clickHouseResult,
		"QUERY_MODE=first_touch_after_open",
		"METADATA_MODE=no_aggregate_metadata",
		"TREEDB_VALIDATE_RECONSTRUCTION=1",
		"OUT_DIR="+outDir,
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("preferred script failed: %v\n%s", err, output)
	}

	summaryBytes, err := os.ReadFile(filepath.Join(outDir, "preferred_summary.md"))
	if err != nil {
		t.Fatalf("read preferred summary: %v", err)
	}
	summary := string(summaryBytes)
	for _, want := range []string{
		"## Full-data storage headline",
		"| TreeDB column-store-full-prepared | q1 |",
		"- data shape: `full-retained-json`",
		"- retained payload: `non-column`",
		"- typed owner: `typed_column_part`",
		"## Standard comparison detail",
		"| system/layout | query | rows loaded | load | insert | storage (TreeDB WAL-excl) | query mode | metadata mode | prepare/setup |",
		"| TreeDB column-store-full-prepared | q1 | 6 |",
		"first_touch_after_open",
		"no_aggregate_metadata",
		"row/doc materializations",
		"## Metadata cost accounting",
		"| query | aggregate metadata used | available metadata storage | metadata cost storage |",
		"## Query-shaped attribution rows",
		"| TreeDB column-store-full-prepared | qexpr |",
		"typed_row_asset | attribution only |",
		"## ClickHouse comparison mode",
		"raw_scan_jsonasobject_no_projection_no_materialized_summary",
		"projections/materialized summaries: `none configured by this wrapper`",
		"Query-shaped `column-store*` rows are attribution rows only",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("preferred summary missing %q\n%s", want, summary)
		}
	}

	reportBytes, err := os.ReadFile(filepath.Join(outDir, "treedb", "report.json"))
	if err != nil {
		t.Fatalf("read treedb report: %v", err)
	}
	var report reportDocument
	if err := json.Unmarshal(reportBytes, &report); err != nil {
		t.Fatalf("decode treedb report: %v", err)
	}
	var foundFullHeadline bool
	var foundQueryAttribution bool
	for _, row := range report.Rows {
		if row.StorageLayout == storageLayoutColumnStoreFullPrepared &&
			row.Query == "q1" &&
			row.QueryMode == queryModeFirstTouchAfterOpen &&
			row.MetadataMode == metadataModeNoAggregateMetadata &&
			row.DataShape == "full-retained-json" &&
			row.TypedColumnOwner == "typed_column_part" &&
			row.RetainedPayloadPolicy == "non-column" &&
			row.RetainedPayloadEncoding == "semantic-stream-v1" &&
			row.RetainedPayloadEncodingStatus == "active_semantic_stream_v1_non_column_retained_payload" &&
			row.ReconstructionValid != nil &&
			*row.ReconstructionValid {
			if row.InsertSec <= 0 {
				t.Fatalf("full headline row insert_seconds=%f want >0: %+v", row.InsertSec, row)
			}
			foundFullHeadline = true
		}
		if row.StorageLayout == storageLayoutColumnStorePreparedMetadata &&
			row.Query == "q4" &&
			row.QueryMode == queryModeFirstTouchAfterOpen &&
			row.MetadataMode == metadataModeNoAggregateMetadata &&
			row.DataShape == "query-shaped-projection" &&
			row.TypedColumnOwner == "typed_row_asset" {
			foundQueryAttribution = true
		}
	}
	if !foundFullHeadline {
		t.Fatalf("report missing full-data typed_column_part headline row: %+v", report.Rows)
	}
	if !foundQueryAttribution {
		t.Fatalf("report missing query-shaped attribution row: %+v", report.Rows)
	}
}
