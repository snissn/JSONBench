package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunTreeDBBenchmarkRejectsMalformedJSONByDefault(t *testing.T) {
	dataDir := writeMalformedJSONBenchFixture(t)
	cfg := malformedJSONBenchRunConfig(t, dataDir)

	_, err := runTreeDBBenchmark(cfg)
	if err == nil {
		t.Fatal("runTreeDBBenchmark err=nil, want malformed JSON failure")
	}
	if !strings.Contains(err.Error(), "hash source JSON input row 2") {
		t.Fatalf("runTreeDBBenchmark err=%v, want source JSON input row failure", err)
	}
}

func TestRunTreeDBBenchmarkRejectsMalformedJSONByDefaultWithoutReconstructionValidation(t *testing.T) {
	dataDir := writeMalformedJSONBenchFixture(t)
	cfg := malformedJSONBenchRunConfig(t, dataDir)
	cfg.ValidateReconstruction = false

	_, err := runTreeDBBenchmark(cfg)
	if err == nil {
		t.Fatal("runTreeDBBenchmark err=nil, want malformed JSON failure")
	}
	if !strings.Contains(err.Error(), "invalid JSON document") {
		t.Fatalf("runTreeDBBenchmark err=%v, want invalid JSON document failure", err)
	}
}

func TestRunTreeDBBenchmarkAllowErrorsSkipsMalformedJSON(t *testing.T) {
	dataDir := writeMalformedJSONBenchFixture(t)
	cfg := malformedJSONBenchRunConfig(t, dataDir)
	cfg.AllowErrors = true

	result, err := runTreeDBBenchmark(cfg)
	if err != nil {
		t.Fatalf("runTreeDBBenchmark allow-errors: %v", err)
	}
	if got, want := result.RequestedRows, 3; got != want {
		t.Fatalf("requested_rows=%d want %d", got, want)
	}
	if got, want := result.DatasetSize, 2; got != want {
		t.Fatalf("dataset_size=%d want %d", got, want)
	}
	if got, want := result.Load.InputRows, 3; got != want {
		t.Fatalf("load.input_rows=%d want %d", got, want)
	}
	if got, want := result.Load.SkippedInvalidJSONRows, 1; got != want {
		t.Fatalf("load.skipped_invalid_json_rows=%d want %d", got, want)
	}
	if result.Reconstruction == nil || !result.Reconstruction.Valid || result.Reconstruction.Rows != 2 {
		t.Fatalf("reconstruction=%+v want valid rows=2", result.Reconstruction)
	}
	if got, want := len(result.Queries), 1; got != want {
		t.Fatalf("queries=%d want %d", got, want)
	}
}

func malformedJSONBenchRunConfig(t *testing.T, dataDir string) runConfig {
	t.Helper()
	return runConfig{
		DataDir:                dataDir,
		DBDir:                  t.TempDir(),
		Reset:                  true,
		Scale:                  "subset",
		Rows:                   3,
		MaxFiles:               1,
		Format:                 "json",
		StorageLayout:          storageLayoutColumnStoreFullPrepared,
		Projection:             "full",
		Queries:                []string{"q1"},
		BatchSize:              defaultBatchSize,
		Profile:                "fast",
		DataRoot:               "fast",
		Collection:             defaultCollectionName,
		Checkpoint:             true,
		ValidateReconstruction: true,
		Tries:                  1,
	}
}

func writeMalformedJSONBenchFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	body := strings.Join([]string{
		`{"did":"did:plc:alice","time_us":1700000000000000,"kind":"commit","commit":{"operation":"create","collection":"app.bsky.feed.post"}}`,
		`{"did":"did:plc:bad","time_us":1700000000000001,"kind":"commit","commit":{"operation":"create","collection":"app.bsky.feed.post"}`,
		`{"did":"did:plc:bob","time_us":1700000000000002,"kind":"identity","commit":{"operation":"create","collection":"app.bsky.graph.follow"}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(filepath.Join(dir, "file_0001.json"), []byte(body), 0o644); err != nil {
		t.Fatalf("write malformed fixture: %v", err)
	}
	return dir
}
