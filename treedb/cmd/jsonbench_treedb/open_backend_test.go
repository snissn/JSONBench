package main

import (
	"path/filepath"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestOpenBackendColumnStorePersistsFullCommandWALFormat(t *testing.T) {
	dir := t.TempDir()
	backend, cleanup, err := openBackend(runConfig{
		DBDir:         dir,
		Profile:       "durable",
		StorageLayout: storageLayoutColumnStoreFullPrepared,
		DataRoot:      "fast",
	})
	if err != nil {
		t.Fatalf("openBackend: %v", err)
	}
	if backend == nil {
		t.Fatalf("openBackend returned nil backend")
	}
	if err := cleanup(); err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	cfg, ok, err := backenddb.LoadFormatConfig(filepath.Join(dir, "maindb"))
	if err != nil {
		t.Fatalf("LoadFormatConfig: %v", err)
	}
	if !ok {
		t.Fatalf("format config not written")
	}
	if !cfg.RequiresCommandWALV1() {
		t.Fatalf("format config missing command WAL requirement: %+v", cfg)
	}
	if !cfg.IndexOuterLeavesInValueLog {
		t.Fatalf("IndexOuterLeavesInValueLog=false, want true so larger full-data column-store roots keep leaf_vlog pointers readable")
	}
	if cfg.IndexInternalBaseDelta {
		t.Fatalf("IndexInternalBaseDelta=true, want false with outer leaf-log refs")
	}
}
