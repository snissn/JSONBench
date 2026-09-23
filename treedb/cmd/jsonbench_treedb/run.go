package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash"
	"io"
	"os"
	stdpath "path"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/tidwall/gjson"
)

const (
	defaultCollectionName = "bluesky"
	defaultBatchSize      = 16000

	envColumnStoreRetainedPayloadEncoding = "TREEDB_COLUMN_STORE_RETAINED_PAYLOAD_ENCODING"
)

type runConfig struct {
	DataDir                 string
	DBDir                   string
	Out                     string
	Reset                   bool
	Scale                   string
	Rows                    int
	MaxFiles                int
	Format                  string
	StorageLayout           string
	QueryMode               string
	MetadataMode            string
	RetainedPayloadEncoding string
	Projection              string
	Queries                 []string
	BatchSize               int
	LoadPipelineDepth       int
	EnginePrepareDepth      int
	EnginePrepareMaxBytes   int64
	Profile                 string
	QueryProfileDir         string
	DataRoot                string
	Collection              string
	Checkpoint              bool
	CompactAfterLoad        bool
	CompactBatchSize        int
	ValidateReconstruction  bool
	AllowErrors             bool
	Tries                   int
	Progress                bool
}

type runResult struct {
	SchemaVersion string `json:"schema_version"`
	GeneratedAt   string `json:"generated_at"`
	System        string `json:"system"`
	Engine        string `json:"engine"`
	Scale         string `json:"scale"`
	ScaleLabel    string `json:"scale_label"`
	RequestedRows int    `json:"requested_rows"`
	// Kept for parsing old result JSON; new runs always reject partial input.
	AllowShortData                bool                                          `json:"allow_short_data,omitempty"`
	DatasetSize                   int                                           `json:"dataset_size"`
	DataDir                       string                                        `json:"data_dir"`
	DBDir                         string                                        `json:"db_dir"`
	Collection                    string                                        `json:"collection"`
	Format                        string                                        `json:"format"`
	StorageLayout                 string                                        `json:"storage_layout"`
	QueryMode                     string                                        `json:"query_mode"`
	MetadataMode                  string                                        `json:"metadata_mode"`
	Projection                    string                                        `json:"projection"`
	RetainsJSON                   bool                                          `json:"retains_json_structure"`
	DataShape                     string                                        `json:"data_shape,omitempty"`
	RetainedPayloadPolicy         string                                        `json:"retained_payload_policy,omitempty"`
	RetainedPayloadEncoding       string                                        `json:"retained_payload_encoding,omitempty"`
	RetainedPayloadEncodingStatus string                                        `json:"retained_payload_encoding_status,omitempty"`
	ColumnReconstructionPolicy    string                                        `json:"column_reconstruction_policy,omitempty"`
	TypedColumnOwner              string                                        `json:"typed_column_owner,omitempty"`
	Profile                       string                                        `json:"profile"`
	QueryProfileDir               string                                        `json:"query_profile_dir,omitempty"`
	DataRoot                      string                                        `json:"data_root"`
	Load                          loadResult                                    `json:"load"`
	Storage                       storageResult                                 `json:"storage"`
	Compaction                    *compactionResult                             `json:"compaction,omitempty"`
	Reconstruction                *reconstructionResult                         `json:"reconstruction,omitempty"`
	QueryReadyPreparation         *collections.QueryReadyColumnPreparationStats `json:"query_ready_preparation,omitempty"`
	Queries                       []queryRun                                    `json:"queries"`
	Command                       []string                                      `json:"command,omitempty"`
	Notes                         []string                                      `json:"notes,omitempty"`
}

type loadResult struct {
	Rows                               int                `json:"rows"`
	InputRows                          int                `json:"input_rows,omitempty"`
	SkippedInvalidJSONRows             int                `json:"skipped_invalid_json_rows,omitempty"`
	Files                              []string           `json:"files"`
	Batches                            int                `json:"batches"`
	GenerationSec                      float64            `json:"generation_seconds"`
	InsertSec                          float64            `json:"insert_seconds"`
	PipelineDepth                      int                `json:"pipeline_depth"`
	EnginePrepareDepth                 int                `json:"engine_prepare_depth"`
	EnginePreparePath                  string             `json:"engine_prepare_path"`
	EngineFallbackReason               string             `json:"engine_fallback_reason,omitempty"`
	EngineFallbackBatches              int                `json:"engine_fallback_batches"`
	EnginePreparedBatches              int                `json:"engine_prepared_batches"`
	EngineCommittedBatches             int                `json:"engine_committed_batches"`
	EngineAbandonedBatches             int                `json:"engine_abandoned_batches"`
	EnginePrepareSec                   float64            `json:"engine_prepare_seconds"`
	EngineCommitSec                    float64            `json:"engine_commit_seconds"`
	EngineOverlapSec                   float64            `json:"engine_prepare_commit_overlap_seconds"`
	EnginePeakOwnedBytes               int64              `json:"engine_peak_owned_bytes"`
	EnginePeakReservedBytes            int64              `json:"engine_peak_reserved_bytes"`
	EngineIdleScratchReserveBytes      int64              `json:"engine_idle_scratch_reserve_bytes"`
	EngineMaxTokenReservedBytes        int64              `json:"engine_max_token_reserved_bytes"`
	EngineBudgetRetryBatches           int                `json:"engine_budget_retry_batches"`
	EngineFirstBudgetRetryReason       string             `json:"engine_first_budget_retry_reason,omitempty"`
	EnginePeakOwnedBatches             int                `json:"engine_peak_owned_batches"`
	ProducerElapsedSec                 float64            `json:"producer_elapsed_seconds"`
	ProducerWorkSec                    float64            `json:"producer_work_seconds"`
	ProducerWaitSec                    float64            `json:"producer_wait_seconds"`
	ProducerCreditWaitSec              float64            `json:"producer_credit_wait_seconds"`
	ProducerSourceCreditWaitSec        float64            `json:"producer_source_credit_wait_seconds"`
	ProducerPrepareCreditWaitSec       float64            `json:"producer_prepare_credit_wait_seconds"`
	ProducerRetryWaitSec               float64            `json:"producer_retry_wait_seconds"`
	ProducerHandoffWaitSec             float64            `json:"producer_handoff_wait_seconds"`
	ConsumerWaitSec                    float64            `json:"consumer_wait_seconds"`
	OverlapSec                         float64            `json:"overlap_seconds"`
	InputOverlapSec                    float64            `json:"input_overlap_seconds"`
	MaxQueuedBatches                   int                `json:"max_queued_batches"`
	MaxBatchBytes                      int64              `json:"max_batch_bytes"`
	MaxInFlightBytesBound              int64              `json:"max_in_flight_bytes_bound"`
	AllocatedBytes                     uint64             `json:"allocated_bytes"`
	Allocations                        uint64             `json:"allocations"`
	AllocatedBytesPerRow               float64            `json:"allocated_bytes_per_row"`
	AllocationsPerRow                  float64            `json:"allocations_per_row"`
	AggregateMetadataPrepareSec        float64            `json:"aggregate_metadata_prepare_seconds,omitempty"`
	AggregateMetadataAppendShareSec    float64            `json:"aggregate_metadata_append_share_seconds,omitempty"`
	AggregateMetadataInsertCostSec     float64            `json:"aggregate_metadata_insert_cost_seconds,omitempty"`
	AggregateMetadataInsertCostBasis   string             `json:"aggregate_metadata_insert_cost_basis,omitempty"`
	AggregateMetadataBytes             int64              `json:"aggregate_metadata_bytes,omitempty"`
	AggregateMetadataSharedAppendBytes int64              `json:"aggregate_metadata_shared_append_bytes,omitempty"`
	InsertStats                        *insertStatsResult `json:"insert_stats,omitempty"`
	FlushSec                           float64            `json:"flush_seconds"`
	CheckpointSec                      float64            `json:"checkpoint_seconds,omitempty"`
	WallSec                            float64            `json:"wall_seconds"`
	RowsPerSec                         float64            `json:"rows_per_second"`
	BytesRead                          int64              `json:"bytes_read"`
	CompressedBytes                    int64              `json:"compressed_bytes"`
	SourceCanonicalJSONHash            string             `json:"source_canonical_json_hash,omitempty"`
}

type storageResult struct {
	TotalBytes                         int64                                      `json:"total_bytes"`
	GrossBytes                         int64                                      `json:"gross_bytes,omitempty"`
	ExcludedBytes                      int64                                      `json:"excluded_bytes,omitempty"`
	BytesPerRow                        float64                                    `json:"bytes_per_row,omitempty"`
	FileCount                          int                                        `json:"file_count"`
	ExcludedFileCount                  int                                        `json:"excluded_file_count,omitempty"`
	WALBytesExcludedFromDurable        int64                                      `json:"wal_bytes_excluded_from_durable_storage"`
	DurableStorageBytesWALExcluded     int64                                      `json:"durable_storage_bytes_wal_excluded"`
	DurableStorageBytesWALExcludedNote string                                     `json:"durable_storage_bytes_wal_excluded_note,omitempty"`
	AccountingScope                    string                                     `json:"accounting_scope,omitempty"`
	MeasurementPhase                   string                                     `json:"measurement_phase,omitempty"`
	Categories                         []storageCategoryResult                    `json:"categories,omitempty"`
	ColumnStorePhysical                *collections.ColumnStorePhysicalAccounting `json:"column_store_physical,omitempty"`
	ColumnAssetReachability            *collections.ColumnAssetReachabilityPlan   `json:"column_asset_reachability,omitempty"`
}

type storageCategoryResult struct {
	Category    string  `json:"category"`
	Bytes       int64   `json:"bytes"`
	BytesPerRow float64 `json:"bytes_per_row,omitempty"`
	FileCount   int     `json:"file_count"`
	Included    bool    `json:"included"`
}

type compactionResult struct {
	Enabled               bool                                             `json:"enabled"`
	WallSec               float64                                          `json:"wall_seconds,omitempty"`
	StorageBefore         storageResult                                    `json:"storage_before"`
	StorageAfter          storageResult                                    `json:"storage_after"`
	CompactStorageStats   backenddb.CompactStorageStats                    `json:"compact_storage_stats"`
	RootOverlaySec        float64                                          `json:"root_overlay_seconds,omitempty"`
	RootOverlayStats      collections.CollectionRootOverlayCompactionStats `json:"root_overlay_stats"`
	CheckpointSec         float64                                          `json:"checkpoint_seconds,omitempty"`
	ValueLogRewriteSec    float64                                          `json:"value_log_rewrite_seconds,omitempty"`
	ValueLogRewriteStats  backenddb.ValueLogRewriteStats                   `json:"value_log_rewrite_stats"`
	ValueLogGCSec         float64                                          `json:"value_log_gc_seconds,omitempty"`
	ValueLogGCStats       backenddb.ValueLogGCStats                        `json:"value_log_gc_stats"`
	IndexVacuumSec        float64                                          `json:"index_vacuum_seconds,omitempty"`
	LeafGenerationGCSec   float64                                          `json:"leaf_generation_gc_seconds,omitempty"`
	LeafGenerationGCStats backenddb.LeafGenerationGCStats                  `json:"leaf_generation_gc_stats"`
}

type reconstructionResult struct {
	Enabled                 bool                     `json:"enabled"`
	Rows                    int                      `json:"rows"`
	Mode                    string                   `json:"mode"`
	SourceCanonicalJSONHash string                   `json:"source_canonical_json_hash,omitempty"`
	StoredCanonicalJSONHash string                   `json:"stored_canonical_json_hash,omitempty"`
	Valid                   bool                     `json:"valid"`
	Mismatch                string                   `json:"mismatch,omitempty"`
	ScanStats               *documentScanStatsResult `json:"scan_stats,omitempty"`
}

type documentScanStatsResult struct {
	CertifiedMonotonicPath    bool   `json:"certified_monotonic_path"`
	GenericFallback           bool   `json:"generic_fallback"`
	PhysicalPasses            uint64 `json:"physical_passes,omitempty"`
	PhysicalRows              uint64 `json:"physical_rows,omitempty"`
	PhysicalBytes             uint64 `json:"physical_bytes,omitempty"`
	PhysicalDecodedBlocks     uint64 `json:"physical_decoded_blocks,omitempty"`
	LocatorLookupBatches      uint64 `json:"locator_lookup_batches,omitempty"`
	LocatorLookups            uint64 `json:"locator_lookups,omitempty"`
	PointRowFetches           uint64 `json:"point_row_fetches,omitempty"`
	ReconstructedRows         uint64 `json:"reconstructed_rows,omitempty"`
	MaxRecordWindow           uint64 `json:"max_record_window,omitempty"`
	MaxVisibleRowWindow       uint64 `json:"max_visible_row_window,omitempty"`
	MaxTypedGenerations       uint64 `json:"max_typed_generations,omitempty"`
	MaxTypedDecodedBytes      uint64 `json:"max_typed_decoded_bytes,omitempty"`
	MaxTypedSourcePartBytes   uint64 `json:"max_typed_source_part_bytes,omitempty"`
	MaxRetainedBlocks         uint64 `json:"max_retained_blocks,omitempty"`
	PreflightProjectedColumns uint64 `json:"preflight_projected_columns,omitempty"`
}

type queryRun struct {
	Name         string                `json:"name"`
	SQL          string                `json:"sql"`
	QueryMode    string                `json:"query_mode"`
	MetadataMode string                `json:"metadata_mode"`
	AttemptsSec  []float64             `json:"attempts_seconds"`
	BestSec      float64               `json:"best_seconds"`
	MedianSec    float64               `json:"median_seconds"`
	RowsScanned  int                   `json:"rows_scanned"`
	ResultRows   int                   `json:"result_rows"`
	ResultHash   string                `json:"result_hash"`
	Preview      []queryRow            `json:"preview,omitempty"`
	Diagnostics  queryDiagnostics      `json:"diagnostics,omitempty"`
	Profiles     []queryAttemptProfile `json:"attempt_profiles,omitempty"`
}

type queryRow map[string]any

func parseRunFlags(args []string) (runConfig, error) {
	cfg := runConfig{
		DataDir:               "~/data/bluesky",
		Scale:                 "subset",
		Format:                "json",
		StorageLayout:         storageLayoutRow,
		QueryMode:             queryModeOneShotEndToEnd,
		MetadataMode:          metadataModeAutoAggregateMetadata,
		Projection:            "full",
		BatchSize:             defaultBatchSize,
		LoadPipelineDepth:     1,
		EnginePrepareDepth:    1,
		EnginePrepareMaxBytes: 9 << 29,
		Profile:               "fast",
		DataRoot:              "fast",
		Collection:            defaultCollectionName,
		Checkpoint:            true,
		CompactBatchSize:      defaultBatchSize,
		Tries:                 1,
		Queries:               append([]string(nil), jsonBenchQueryNames...),
	}
	var queryList string
	var deprecatedAllowShortData bool
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	flagUsage(fs, "Run one TreeDB JSONBench collection cell.")
	fs.StringVar(&cfg.DataDir, "data-dir", cfg.DataDir, "Directory containing JSONBench file_*.json.gz or .json files")
	fs.StringVar(&cfg.DBDir, "db-dir", "", "TreeDB directory for this cell; defaults to a temp directory")
	fs.StringVar(&cfg.Out, "out", "", "Result JSON path; stdout when empty")
	fs.BoolVar(&cfg.Reset, "reset", false, "Remove -db-dir before loading")
	fs.StringVar(&cfg.Scale, "scale", cfg.Scale, "Scale label: subset, 1m, 10m, 100m, 1000m")
	fs.IntVar(&cfg.Rows, "rows", 0, "Maximum rows to load; defaults from -scale")
	fs.IntVar(&cfg.MaxFiles, "max-files", 0, "Maximum input files to read; defaults from -scale")
	fs.StringVar(&cfg.Format, "format", cfg.Format, "TreeDB collection format: json or template-v1")
	fs.StringVar(&cfg.StorageLayout, "storage-layout", cfg.StorageLayout, "TreeDB storage layout: row, column-store, column-store-prepared, or column-store-prepared-metadata")
	fs.StringVar(&cfg.QueryMode, "query-mode", cfg.QueryMode, "Query timing mode: one_shot_end_to_end, first_touch_after_open, or hot_prepared_run")
	fs.StringVar(&cfg.MetadataMode, "metadata-mode", cfg.MetadataMode, "Column-store metadata mode: auto_aggregate_metadata or no_aggregate_metadata")
	fs.StringVar(&cfg.RetainedPayloadEncoding, "column-store-retained-payload-encoding", os.Getenv(envColumnStoreRetainedPayloadEncoding), "Retained-payload encoding override for full-data column-store non-column retained payloads (default/template-v1,json,semantic-stream-v1). Can also be set with "+envColumnStoreRetainedPayloadEncoding)
	fs.StringVar(&cfg.Projection, "projection", cfg.Projection, "Projection: full, minimal, q1, q2, q3, q4, q4a, q4b, q5, qexpr")
	fs.StringVar(&queryList, "queries", "all", "Comma-separated query names: all, q1, q2, q3, q4, q4a, q4b, q5, qexpr")
	fs.IntVar(&cfg.BatchSize, "batch-size", cfg.BatchSize, "Documents per InsertBatch")
	fs.IntVar(&cfg.LoadPipelineDepth, "load-pipeline-depth", cfg.LoadPipelineDepth, "Prepared load batches queued ahead of InsertBatch; 0 preserves serial loading")
	fs.IntVar(&cfg.EnginePrepareDepth, "engine-prepare-depth", cfg.EnginePrepareDepth, "Engine batches prepared ahead of ordered commit (0 or 1)")
	fs.Int64Var(&cfg.EnginePrepareMaxBytes, "engine-prepare-max-bytes", cfg.EnginePrepareMaxBytes, "Total in-flight source, prepared batch, and ordered-commit reservation bytes")
	fs.StringVar(&cfg.Profile, "profile", cfg.Profile, "TreeDB profile: fast, wal_on_fast, durable, bench")
	fs.StringVar(&cfg.QueryProfileDir, "query-profile-dir", "", "Directory for per-query timed-attempt CPU and allocs pprof artifacts; disabled when empty")
	fs.StringVar(&cfg.DataRoot, "data-root", cfg.DataRoot, "Collection data root storage: fast or compressed")
	fs.StringVar(&cfg.Collection, "collection", cfg.Collection, "Collection name")
	fs.BoolVar(&cfg.Checkpoint, "checkpoint", cfg.Checkpoint, "Checkpoint after loading")
	fs.BoolVar(&cfg.CompactAfterLoad, "compact-after-load", false, "Run full TreeDB maintenance compaction after loading and before query timing")
	fs.IntVar(&cfg.CompactBatchSize, "compact-batch-size", cfg.CompactBatchSize, "Value-log rewrite pointer-swap batch size for -compact-after-load")
	fs.BoolVar(&cfg.ValidateReconstruction, "validate-reconstruction", false, "Validate full-data column-store reconstruction by hashing source JSON against materialized stored JSON")
	fs.BoolVar(&cfg.AllowErrors, "allow-errors", false, "Skip malformed JSON input rows; intended for apples-to-apples comparison with ClickHouse allow-errors loads")
	fs.IntVar(&cfg.Tries, "tries", cfg.Tries, "Query attempts per query")
	fs.BoolVar(&cfg.Progress, "progress", false, "Print load progress to stderr")
	fs.BoolVar(&deprecatedAllowShortData, "allow-short-data", false, "removed; partial input is not accepted")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if deprecatedAllowShortData {
		return cfg, errors.New("-allow-short-data has been removed; point -data-dir at enough data or lower -rows")
	}
	queries, err := parseQueryList(queryList)
	if err != nil {
		return cfg, err
	}
	cfg.Queries = queries
	cfg.Format = strings.ToLower(strings.TrimSpace(cfg.Format))
	cfg.StorageLayout, err = normalizeStorageLayout(cfg.StorageLayout)
	if err != nil {
		return cfg, err
	}
	cfg.QueryMode, err = normalizeQueryMode(cfg.QueryMode)
	if err != nil {
		return cfg, err
	}
	cfg.MetadataMode, err = normalizeMetadataMode(cfg.MetadataMode)
	if err != nil {
		return cfg, err
	}
	cfg.RetainedPayloadEncoding = strings.ToLower(strings.TrimSpace(cfg.RetainedPayloadEncoding))
	cfg.Projection = strings.ToLower(strings.TrimSpace(cfg.Projection))
	cfg.Profile = strings.ToLower(strings.TrimSpace(cfg.Profile))
	cfg.QueryProfileDir = strings.TrimSpace(cfg.QueryProfileDir)
	cfg.DataRoot = strings.ToLower(strings.TrimSpace(cfg.DataRoot))
	cfg.Scale = strings.ToLower(strings.TrimSpace(cfg.Scale))
	if cfg.Rows == 0 {
		cfg.Rows, err = defaultRowsForScale(cfg.Scale)
		if err != nil {
			return cfg, err
		}
	}
	if cfg.MaxFiles == 0 {
		cfg.MaxFiles = defaultFilesForScale(cfg.Scale)
	}
	if cfg.Rows <= 0 {
		return cfg, errors.New("-rows must be positive")
	}
	if cfg.BatchSize <= 0 {
		return cfg, errors.New("-batch-size must be positive")
	}
	if cfg.LoadPipelineDepth < 0 {
		return cfg, errors.New("-load-pipeline-depth must be non-negative")
	}
	if cfg.EnginePrepareDepth < 0 || cfg.EnginePrepareDepth > 1 {
		return cfg, errors.New("-engine-prepare-depth requires 0 or 1")
	}
	if cfg.CompactBatchSize <= 0 {
		return cfg, errors.New("-compact-batch-size must be positive")
	}
	if cfg.Tries <= 0 {
		return cfg, errors.New("-tries must be positive")
	}
	if err := validateQueryModeAttempts(cfg.QueryMode, cfg.Tries); err != nil {
		return cfg, err
	}
	if _, err := collectionFormat(cfg.Format); err != nil {
		return cfg, err
	}
	if _, err := projectionFields(cfg.Projection); err != nil {
		return cfg, err
	}
	if _, err := rootStoragePolicy(cfg.DataRoot); err != nil {
		return cfg, err
	}
	if err := validateStorageLayoutConfig(cfg); err != nil {
		return cfg, err
	}
	if isColumnStoreLayout(cfg.StorageLayout) {
		if _, err := columnStoreConfigForProjection(cfg.Projection, cfg.StorageLayout, cfg.RetainedPayloadEncoding); err != nil {
			return cfg, err
		}
	}
	if enginePreparationTargetConfig(cfg) {
		if cfg.EnginePrepareMaxBytes <= 0 || cfg.LoadPipelineDepth > 1 || cfg.EnginePrepareDepth == 1 && cfg.LoadPipelineDepth != 1 {
			return cfg, errors.New("target engine preparation requires a positive byte limit, input depth at most 1, and input depth 1 when engine depth is 1")
		}
		if cfg.BatchSize > 16<<10 {
			return cfg, errors.New("target engine preparation requires -batch-size at most 16384")
		}
	}
	if cfg.ValidateReconstruction && !isFullDataColumnStoreLayout(cfg.StorageLayout) {
		return cfg, errors.New("-validate-reconstruction requires -storage-layout column-store-full or column-store-full-prepared")
	}
	if strings.TrimSpace(cfg.Collection) == "" {
		return cfg, errors.New("-collection cannot be empty")
	}
	return cfg, nil
}

type backendOpener func(runConfig) (*backenddb.DB, func() error, error)

func runTreeDBBenchmark(cfg runConfig) (runResult, error) {
	return runTreeDBBenchmarkWithOpener(cfg, openBackend)
}

func runTreeDBBenchmarkWithOpener(cfg runConfig, open backendOpener) (runResult, error) {
	var err error
	cfg.QueryMode, err = normalizeQueryMode(cfg.QueryMode)
	if err != nil {
		return runResult{}, err
	}
	cfg.MetadataMode, err = normalizeMetadataMode(cfg.MetadataMode)
	if err != nil {
		return runResult{}, err
	}
	if err := validateQueryModeAttempts(cfg.QueryMode, cfg.Tries); err != nil {
		return runResult{}, err
	}
	if cfg.QueryMode == queryModeFirstTouchAfterOpen && cfg.StorageLayout == storageLayoutColumnStoreFullPrepared {
		return runResult{}, errors.New("-query-mode first_touch_after_open is unsupported for column-store-full-prepared because query-ready generation preparation touches the reopened state before execution; use one_shot_end_to_end")
	}
	dataDir, err := expandPath(cfg.DataDir)
	if err != nil {
		return runResult{}, err
	}
	cfg.DataDir = dataDir
	if cfg.DBDir == "" {
		tmp, err := os.MkdirTemp("", "jsonbench_treedb_*")
		if err != nil {
			return runResult{}, err
		}
		cfg.DBDir = tmp
	} else {
		dbDir, err := expandPath(cfg.DBDir)
		if err != nil {
			return runResult{}, err
		}
		cfg.DBDir = dbDir
	}
	if cfg.QueryProfileDir != "" {
		queryProfileDir, err := expandPath(cfg.QueryProfileDir)
		if err != nil {
			return runResult{}, err
		}
		cfg.QueryProfileDir = queryProfileDir
		if err := os.MkdirAll(cfg.QueryProfileDir, 0o755); err != nil {
			return runResult{}, fmt.Errorf("create query profile dir: %w", err)
		}
	}
	if cfg.Reset {
		if err := os.RemoveAll(cfg.DBDir); err != nil {
			return runResult{}, fmt.Errorf("reset db dir: %w", err)
		}
	}
	if err := os.MkdirAll(cfg.DBDir, 0o755); err != nil {
		return runResult{}, fmt.Errorf("create db dir: %w", err)
	}

	backend, cleanup, err := open(cfg)
	if err != nil {
		return runResult{}, err
	}
	defer func() {
		if cleanup != nil {
			_ = cleanup()
		}
	}()

	manager := collections.NewCollectionManager(backend)
	collection, err := createCollection(manager, cfg)
	if err != nil {
		return runResult{}, err
	}
	files, err := inputFiles(cfg.DataDir, cfg.MaxFiles)
	if err != nil {
		return runResult{}, err
	}
	load, err := loadData(collection, backend, cfg, files)
	if err != nil {
		return runResult{}, err
	}
	if cfg.AllowErrors {
		if load.InputRows != cfg.Rows {
			return runResult{}, fmt.Errorf("processed %d input rows and loaded %d valid rows, requested %d from %s; point -data-dir at enough data or lower -rows", load.InputRows, load.Rows, cfg.Rows, cfg.DataDir)
		}
	} else if load.Rows != cfg.Rows {
		return runResult{}, fmt.Errorf("loaded %d rows, requested %d from %s; point -data-dir at enough data or lower -rows", load.Rows, cfg.Rows, cfg.DataDir)
	}
	var compaction *compactionResult
	if cfg.CompactAfterLoad {
		compact, err := compactLoadedData(context.Background(), collection, backend, cfg, load.Rows)
		if err != nil {
			return runResult{}, err
		}
		compaction = &compact
	}
	if cfg.QueryMode == queryModeFirstTouchAfterOpen || cfg.StorageLayout == storageLayoutColumnStoreFullPrepared {
		closeBackend := cleanup
		if err := closeBackend(); err != nil {
			return runResult{}, fmt.Errorf("close backend before query-ready reopen: %w", err)
		}
		cleanup = nil
		reopenedBackend, reopenedCleanup, reopenErr := open(cfg)
		if reopenErr != nil {
			return runResult{}, fmt.Errorf("reopen backend for query execution: %w", reopenErr)
		}
		backend, cleanup = reopenedBackend, reopenedCleanup
		manager = collections.NewCollectionManager(backend)
		collection, err = manager.OpenCollection(cfg.Collection)
		if err != nil {
			return runResult{}, fmt.Errorf("reopen collection for query execution: %w", err)
		}
	}
	var queryReadyPreparation *collections.QueryReadyColumnPreparedGeneration
	var queryReadyPreparationStats *collections.QueryReadyColumnPreparationStats
	var queryReadyFiles *collections.QueryReadyColumnGenerationFiles
	if cfg.StorageLayout == storageLayoutColumnStoreFullPrepared {
		queryReadyPreparation, err = collection.PrepareQueryReadyColumnGeneration(context.Background(), collections.QueryReadyColumnPreparationOptions{})
		if err != nil {
			return runResult{}, fmt.Errorf("prepare query-ready column generation: %w", err)
		}
		stats := queryReadyPreparation.Stats()
		files := queryReadyPreparation.Files()
		queryReadyPreparationStats, queryReadyFiles = &stats, &files
	}
	queryResults, err := runQueries(collection, cfg, load.Rows, queryReadyFiles)
	if queryReadyPreparation != nil {
		err = errors.Join(err, queryReadyPreparation.Close())
	}
	if err != nil {
		return runResult{}, err
	}
	var reconstruction *reconstructionResult
	if cfg.ValidateReconstruction {
		validated, err := validateStoredReconstruction(collection, cfg, load.Rows, load.SourceCanonicalJSONHash)
		if err != nil {
			return runResult{}, err
		}
		reconstruction = &validated
	}
	measurementPhase := "post_load"
	if cfg.CompactAfterLoad {
		measurementPhase = "post_maintenance"
	}
	storage, err := collectTreeDBStorageUsage(context.Background(), cfg.DBDir, collection, cfg, load.Rows, measurementPhase)
	if err != nil {
		return runResult{}, err
	}
	retainedPayloadEncoding, retainedPayloadEncodingStatus := columnStoreRetainedPayloadEncodingStatus(cfg)
	return runResult{
		SchemaVersion:                 schemaVersion,
		GeneratedAt:                   time.Now().UTC().Format(time.RFC3339),
		System:                        "TreeDB",
		Engine:                        treeDBEngineName(cfg),
		Scale:                         cfg.Scale,
		ScaleLabel:                    scaleLabel(cfg.Scale, cfg.Rows, load.Rows),
		RequestedRows:                 cfg.Rows,
		DatasetSize:                   load.Rows,
		DataDir:                       cfg.DataDir,
		DBDir:                         cfg.DBDir,
		Collection:                    cfg.Collection,
		Format:                        cfg.Format,
		StorageLayout:                 cfg.StorageLayout,
		QueryMode:                     cfg.QueryMode,
		MetadataMode:                  cfg.MetadataMode,
		Projection:                    cfg.Projection,
		RetainsJSON:                   cfg.Projection == "full" && (cfg.StorageLayout == storageLayoutRow || isFullDataColumnStoreLayout(cfg.StorageLayout)),
		DataShape:                     treeDBDataShape(cfg),
		RetainedPayloadPolicy:         columnStoreRetainedPayloadPolicy(cfg),
		RetainedPayloadEncoding:       retainedPayloadEncoding,
		RetainedPayloadEncodingStatus: retainedPayloadEncodingStatus,
		ColumnReconstructionPolicy:    columnStoreReconstructionPolicy(cfg),
		TypedColumnOwner:              columnStoreTypedColumnOwner(cfg),
		Profile:                       cfg.Profile,
		QueryProfileDir:               cfg.QueryProfileDir,
		DataRoot:                      cfg.DataRoot,
		Load:                          load,
		Storage:                       storage,
		Compaction:                    compaction,
		Reconstruction:                reconstruction,
		Queries:                       queryResults,
		QueryReadyPreparation:         queryReadyPreparationStats,
		Notes:                         runNotes(cfg),
	}, nil
}

func validateQueryModeAttempts(queryMode string, tries int) error {
	switch queryMode {
	case queryModeOneShotEndToEnd:
		if tries != 1 {
			return errors.New("-query-mode one_shot_end_to_end measures one submitted execution; pass -tries 1")
		}
	case queryModeFirstTouchAfterOpen:
		if tries != 1 {
			return errors.New("-query-mode first_touch_after_open measures one freshly opened execution; pass -tries 1")
		}
	}
	return nil
}

func compactLoadedData(ctx context.Context, collection *collections.Collection, _ *backenddb.DB, cfg runConfig, rows int) (compactionResult, error) {
	out := compactionResult{Enabled: true}
	wallStart := time.Now()

	storageBefore, err := collectTreeDBStorageUsage(ctx, cfg.DBDir, collection, cfg, rows, "pre_maintenance")
	if err != nil {
		return out, fmt.Errorf("measure storage before compaction: %w", err)
	}
	out.StorageBefore = storageBefore

	stats, err := collection.CompactStorage(ctx, collections.CompactStorageOptions{
		SyncEachPhase:            true,
		ValueLogRewriteBatchSize: cfg.CompactBatchSize,
	})
	if err != nil {
		return out, fmt.Errorf("compact storage: %w", err)
	}
	if !stats.Storage.FullyCompacted {
		return out, fmt.Errorf("compact storage left remaining debt: %+v", stats.Storage.RemainingDebt)
	}
	out.CompactStorageStats = stats.Storage
	if rootStats, ok := stats.RootOverlays[cfg.Collection]; ok {
		out.RootOverlayStats = rootStats
	}
	out.ValueLogRewriteStats = stats.Storage.ValueLogRewrite
	out.ValueLogGCStats = stats.Storage.ValueLogGC
	out.LeafGenerationGCStats = stats.Storage.LeafGenerationGC

	storageAfter, err := collectTreeDBStorageUsage(ctx, cfg.DBDir, collection, cfg, rows, "post_maintenance")
	if err != nil {
		return out, fmt.Errorf("measure storage after compaction: %w", err)
	}
	out.StorageAfter = storageAfter
	out.WallSec = time.Since(wallStart).Seconds()
	return out, nil
}

func scaleLabel(scale string, requestedRows, loadedRows int) string {
	switch strings.ToLower(strings.TrimSpace(scale)) {
	case "subset", "smoke":
		if requestedRows > 0 && loadedRows > 0 && requestedRows != loadedRows {
			return fmt.Sprintf("%d of %d requested rows", loadedRows, requestedRows)
		}
		return fmt.Sprintf("%d rows", loadedRows)
	default:
		return scale
	}
}

func openBackend(cfg runConfig) (*backenddb.DB, func() error, error) {
	profile, err := parseProfile(cfg.Profile)
	if err != nil {
		return nil, nil, err
	}
	opts := treedb.OptionsFor(profile, cfg.DBDir)
	compressed := cfg.DataRoot == "compressed"
	if compressed {
		opts.IndexOuterLeavesInValueLog = true
		opts.IndexInternalBaseDelta = false
	}
	if isColumnStoreLayout(cfg.StorageLayout) {
		// Current typed-column publication requires durable command-WAL mode even
		// for benchmark-relaxed column-store metadata. Keep the selected profile's
		// other explicit performance knobs, but apply the immutable durable profile
		// contract required by the public column-store write path. Let TreeDB persist
		// the full format config during open so index layout knobs (notably outer
		// leaf-log storage) stay in sync with the selected profile instead of
		// pre-writing a partial config.
		treedb.ApplyProfile(&opts, treedb.ProfileCommandWALDurable)
	}
	return treedb.OpenBackendWithCachedLeafLog(opts)
}

func createCollection(manager *collections.CollectionManager, cfg runConfig) (*collections.Collection, error) {
	format, err := collectionFormat(cfg.Format)
	if err != nil {
		return nil, err
	}
	policy, err := rootStoragePolicy(cfg.DataRoot)
	if err != nil {
		return nil, err
	}
	var columnStore *collections.ColumnStoreConfig
	if isColumnStoreLayout(cfg.StorageLayout) {
		columnStore, err = columnStoreConfigForProjection(cfg.Projection, cfg.StorageLayout, cfg.RetainedPayloadEncoding)
		if err != nil {
			return nil, err
		}
	}
	_, err = manager.CreateCollection(&collections.CollectionMeta{
		Name: cfg.Collection,
		Options: collections.CollectionOptions{
			DocumentFormat:          format,
			DataRootStoragePolicy:   policy,
			IndexStateStoragePolicy: policy,
			ColumnStore:             columnStore,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("create collection: %w", err)
	}
	collection, err := manager.OpenCollection(cfg.Collection)
	if err != nil {
		return nil, fmt.Errorf("open collection: %w", err)
	}
	return collection, nil
}

func enginePreparationTargetConfig(cfg runConfig) bool {
	if cfg.StorageLayout != storageLayoutColumnStoreFullPrepared {
		return false
	}
	encoding, _ := columnStoreRetainedPayloadEncodingStatus(cfg)
	return encoding == string(collections.ColumnRetainedPayloadEncodingSemanticStreamV1)
}

func loadData(collection *collections.Collection, backend *backenddb.DB, cfg runConfig, files []string) (loadResult, error) {
	format, err := collectionFormat(cfg.Format)
	if err != nil {
		return loadResult{}, err
	}
	retainedEncoding, _ := collections.ColumnRetainedPayloadEncodingStatus(collection.Meta().Options.ColumnStore)
	// Direct programmatic configs with an unset limit keep the historical
	// ordinary path. Parsed target CLI configs require a positive limit.
	engineTarget := cfg.StorageLayout == storageLayoutColumnStoreFullPrepared && cfg.Projection == "full" && format == collections.DocumentFormatJSON &&
		retainedEncoding == string(collections.ColumnRetainedPayloadEncodingSemanticStreamV1) && cfg.EnginePrepareMaxBytes > 0
	const preparedSourceScratchReserve = 2 << 20 // Scanner token and gzip/buffered-reader scratch.
	const preparedIdleScratchReserve = 32 << 20  // TreeDB retained raw-block pool: four 8 MiB slots.
	const preparedSourceBatchCeiling = 10 << 20
	var sourceBatchCeiling, sourceSlotBytes int64
	var engineReservation *enginePrepareReservation
	if engineTarget {
		if cfg.EnginePrepareDepth < 0 || cfg.EnginePrepareDepth > 1 || cfg.LoadPipelineDepth < 0 || cfg.LoadPipelineDepth > 1 || cfg.EnginePrepareDepth == 1 && cfg.LoadPipelineDepth != 1 {
			return loadResult{}, fmt.Errorf("target engine preparation requires engine and input depths 0 or 1; engine depth 1 requires input depth 1")
		}
		if cfg.BatchSize <= 0 || cfg.BatchSize > 16<<10 {
			return loadResult{}, fmt.Errorf("%w: target batch size %d must be between 1 and 16384 rows", collections.ErrPreparedInsertResourceLimit, cfg.BatchSize)
		}
		fixedReserve := int64(preparedSourceScratchReserve + preparedIdleScratchReserve)
		if cfg.EnginePrepareMaxBytes <= fixedReserve {
			return loadResult{}, fmt.Errorf("%w: prepared engine byte limit %d cannot reserve source and idle encoder scratch", collections.ErrPreparedInsertResourceLimit, cfg.EnginePrepareMaxBytes)
		}
		engineReservation = newEnginePrepareReservation(cfg.EnginePrepareMaxBytes - fixedReserve)
		sourceBatchCeiling = min(int64(preparedSourceBatchCeiling), max(int64(1<<20), cfg.EnginePrepareMaxBytes/16))
		sourceSlotBytes = sourceBatchCeiling + int64(2*cfg.BatchSize)*24
		if sourceSlotBytes >= engineReservation.limit {
			return loadResult{}, fmt.Errorf("%w: prepared source slot %d exceeds byte limit %d", collections.ErrPreparedInsertResourceLimit, sourceSlotBytes, cfg.EnginePrepareMaxBytes)
		}
	}
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)
	out := loadResult{Files: make([]string, 0, len(files))}
	var batches int
	var generationElapsed time.Duration
	var insertStats insertStatsAccounting
	var aggregateMetadataAccounting aggregateMetadataLoadAccounting
	var flushElapsed time.Duration
	var checkpointElapsed time.Duration
	var enginePrepareElapsed, engineCommitElapsed, engineOverlapElapsed time.Duration
	var previousCommitStart, previousCommitEnd time.Time
	var enginePrepared, engineCommitted int
	var engineAbandoned atomic.Int64
	var engineFallbackBatches atomic.Int64
	var producerCreditWait atomic.Int64
	var producerSourceCreditWait, producerPrepareCreditWait, producerRetryWait, producerHandoffWait atomic.Int64
	var engineMaxTokenReservedBytes atomic.Int64
	var engineBudgetRetryBatches atomic.Int64
	var engineFirstBudgetRetryReason string
	var engineFallbackReason string
	var engineLiveBytes, enginePeakOwnedBytes atomic.Int64
	var engineLiveBatches, enginePeakOwnedBatches atomic.Int64
	trackPrepared := func(prepared *collections.PreparedInsertBatch) {
		if prepared == nil {
			return
		}
		liveBytes := engineLiveBytes.Add(prepared.OwnedBytes())
		liveBatches := engineLiveBatches.Add(1)
		for peak := enginePeakOwnedBytes.Load(); liveBytes > peak && !enginePeakOwnedBytes.CompareAndSwap(peak, liveBytes); peak = enginePeakOwnedBytes.Load() {
		}
		for peak := enginePeakOwnedBatches.Load(); liveBatches > peak && !enginePeakOwnedBatches.CompareAndSwap(peak, liveBatches); peak = enginePeakOwnedBatches.Load() {
		}
	}
	releasePrepared := func(prepared *collections.PreparedInsertBatch) {
		if prepared == nil {
			return
		}
		engineLiveBytes.Add(-prepared.OwnedBytes())
		engineLiveBatches.Add(-1)
	}
	prepareEngineBatch := func(ctx context.Context, batch *preparedLoadBatch) error {
		if !engineTarget {
			return nil
		}
		for {
			creditWaitStart := time.Now()
			extra, hadOther, err := engineReservation.acquireAvailable(ctx, sourceSlotBytes, batch.reservation)
			if cfg.EnginePrepareDepth == 1 {
				elapsed := int64(time.Since(creditWaitStart))
				producerCreditWait.Add(elapsed)
				producerPrepareCreditWait.Add(elapsed)
			}
			if err != nil {
				if batch.prepCreditReady != nil {
					close(batch.prepCreditReady)
				}
				return err
			}
			batch.reservation += extra
			if batch.prepCreditReady != nil {
				close(batch.prepCreditReady)
				batch.prepCreditReady = nil
			}
			batch.enginePrepareStart = time.Now()
			batch.engine, err = collection.PrepareInsertBatchOwned(batch.ids, batch.docs, batch.reservation)
			batch.enginePrepareEnd = time.Now()
			enginePrepareElapsed += batch.enginePrepareEnd.Sub(batch.enginePrepareStart)
			if err == nil {
				enginePrepared++
				trackPrepared(batch.engine)
				for peak := engineMaxTokenReservedBytes.Load(); batch.engine.ReservedBytes() > peak && !engineMaxTokenReservedBytes.CompareAndSwap(peak, batch.engine.ReservedBytes()); peak = engineMaxTokenReservedBytes.Load() {
				}
				engineReservation.shrink(batch.reservation, batch.engine.ReservedBytes())
				batch.reservation = batch.engine.ReservedBytes()
				return nil
			}
			if cfg.EnginePrepareDepth == 1 && errors.Is(err, collections.ErrPreparedInsertResourceLimit) && hadOther {
				engineBudgetRetryBatches.Add(1)
				if engineFirstBudgetRetryReason == "" {
					engineFirstBudgetRetryReason = err.Error()
				}
				// A temporary shortfall is not a structural resource rejection.
				// Retire excess credit so the ordered committer can finish, then
				// retry once its reservation has been returned.
				engineReservation.release(extra)
				batch.reservation -= extra
				creditWaitStart = time.Now()
				err := engineReservation.waitForOtherOwner(ctx, batch.reservation)
				elapsed := int64(time.Since(creditWaitStart))
				producerCreditWait.Add(elapsed)
				producerRetryWait.Add(elapsed)
				if err != nil {
					return err
				}
				continue
			}
			if errors.Is(err, collections.ErrPreparedInsertResourceLimit) || !errors.Is(err, collections.ErrPreparedInsertIneligible) {
				return err
			}
			engineFallbackReason = err.Error()
			engineFallbackBatches.Add(1)
			return nil
		}
	}
	wallStart := time.Now()
	var sourceHasher *canonicalJSONHasher
	if cfg.ValidateReconstruction && !engineTarget {
		sourceHasher = newCanonicalJSONHasher()
	}

	prepare := func(ctx context.Context, emit func(context.Context, preparedLoadBatch) error) error {
		var encoder collections.TemplateV1Encoder
		var ids, docs [][]byte
		if !engineTarget {
			ids = make([][]byte, 0, cfg.BatchSize)
			docs = make([][]byte, 0, cfg.BatchSize)
		}
		var currentReservation int64
		defer func() {
			if currentReservation != 0 {
				engineReservation.release(currentReservation)
			}
		}()
		ensureBatchBuffers := func() error {
			if !engineTarget || currentReservation != 0 {
				return nil
			}
			outerBytes := int64(2*cfg.BatchSize) * 24
			creditWaitStart := time.Now()
			err := engineReservation.acquire(ctx, outerBytes)
			elapsed := int64(time.Since(creditWaitStart))
			producerCreditWait.Add(elapsed)
			producerSourceCreditWait.Add(elapsed)
			if err != nil {
				return err
			}
			currentReservation = outerBytes
			ids = make([][]byte, 0, cfg.BatchSize)
			docs = make([][]byte, 0, cfg.BatchSize)
			return nil
		}
		var logicalBytes int64
		batchOrdinal := 0
		lastProgress := time.Now()
		targetReached := func() bool {
			if cfg.AllowErrors {
				return out.InputRows >= cfg.Rows
			}
			return out.Rows >= cfg.Rows
		}
		emitBatch := func() error {
			if len(ids) == 0 {
				return nil
			}
			batchOrdinal++
			batch := preparedLoadBatch{
				ordinal:      batchOrdinal,
				ids:          ids,
				docs:         docs,
				logicalBytes: logicalBytes,
				reservation:  currentReservation,
			}
			if engineTarget {
				if cfg.EnginePrepareDepth == 1 {
					if err := prepareEngineBatch(ctx, &batch); err != nil {
						currentReservation = batch.reservation
						return err
					}
				} else if cfg.LoadPipelineDepth > 0 {
					batch.prepCreditReady = make(chan struct{})
				}
			}
			currentReservation = 0 // ownership transfers to the emitted batch
			if err := emit(ctx, batch); err != nil {
				// Depth zero invokes insert synchronously, so the consumer has
				// already released this batch. At depth one, a failed send did not
				// transfer it to the consumer.
				if cfg.LoadPipelineDepth > 0 {
					if batch.engine != nil {
						batch.engine.Abandon()
						releasePrepared(batch.engine)
						engineAbandoned.Add(1)
					}
					if engineReservation != nil {
						engineReservation.release(batch.reservation)
					}
				}
				return err
			}
			if batch.prepCreditReady != nil {
				creditWaitStart := time.Now()
				select {
				case <-batch.prepCreditReady:
				case <-ctx.Done():
					elapsed := int64(time.Since(creditWaitStart))
					producerCreditWait.Add(elapsed)
					producerHandoffWait.Add(elapsed)
					return ctx.Err()
				}
				elapsed := int64(time.Since(creditWaitStart))
				producerCreditWait.Add(elapsed)
				producerHandoffWait.Add(elapsed)
			}
			if engineTarget {
				ids, docs = nil, nil // allocate successor backing only after admission
			} else {
				ids, docs = resetPreparedLoadBuffers(ids, docs, cfg.BatchSize, cfg.LoadPipelineDepth == 0)
			}
			logicalBytes = 0
			return nil
		}

		for _, path := range files {
			if targetReached() {
				break
			}
			maxInputLineBytes := 1 << 30
			if engineTarget {
				// The prepared lane fails closed on unusually large source rows
				// before Scanner can grow to its ordinary 1 GiB token limit.
				maxInputLineBytes = 1 << 20
			}
			readBytes, compressedBytes, err := scanInputFile(path, maxInputLineBytes, func(raw []byte) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
				if targetReached() {
					return errStopScan
				}
				out.InputRows++
				if !json.Valid(raw) {
					if cfg.AllowErrors {
						out.SkippedInvalidJSONRows++
						return nil
					}
					return fmt.Errorf("invalid source JSON input row %d", out.InputRows)
				}
				if err := ensureBatchBuffers(); err != nil {
					return err
				}
				if engineTarget {
					if len(raw) > 128<<10 {
						return fmt.Errorf("%w: document at input row %d exceeds prepared 128 KiB limit", collections.ErrPreparedInsertResourceLimit, out.InputRows)
					}
					// The full projection clones this source row. Check the request
					// and outer slice capacities before the clone can allocate.
					outerBytes := int64(cap(ids)+cap(docs)) * 24
					needed := outerBytes + logicalBytes + int64(len(raw)) + 8
					if needed > currentReservation {
						if needed > engineReservation.limit {
							return fmt.Errorf("%w: source batch needs %d bytes before input row %d, limit %d", collections.ErrPreparedInsertResourceLimit, needed, out.InputRows, engineReservation.limit)
						}
						creditWaitStart := time.Now()
						err := engineReservation.acquire(ctx, needed-currentReservation)
						elapsed := int64(time.Since(creditWaitStart))
						producerCreditWait.Add(elapsed)
						producerSourceCreditWait.Add(elapsed)
						if err != nil {
							return err
						}
						currentReservation = needed
					}
					if logicalBytes+int64(len(raw)+8) > sourceBatchCeiling {
						return fmt.Errorf("%w: prepared source batch exceeds %d bytes at input row %d", collections.ErrPreparedInsertResourceLimit, sourceBatchCeiling, out.InputRows)
					}
				}
				if sourceHasher != nil {
					if err := sourceHasher.Add(raw); err != nil {
						return fmt.Errorf("hash source JSON input row %d: %w", out.InputRows, err)
					}
				}
				genStart := time.Now()
				doc, err := buildDocument(raw, format, cfg.Projection, cfg.StorageLayout, &encoder)
				if err != nil {
					return fmt.Errorf("build document input row %d: %w", out.InputRows, err)
				}
				id := documentID(uint64(out.Rows + 1))
				generationElapsed += time.Since(genStart)
				ids = append(ids, id)
				docs = append(docs, doc)
				logicalBytes += int64(len(id) + len(doc))
				out.Rows++
				if len(ids) >= cfg.BatchSize {
					if err := emitBatch(); err != nil {
						return err
					}
				}
				if cfg.Progress && time.Since(lastProgress) >= time.Second {
					fmt.Fprintf(os.Stderr, "loaded %d/%d rows into %s\n", out.Rows, cfg.Rows, cfg.DBDir)
					lastProgress = time.Now()
				}
				return nil
			})
			if err != nil {
				return fmt.Errorf("read %s: %w", path, err)
			}
			out.Files = append(out.Files, path)
			out.BytesRead += readBytes
			out.CompressedBytes += compressedBytes
		}
		return emitBatch()
	}

	pipelineStats, err := runPreparedLoadPipeline(
		context.Background(),
		cfg.LoadPipelineDepth,
		prepare,
		func(batch preparedLoadBatch) error {
			if engineReservation != nil {
				defer func() { engineReservation.release(batch.reservation) }()
			}
			prepared := batch.engine
			if engineTarget && cfg.EnginePrepareDepth == 0 {
				if err := prepareEngineBatch(context.Background(), &batch); err != nil {
					return err
				}
				prepared = batch.engine
			}
			if prepared != nil && !previousCommitStart.IsZero() {
				start := batch.enginePrepareStart
				if start.Before(previousCommitStart) {
					start = previousCommitStart
				}
				end := batch.enginePrepareEnd
				if end.After(previousCommitEnd) {
					end = previousCommitEnd
				}
				if end.After(start) {
					engineOverlapElapsed += end.Sub(start)
				}
			}
			start := time.Now()
			var commitErr error
			if prepared != nil {
				_, commitErr = prepared.Commit()
				releasePrepared(prepared)
				if commitErr == nil {
					engineCommitted++
				}
			} else {
				_, commitErr = collection.InsertBatch(batch.ids, batch.docs)
			}
			commitEnd := time.Now()
			engineCommitElapsed += commitEnd.Sub(start)
			previousCommitStart, previousCommitEnd = start, commitEnd
			if commitErr != nil {
				return commitErr
			}
			lastInsertStats := collection.LastInsertStats()
			insertStats.add(lastInsertStats)
			aggregateMetadataAccounting.addInsertStats(lastInsertStats)
			batches++
			return nil
		},
		func(batch preparedLoadBatch) {
			if engineReservation != nil {
				engineReservation.release(batch.reservation)
			}
			if batch.engine != nil {
				batch.engine.Abandon()
				releasePrepared(batch.engine)
				engineAbandoned.Add(1)
			}
		},
	)
	// Pipeline work excludes only channel-send wait. Reservation admission and
	// the depth-zero credit handoff also leave the producer idle; remove those
	// waits before reporting work or overlap. Depth-one preparation is producer
	// work, so expose source overlap separately from engine/commit overlap.
	pipelineStats.ProducerCreditWait = time.Duration(producerCreditWait.Load())
	pipelineStats.ProducerWork = max(0, pipelineStats.ProducerWork-pipelineStats.ProducerCreditWait)
	pipelineStats.Overlap = max(0, pipelineStats.Overlap-pipelineStats.ProducerCreditWait)
	pipelineStats.InputOverlap = max(0, pipelineStats.Overlap-engineOverlapElapsed)
	if err != nil {
		return loadResult{}, err
	}
	if cfg.ValidateReconstruction && engineTarget {
		// Canonicalization decodes into dynamic Go maps. Keep that validation
		// pass outside the bounded producer/committer window so its scratch
		// cannot coexist with either prepared token or commit buffers.
		sourceHasher = newCanonicalJSONHasher()
		remaining := out.Rows
		for _, path := range out.Files {
			if remaining == 0 {
				break
			}
			_, _, scanErr := scanInputFile(path, 1<<20, func(raw []byte) error {
				if remaining == 0 {
					return errStopScan
				}
				if !json.Valid(raw) {
					return nil
				}
				if err := sourceHasher.Add(raw); err != nil {
					return err
				}
				remaining--
				return nil
			})
			if scanErr != nil {
				return loadResult{}, fmt.Errorf("hash source after bounded load %s: %w", path, scanErr)
			}
		}
		if remaining != 0 {
			return loadResult{}, fmt.Errorf("source hash pass found %d of %d loaded rows", out.Rows-remaining, out.Rows)
		}
	}
	flushStart := time.Now()
	if err := collection.Flush(); err != nil {
		return loadResult{}, fmt.Errorf("flush collection: %w", err)
	}
	flushElapsed = time.Since(flushStart)
	if cfg.Checkpoint {
		checkpointStart := time.Now()
		if err := backend.Checkpoint(); err != nil {
			return loadResult{}, fmt.Errorf("checkpoint: %w", err)
		}
		checkpointElapsed = time.Since(checkpointStart)
	}
	wallElapsed := time.Since(wallStart)
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)
	out.Batches = batches
	out.GenerationSec = generationElapsed.Seconds()
	out.InsertSec = pipelineStats.InsertElapsed.Seconds()
	out.PipelineDepth = pipelineStats.Depth
	out.EnginePrepareDepth = cfg.EnginePrepareDepth
	out.EnginePreparePath = "ordinary"
	if engineTarget {
		out.EnginePreparePath = "prepared"
	}
	if engineFallbackReason != "" {
		out.EnginePreparePath = "mixed-or-fallback"
	}
	out.EngineFallbackReason = engineFallbackReason
	out.EngineFallbackBatches = int(engineFallbackBatches.Load())
	out.EnginePreparedBatches = enginePrepared
	out.EngineCommittedBatches = engineCommitted
	out.EngineAbandonedBatches = int(engineAbandoned.Load())
	out.EnginePrepareSec = enginePrepareElapsed.Seconds()
	out.EngineCommitSec = engineCommitElapsed.Seconds()
	out.EngineOverlapSec = engineOverlapElapsed.Seconds()
	out.EnginePeakOwnedBytes = enginePeakOwnedBytes.Load()
	if engineReservation != nil {
		out.EngineIdleScratchReserveBytes = preparedIdleScratchReserve
		out.EnginePeakReservedBytes = engineReservation.peak() + preparedSourceScratchReserve + preparedIdleScratchReserve
	}
	out.EnginePeakOwnedBatches = int(enginePeakOwnedBatches.Load())
	out.EngineMaxTokenReservedBytes = engineMaxTokenReservedBytes.Load()
	out.EngineBudgetRetryBatches = int(engineBudgetRetryBatches.Load())
	out.EngineFirstBudgetRetryReason = engineFirstBudgetRetryReason
	out.ProducerElapsedSec = pipelineStats.ProducerElapsed.Seconds()
	out.ProducerWorkSec = pipelineStats.ProducerWork.Seconds()
	out.ProducerWaitSec = pipelineStats.ProducerWait.Seconds()
	out.ProducerCreditWaitSec = pipelineStats.ProducerCreditWait.Seconds()
	out.ProducerSourceCreditWaitSec = time.Duration(producerSourceCreditWait.Load()).Seconds()
	out.ProducerPrepareCreditWaitSec = time.Duration(producerPrepareCreditWait.Load()).Seconds()
	out.ProducerRetryWaitSec = time.Duration(producerRetryWait.Load()).Seconds()
	out.ProducerHandoffWaitSec = time.Duration(producerHandoffWait.Load()).Seconds()
	out.ConsumerWaitSec = pipelineStats.ConsumerWait.Seconds()
	out.OverlapSec = pipelineStats.Overlap.Seconds()
	out.InputOverlapSec = pipelineStats.InputOverlap.Seconds()
	out.MaxQueuedBatches = pipelineStats.MaxQueuedBatches
	out.MaxBatchBytes = pipelineStats.MaxBatchBytes
	out.MaxInFlightBytesBound = pipelineStats.MaxInFlightBytesBound
	if memAfter.TotalAlloc >= memBefore.TotalAlloc {
		out.AllocatedBytes = memAfter.TotalAlloc - memBefore.TotalAlloc
	}
	if memAfter.Mallocs >= memBefore.Mallocs {
		out.Allocations = memAfter.Mallocs - memBefore.Mallocs
	}
	if out.Rows > 0 {
		out.AllocatedBytesPerRow = float64(out.AllocatedBytes) / float64(out.Rows)
		out.AllocationsPerRow = float64(out.Allocations) / float64(out.Rows)
	}
	out.InsertStats = insertStats.result()
	aggregateMetadataAccounting.apply(&out)
	out.FlushSec = flushElapsed.Seconds()
	out.CheckpointSec = checkpointElapsed.Seconds()
	out.WallSec = wallElapsed.Seconds()
	if out.Rows > 0 && wallElapsed > 0 {
		out.RowsPerSec = float64(out.Rows) / wallElapsed.Seconds()
	}
	if sourceHasher != nil {
		out.SourceCanonicalJSONHash = sourceHasher.Sum()
	}
	return out, nil
}

func validateStoredReconstruction(collection *collections.Collection, cfg runConfig, rows int, sourceHash string) (reconstructionResult, error) {
	out := reconstructionResult{
		Enabled: true,
		Mode:    "canonical_json_hash",
	}
	if !isFullDataColumnStoreLayout(cfg.StorageLayout) || cfg.Projection != "full" {
		return out, fmt.Errorf("reconstruction validation requires full-data column-store layout, got layout=%s projection=%s", cfg.StorageLayout, cfg.Projection)
	}
	if sourceHash == "" {
		return out, errors.New("reconstruction validation missing source canonical JSON hash")
	}
	hasher := newCanonicalJSONHasher()
	scanned, err := scanCollectionJSON(collection, rows+1, func(raw []byte) error {
		if err := hasher.Add(raw); err != nil {
			return fmt.Errorf("hash stored JSON row %d: %w", hasher.Rows()+1, err)
		}
		return nil
	})
	if err != nil {
		return out, err
	}
	if scanned != rows {
		return out, fmt.Errorf("reconstruction validation scanned %d stored rows, want %d", scanned, rows)
	}
	out.Rows = scanned
	out.ScanStats = readDocumentScanStats(collection)
	out.SourceCanonicalJSONHash = sourceHash
	out.StoredCanonicalJSONHash = hasher.Sum()
	out.Valid = out.StoredCanonicalJSONHash == out.SourceCanonicalJSONHash
	if !out.Valid {
		out.Mismatch = fmt.Sprintf("reconstructed JSON canonical hash=%s want source hash=%s", out.StoredCanonicalJSONHash, out.SourceCanonicalJSONHash)
	}
	return out, nil
}

func readDocumentScanStats(source *collections.Collection) *documentScanStatsResult {
	if source == nil {
		return nil
	}
	return documentScanStatsResultFromCollectionStats(source.LastDocumentScanStats())
}

func documentScanStatsResultFromCollectionStats(stats collections.CollectionDocumentScanStats) *documentScanStatsResult {
	return &documentScanStatsResult{
		CertifiedMonotonicPath:    stats.CertifiedMonotonicPath,
		GenericFallback:           stats.GenericFallback,
		PhysicalPasses:            stats.PhysicalPasses,
		PhysicalRows:              stats.PhysicalRows,
		PhysicalBytes:             stats.PhysicalBytes,
		PhysicalDecodedBlocks:     stats.PhysicalDecodedBlocks,
		LocatorLookupBatches:      stats.LocatorLookupBatches,
		LocatorLookups:            stats.LocatorLookups,
		PointRowFetches:           stats.PointRowFetches,
		ReconstructedRows:         stats.ReconstructedRows,
		MaxRecordWindow:           stats.MaxRecordWindow,
		MaxVisibleRowWindow:       stats.MaxVisibleRowWindow,
		MaxTypedGenerations:       stats.MaxTypedGenerations,
		MaxTypedDecodedBytes:      stats.MaxTypedDecodedBytes,
		MaxTypedSourcePartBytes:   stats.MaxTypedSourcePartBytes,
		MaxRetainedBlocks:         stats.MaxRetainedBlocks,
		PreflightProjectedColumns: stats.PreflightProjectedColumns,
	}
}

var errStopScan = errors.New("stop scan")

func scanInputFile(path string, maxInputLineBytes int, fn func(raw []byte) error) (readBytes int64, compressedBytes int64, err error) {
	stat, statErr := os.Stat(path)
	if statErr == nil {
		compressedBytes = stat.Size()
	}
	file, err := os.Open(path)
	if err != nil {
		return 0, compressedBytes, err
	}
	defer func() { _ = file.Close() }()
	var reader io.Reader = file
	var gz *gzip.Reader
	if strings.HasSuffix(path, ".gz") {
		gz, err = gzip.NewReader(file)
		if err != nil {
			return 0, compressedBytes, err
		}
		defer func() { _ = gz.Close() }()
		reader = gz
	}
	counting := &countingReader{reader: reader}
	scanner := bufio.NewScanner(counting)
	scanner.Buffer(make([]byte, 0, min(1<<20, maxInputLineBytes)), maxInputLineBytes)
	var lineNumber int
	for scanner.Scan() {
		lineNumber++
		raw := bytes.TrimSpace(scanner.Bytes())
		if len(raw) == 0 {
			continue
		}
		if err := fn(raw); err != nil {
			if errors.Is(err, errStopScan) {
				return counting.n, compressedBytes, nil
			}
			return counting.n, compressedBytes, err
		}
	}
	if err := scanner.Err(); err != nil {
		return counting.n, compressedBytes, fmt.Errorf("source line %d in %s: %w", lineNumber+1, path, err)
	}
	return counting.n, compressedBytes, nil
}

type countingReader struct {
	reader io.Reader
	n      int64
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.n += int64(n)
	return n, err
}

func buildDocument(raw []byte, format collections.DocumentFormat, projection, storageLayout string, encoder *collections.TemplateV1Encoder) ([]byte, error) {
	if projection == "full" {
		if format == collections.DocumentFormatTemplateV1 {
			return collections.EncodeTemplateV1DocumentJSON(raw)
		}
		if storageLayout == storageLayoutColumnStoreFullPrepared {
			// Source credit is acquired before this clone using len(raw).
			// make with an explicit length gives the charged target lane an
			// exact-capacity backing, unlike bytes.Clone's append growth.
			doc := make([]byte, len(raw))
			copy(doc, raw)
			return doc, nil
		}
		return bytes.Clone(raw), nil
	}
	fields, err := projectionFields(projection)
	if err != nil {
		return nil, err
	}
	extracted := extractFullJSONFields(raw, fields)
	if isColumnStoreLayout(storageLayout) {
		applyColumnStoreQueryMask(&extracted, projection, storageLayout)
	}
	if format == collections.DocumentFormatTemplateV1 {
		if encoder == nil {
			encoder = &collections.TemplateV1Encoder{}
		}
		names, values := extracted.templateValues(fields)
		return encoder.EncodeDocument(names, values)
	}
	return extracted.minimalJSON(fields), nil
}

type extractedFields struct {
	Event     string
	DID       string
	Kind      string
	Operation string
	TimeUS    int64
}

func extractFullJSONFields(raw []byte, fields []string) extractedFields {
	var out extractedFields
	for _, field := range fields {
		switch field {
		case "event":
			out.Event = gjson.GetBytes(raw, "commit.collection").String()
		case "did":
			out.DID = gjson.GetBytes(raw, "did").String()
		case "kind":
			out.Kind = gjson.GetBytes(raw, "kind").String()
		case "operation":
			out.Operation = gjson.GetBytes(raw, "commit.operation").String()
		case "time_us":
			out.TimeUS = jsonInt64(gjson.GetBytes(raw, "time_us"))
		}
	}
	return out
}

func (f extractedFields) templateValues(fields []string) ([]string, []any) {
	names := make([]string, 0, len(fields))
	values := make([]any, 0, len(fields))
	for _, field := range fields {
		names = append(names, field)
		switch field {
		case "event":
			values = append(values, f.Event)
		case "did":
			values = append(values, f.DID)
		case "kind":
			values = append(values, f.Kind)
		case "operation":
			values = append(values, f.Operation)
		case "time_us":
			values = append(values, f.TimeUS)
		}
	}
	return names, values
}

func (f extractedFields) minimalJSON(fields []string) []byte {
	out := make([]byte, 0, 128)
	out = append(out, '{')
	for i, field := range fields {
		if i > 0 {
			out = append(out, ',')
		}
		out = appendJSONString(out, field)
		out = append(out, ':')
		switch field {
		case "event":
			out = appendJSONString(out, f.Event)
		case "did":
			out = appendJSONString(out, f.DID)
		case "kind":
			out = appendJSONString(out, f.Kind)
		case "operation":
			out = appendJSONString(out, f.Operation)
		case "time_us":
			out = strconv.AppendInt(out, f.TimeUS, 10)
		}
	}
	out = append(out, '}')
	return out
}

func appendJSONString(dst []byte, s string) []byte {
	encoded, err := json.Marshal(s)
	if err != nil {
		return append(dst, `""`...)
	}
	return append(dst, encoded...)
}

func parseQueryList(raw string) ([]string, error) {
	raw = strings.TrimSpace(strings.ToLower(raw))
	if raw == "" || raw == "all" {
		return append([]string(nil), jsonBenchQueryNames...), nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		name := strings.TrimSpace(strings.ToLower(part))
		if _, ok := querySQL[name]; !ok {
			return nil, fmt.Errorf("unknown query %q", part)
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	return out, nil
}

func projectionFields(projection string) ([]string, error) {
	switch strings.ToLower(strings.TrimSpace(projection)) {
	case "full":
		return nil, nil
	case "q1":
		return []string{"event"}, nil
	case "q2":
		return []string{"event", "did", "kind", "operation"}, nil
	case "q3":
		return []string{"event", "kind", "operation", "time_us"}, nil
	case "qexpr":
		return []string{"time_us"}, nil
	case "q4", "q4a", "q4b", "q5", "minimal":
		return []string{"event", "did", "kind", "operation", "time_us"}, nil
	default:
		return nil, fmt.Errorf("unknown projection %q", projection)
	}
}

func collectionFormat(raw string) (collections.DocumentFormat, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "json":
		return collections.DocumentFormatJSON, nil
	case "template-v1":
		return collections.DocumentFormatTemplateV1, nil
	default:
		return "", fmt.Errorf("unsupported -format %q", raw)
	}
}

func rootStoragePolicy(raw string) (collections.RootStoragePolicy, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "fast":
		return collections.RootStorageFast, nil
	case "compressed":
		return collections.RootStorageCompressed, nil
	default:
		return "", fmt.Errorf("unsupported -data-root %q", raw)
	}
}

func treeDBDataShape(cfg runConfig) string {
	switch {
	case isFullDataColumnStoreLayout(cfg.StorageLayout):
		return "full-retained-json"
	case isColumnStoreLayout(cfg.StorageLayout):
		return "query-shaped-projection"
	case cfg.Projection == "full":
		return "full-json"
	default:
		return "projected-json"
	}
}

func columnStoreRetainedPayloadPolicy(cfg runConfig) string {
	if !isColumnStoreLayout(cfg.StorageLayout) {
		return ""
	}
	if isFullDataColumnStoreLayout(cfg.StorageLayout) {
		return string(collections.ColumnRetainedPayloadNonColumn)
	}
	return string(collections.ColumnRetainedPayloadNone)
}

func columnStoreRetainedPayloadEncodingStatus(cfg runConfig) (string, string) {
	if !isColumnStoreLayout(cfg.StorageLayout) {
		return "", ""
	}
	columnStore, err := columnStoreConfigForProjection(cfg.Projection, cfg.StorageLayout, cfg.RetainedPayloadEncoding)
	if err != nil {
		return "", ""
	}
	return collections.ColumnRetainedPayloadEncodingStatus(columnStore)
}

func columnStoreReconstructionPolicy(cfg runConfig) string {
	if !isColumnStoreLayout(cfg.StorageLayout) {
		return ""
	}
	return string(collections.ColumnReconstructionRetainedPayloadAndColumns)
}

func columnStoreTypedColumnOwner(cfg runConfig) string {
	if !isColumnStoreLayout(cfg.StorageLayout) {
		return ""
	}
	columnStore, err := columnStoreConfigForProjection(cfg.Projection, cfg.StorageLayout, cfg.RetainedPayloadEncoding)
	if err == nil && len(columnStore.Columns) > 0 {
		owner := columnStore.Columns[0].Owner
		mixed := false
		for _, col := range columnStore.Columns[1:] {
			if col.Owner != owner {
				mixed = true
				break
			}
		}
		if !mixed {
			return string(owner)
		}
		return "mixed"
	}
	return string(collections.TypedStorageOwnerRowAsset)
}

func parseProfile(raw string) (treedb.Profile, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "fast", "production_fast", "backend_direct_fast", "backend_direct", "cached":
		return treedb.ProfileFast, nil
	case "wal_on_fast", "production_wal_on_fast", "backend_direct_wal_on_fast":
		return treedb.ProfileWALOnFast, nil
	case "durable":
		return treedb.ProfileDurable, nil
	case "bench":
		return treedb.ProfileBench, nil
	default:
		return "", fmt.Errorf("unsupported -profile %q", raw)
	}
}

func defaultRowsForScale(scale string) (int, error) {
	switch scale {
	case "", "subset", "smoke":
		return 10000, nil
	case "1m":
		return 1_000_000, nil
	case "10m":
		return 10_000_000, nil
	case "100m":
		return 100_000_000, nil
	case "1000m", "1b":
		return 1_000_000_000, nil
	default:
		return 0, fmt.Errorf("unsupported -scale %q", scale)
	}
}

func defaultFilesForScale(scale string) int {
	switch scale {
	case "10m":
		return 10
	case "100m":
		return 100
	case "1000m", "1b":
		return 1000
	default:
		return 1
	}
}

func inputFiles(dir string, maxFiles int) ([]string, error) {
	var files []string
	for _, pattern := range []string{"*.json.gz", "*.json"} {
		matches, err := filepath.Glob(filepath.Join(dir, pattern))
		if err != nil {
			return nil, err
		}
		files = append(files, matches...)
	}
	sort.Strings(files)
	if len(files) == 0 {
		return nil, fmt.Errorf("no .json.gz or .json files found in %s", dir)
	}
	if maxFiles > 0 && len(files) > maxFiles {
		files = files[:maxFiles]
	}
	return files, nil
}

func expandPath(path string) (string, error) {
	if path == "" {
		return "", nil
	}
	if path == "~" || strings.HasPrefix(path, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		if path == "~" {
			return home, nil
		}
		path = filepath.Join(home, path[2:])
	}
	return filepath.Abs(path)
}

func directoryUsage(dir string, rows int) (storageResult, error) {
	var out storageResult
	out.AccountingScope = "treedb_db_directory_durable_files"
	categories := make(map[string]*storageCategoryResult)
	err := filepath.WalkDir(dir, func(filePath string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		size := info.Size()
		out.GrossBytes += size
		rel, err := filepath.Rel(dir, filePath)
		if err != nil {
			return err
		}
		cleanRel := filepath.ToSlash(filepath.Clean(rel))
		base := strings.ToLower(stdpath.Base(cleanRel))
		parent := strings.ToLower(stdpath.Base(stdpath.Dir(cleanRel)))
		category, included := classifyTreeDBStorageFile(rel)
		bucket := categories[category]
		if bucket == nil {
			bucket = &storageCategoryResult{Category: category, Included: included}
			categories[category] = bucket
		}
		bucket.Bytes += size
		bucket.FileCount++
		if included && info.Mode().IsRegular() && category == "wal" && parent == "wal" && isTreeDBCommandWALSegmentName(base) {
			out.WALBytesExcludedFromDurable += size
		}
		if included {
			out.TotalBytes += size
			out.FileCount++
		} else {
			out.ExcludedBytes += size
			out.ExcludedFileCount++
		}
		return nil
	})
	if err != nil {
		return out, err
	}
	out.DurableStorageBytesWALExcluded = durableStorageBytesWALExcluded(out.TotalBytes, out.WALBytesExcludedFromDurable)
	out.DurableStorageBytesWALExcludedNote = "durable_storage_bytes_wal_excluded subtracts only valid regular wal/commit-l<lane>-<seq>.log command WAL segment files; value_vlog, leaf_vlog, index.db, column assets, manifest/control bytes remain counted."
	if rows > 0 {
		out.BytesPerRow = float64(out.TotalBytes) / float64(rows)
	}
	out.Categories = storageCategoriesForResult(categories, rows)
	return out, nil
}

func collectTreeDBStorageUsage(ctx context.Context, dir string, collection *collections.Collection, cfg runConfig, rows int, phase string) (storageResult, error) {
	storage, err := directoryUsage(dir, rows)
	if err != nil {
		return storage, err
	}
	storage.MeasurementPhase = phase
	if !isColumnStoreLayout(cfg.StorageLayout) || collection == nil {
		return storage, nil
	}
	physical, err := collection.ColumnStorePhysicalAccounting(ctx, collections.ColumnStorePhysicalAccountingOptions{
		DetailedSections: true,
		ReadIntegrity:    collections.ColumnAssetReadIntegrityVerify,
	})
	if err != nil {
		return storage, fmt.Errorf("collect column-store physical accounting: %w", err)
	}
	storage.ColumnStorePhysical = &physical
	reachability, err := collection.PlanColumnAssetReachability(ctx, collections.ColumnAssetReachabilityOptions{
		SegmentDetails: true,
	})
	if err != nil {
		return storage, fmt.Errorf("collect column asset reachability accounting: %w", err)
	}
	storage.ColumnAssetReachability = &reachability
	return storage, nil
}

func durableStorageBytesWALExcluded(totalBytes, walBytes int64) int64 {
	if totalBytes <= 0 || walBytes >= totalBytes {
		return 0
	}
	if walBytes <= 0 {
		return totalBytes
	}
	return totalBytes - walBytes
}

func isTreeDBCommandWALSegmentName(name string) bool {
	if !strings.HasPrefix(name, "commit-l") || !strings.HasSuffix(name, ".log") {
		return false
	}
	body := strings.TrimSuffix(strings.TrimPrefix(name, "commit-l"), ".log")
	parts := strings.SplitN(body, "-", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return false
	}
	if _, err := strconv.ParseUint(parts[0], 10, 32); err != nil {
		return false
	}
	seq, err := strconv.ParseUint(parts[1], 10, 64)
	return err == nil && seq != 0
}

func classifyTreeDBStorageFile(rel string) (string, bool) {
	rel = filepath.ToSlash(filepath.Clean(rel))
	lowerRel := strings.ToLower(rel)
	base := strings.ToLower(stdpath.Base(lowerRel))
	if isTreeDBStorageRuntimeLock(lowerRel, base) {
		return "runtime_locks", false
	}
	if isTreeDBStorageProfileArtifact(lowerRel, base) {
		return "profile_artifacts", false
	}
	if isTreeDBStorageTransientTemp(lowerRel, base) {
		return "temp_transient", false
	}
	switch {
	case rel == "index.db" || rel == "maindb/index.db":
		return "primary_index", true
	case rel == "dictdb/index.db":
		return "dictionary_index", true
	case strings.Contains(lowerRel, "/column_assets/") || strings.HasPrefix(lowerRel, "column_assets/"):
		return classifyTreeDBColumnAssetFile(lowerRel), true
	case strings.Contains(lowerRel, "/leaf_vlog/") || strings.HasPrefix(lowerRel, "leaf_vlog/"):
		return "leaf_vlog", true
	case strings.Contains(lowerRel, "/value_vlog/") || strings.HasPrefix(lowerRel, "value_vlog/"):
		return "value_vlog", true
	case strings.Contains(lowerRel, "/wal/") || strings.HasPrefix(lowerRel, "wal/") || strings.HasSuffix(lowerRel, ".wal"):
		return "wal", true
	case base == "format.json" || base == "format_config.json":
		if strings.HasPrefix(lowerRel, "dictdb/") {
			return "dictionary_db_metadata", true
		}
		return "format_metadata", true
	case base == "vlog_ref_counts.meta":
		return "refcount_metadata", true
	case strings.HasPrefix(lowerRel, "dictdb/"):
		return "dictionary_db_metadata", true
	default:
		return "other", true
	}
}

func classifyTreeDBColumnAssetFile(lowerRel string) string {
	switch {
	case strings.Contains(lowerRel, "/assets/segments/"):
		return "column_asset_segments"
	case strings.Contains(lowerRel, "/assets/indexes/"):
		return "column_asset_indexes"
	case strings.Contains(lowerRel, "/quarantine/"):
		return "column_asset_quarantine"
	default:
		return "column_asset_metadata"
	}
}

func isTreeDBStorageRuntimeLock(lowerRel, base string) bool {
	if base == "lock" || base == "lockfile" || strings.HasSuffix(base, ".lock") || strings.HasSuffix(base, "-owner.lock") {
		return true
	}
	return strings.HasSuffix(lowerRel, "/lock")
}

func isTreeDBStorageProfileArtifact(lowerRel, base string) bool {
	switch base {
	case "trace.out", "benchprof_results.json", "benchprof_results.md":
		return true
	}
	return strings.HasSuffix(lowerRel, ".pprof")
}

func isTreeDBStorageTransientTemp(lowerRel, base string) bool {
	if strings.Contains(lowerRel, "/tmp/") || strings.Contains(lowerRel, "/temp/") {
		return true
	}
	return strings.HasSuffix(base, ".tmp") ||
		strings.HasSuffix(base, ".partial") ||
		strings.HasSuffix(base, ".part") ||
		strings.HasPrefix(base, ".tmp")
}

func storageCategoriesForResult(categories map[string]*storageCategoryResult, rows int) []storageCategoryResult {
	if len(categories) == 0 {
		return nil
	}
	out := make([]storageCategoryResult, 0, len(categories))
	for _, category := range categories {
		item := *category
		if rows > 0 {
			item.BytesPerRow = float64(item.Bytes) / float64(rows)
		}
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Included != b.Included {
			return a.Included
		}
		if rankA, rankB := storageCategoryRank(a.Category), storageCategoryRank(b.Category); rankA != rankB {
			return rankA < rankB
		}
		if a.Bytes != b.Bytes {
			return a.Bytes > b.Bytes
		}
		return a.Category < b.Category
	})
	return out
}

func storageCategoryRank(category string) int {
	switch category {
	case "primary_index":
		return 10
	case "dictionary_index":
		return 20
	case "column_asset_segments":
		return 30
	case "column_asset_indexes":
		return 31
	case "column_asset_metadata":
		return 32
	case "column_asset_quarantine":
		return 33
	case "leaf_vlog":
		return 40
	case "value_vlog":
		return 41
	case "wal":
		return 50
	case "format_metadata":
		return 60
	case "refcount_metadata":
		return 61
	case "dictionary_db_metadata":
		return 62
	case "other":
		return 90
	case "runtime_locks":
		return 100
	case "profile_artifacts":
		return 101
	case "temp_transient":
		return 102
	default:
		return 200
	}
}

func documentID(row uint64) []byte {
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, row)
	return out
}

type canonicalJSONHasher struct {
	h    hash.Hash
	rows int
}

func newCanonicalJSONHasher() *canonicalJSONHasher {
	return &canonicalJSONHasher{h: sha256.New()}
}

func (h *canonicalJSONHasher) Add(raw []byte) error {
	canonical, err := canonicalJSON(raw)
	if err != nil {
		return err
	}
	var length [8]byte
	binary.LittleEndian.PutUint64(length[:], uint64(len(canonical)))
	if _, err := h.h.Write(length[:]); err != nil {
		return err
	}
	if _, err := h.h.Write(canonical); err != nil {
		return err
	}
	h.rows++
	return nil
}

func (h *canonicalJSONHasher) Rows() int {
	if h == nil {
		return 0
	}
	return h.rows
}

func (h *canonicalJSONHasher) Sum() string {
	if h == nil {
		return ""
	}
	return hex.EncodeToString(h.h.Sum(nil))
}

func canonicalJSON(raw []byte) ([]byte, error) {
	var value any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&value); err != nil {
		return nil, err
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, errors.New("invalid JSON: multiple top-level values")
		}
		return nil, err
	}
	return json.Marshal(value)
}

func jsonInt64(value gjson.Result) int64 {
	if !value.Exists() {
		return 0
	}
	if value.Type == gjson.Number {
		return value.Int()
	}
	n, _ := strconv.ParseInt(value.String(), 10, 64)
	return n
}

func hashRows(rows any) (string, error) {
	raw, err := json.Marshal(rows)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func seconds(d time.Duration) float64 {
	return d.Seconds()
}
