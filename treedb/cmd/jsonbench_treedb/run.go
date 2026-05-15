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
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/tidwall/gjson"
)

const (
	defaultCollectionName = "bluesky"
	defaultBatchSize      = 16000
)

type runConfig struct {
	DataDir          string
	DBDir            string
	Out              string
	Reset            bool
	Scale            string
	Rows             int
	MaxFiles         int
	Format           string
	Projection       string
	Queries          []string
	BatchSize        int
	Profile          string
	DataRoot         string
	Collection       string
	Checkpoint       bool
	CompactAfterLoad bool
	CompactBatchSize int
	Tries            int
	Progress         bool
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
	AllowShortData bool              `json:"allow_short_data,omitempty"`
	DatasetSize    int               `json:"dataset_size"`
	DataDir        string            `json:"data_dir"`
	DBDir          string            `json:"db_dir"`
	Collection     string            `json:"collection"`
	Format         string            `json:"format"`
	Projection     string            `json:"projection"`
	RetainsJSON    bool              `json:"retains_json_structure"`
	Profile        string            `json:"profile"`
	DataRoot       string            `json:"data_root"`
	Load           loadResult        `json:"load"`
	Storage        storageResult     `json:"storage"`
	Compaction     *compactionResult `json:"compaction,omitempty"`
	Queries        []queryRun        `json:"queries"`
	Command        []string          `json:"command,omitempty"`
	Notes          []string          `json:"notes,omitempty"`
}

type loadResult struct {
	Rows            int      `json:"rows"`
	Files           []string `json:"files"`
	Batches         int      `json:"batches"`
	GenerationSec   float64  `json:"generation_seconds"`
	InsertSec       float64  `json:"insert_seconds"`
	FlushSec        float64  `json:"flush_seconds"`
	CheckpointSec   float64  `json:"checkpoint_seconds,omitempty"`
	WallSec         float64  `json:"wall_seconds"`
	RowsPerSec      float64  `json:"rows_per_second"`
	BytesRead       int64    `json:"bytes_read"`
	CompressedBytes int64    `json:"compressed_bytes"`
}

type storageResult struct {
	TotalBytes  int64   `json:"total_bytes"`
	BytesPerRow float64 `json:"bytes_per_row,omitempty"`
	FileCount   int     `json:"file_count"`
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

type queryRun struct {
	Name        string     `json:"name"`
	SQL         string     `json:"sql"`
	AttemptsSec []float64  `json:"attempts_seconds"`
	BestSec     float64    `json:"best_seconds"`
	MedianSec   float64    `json:"median_seconds"`
	RowsScanned int        `json:"rows_scanned"`
	ResultRows  int        `json:"result_rows"`
	ResultHash  string     `json:"result_hash"`
	Preview     []queryRow `json:"preview,omitempty"`
}

type queryRow map[string]any

func parseRunFlags(args []string) (runConfig, error) {
	cfg := runConfig{
		DataDir:          "~/data/bluesky",
		Scale:            "subset",
		Format:           "json",
		Projection:       "full",
		BatchSize:        defaultBatchSize,
		Profile:          "fast",
		DataRoot:         "fast",
		Collection:       defaultCollectionName,
		Checkpoint:       true,
		CompactBatchSize: defaultBatchSize,
		Tries:            3,
		Queries:          []string{"q1", "q2", "q3", "q4", "q5"},
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
	fs.StringVar(&cfg.Projection, "projection", cfg.Projection, "Projection: full, minimal, q1, q2, q3, q4, q5")
	fs.StringVar(&queryList, "queries", "all", "Comma-separated query names: all, q1, q2, q3, q4, q5")
	fs.IntVar(&cfg.BatchSize, "batch-size", cfg.BatchSize, "Documents per InsertBatch")
	fs.StringVar(&cfg.Profile, "profile", cfg.Profile, "TreeDB profile: fast, wal_on_fast, durable, bench")
	fs.StringVar(&cfg.DataRoot, "data-root", cfg.DataRoot, "Collection data root storage: fast or compressed")
	fs.StringVar(&cfg.Collection, "collection", cfg.Collection, "Collection name")
	fs.BoolVar(&cfg.Checkpoint, "checkpoint", cfg.Checkpoint, "Checkpoint after loading")
	fs.BoolVar(&cfg.CompactAfterLoad, "compact-after-load", false, "Run full TreeDB maintenance compaction after loading and before query timing")
	fs.IntVar(&cfg.CompactBatchSize, "compact-batch-size", cfg.CompactBatchSize, "Value-log rewrite pointer-swap batch size for -compact-after-load")
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
	cfg.Projection = strings.ToLower(strings.TrimSpace(cfg.Projection))
	cfg.Profile = strings.ToLower(strings.TrimSpace(cfg.Profile))
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
	if cfg.CompactBatchSize <= 0 {
		return cfg, errors.New("-compact-batch-size must be positive")
	}
	if cfg.Tries <= 0 {
		return cfg, errors.New("-tries must be positive")
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
	if strings.TrimSpace(cfg.Collection) == "" {
		return cfg, errors.New("-collection cannot be empty")
	}
	return cfg, nil
}

func runTreeDBBenchmark(cfg runConfig) (runResult, error) {
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
	if cfg.Reset {
		if err := os.RemoveAll(cfg.DBDir); err != nil {
			return runResult{}, fmt.Errorf("reset db dir: %w", err)
		}
	}
	if err := os.MkdirAll(cfg.DBDir, 0o755); err != nil {
		return runResult{}, fmt.Errorf("create db dir: %w", err)
	}

	backend, cleanup, err := openBackend(cfg)
	if err != nil {
		return runResult{}, err
	}
	defer func() { _ = cleanup() }()

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
	if load.Rows != cfg.Rows {
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
	storage, err := directoryUsage(cfg.DBDir, load.Rows)
	if err != nil {
		return runResult{}, err
	}
	queryResults, err := runQueries(collection, cfg, load.Rows)
	if err != nil {
		return runResult{}, err
	}
	return runResult{
		SchemaVersion: schemaVersion,
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
		System:        "TreeDB",
		Engine:        "treedb-collections-direct-go",
		Scale:         cfg.Scale,
		ScaleLabel:    scaleLabel(cfg.Scale, cfg.Rows, load.Rows),
		RequestedRows: cfg.Rows,
		DatasetSize:   load.Rows,
		DataDir:       cfg.DataDir,
		DBDir:         cfg.DBDir,
		Collection:    cfg.Collection,
		Format:        cfg.Format,
		Projection:    cfg.Projection,
		RetainsJSON:   cfg.Projection == "full",
		Profile:       cfg.Profile,
		DataRoot:      cfg.DataRoot,
		Load:          load,
		Storage:       storage,
		Compaction:    compaction,
		Queries:       queryResults,
	}, nil
}

func compactLoadedData(ctx context.Context, collection *collections.Collection, _ *backenddb.DB, cfg runConfig, rows int) (compactionResult, error) {
	out := compactionResult{Enabled: true}
	wallStart := time.Now()

	storageBefore, err := directoryUsage(cfg.DBDir, rows)
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

	storageAfter, err := directoryUsage(cfg.DBDir, rows)
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
	_, err = manager.CreateCollection(&collections.CollectionMeta{
		Name: cfg.Collection,
		Options: collections.CollectionOptions{
			DocumentFormat:          format,
			DataRootStoragePolicy:   policy,
			IndexStateStoragePolicy: policy,
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

func loadData(collection *collections.Collection, backend *backenddb.DB, cfg runConfig, files []string) (loadResult, error) {
	format, err := collectionFormat(cfg.Format)
	if err != nil {
		return loadResult{}, err
	}
	var encoder collections.TemplateV1Encoder
	ids := make([][]byte, 0, cfg.BatchSize)
	docs := make([][]byte, 0, cfg.BatchSize)
	out := loadResult{Files: make([]string, 0, len(files))}
	var batches int
	var generationElapsed time.Duration
	var insertElapsed time.Duration
	var flushElapsed time.Duration
	var checkpointElapsed time.Duration
	wallStart := time.Now()
	lastProgress := time.Now()

	flushBatch := func() error {
		if len(ids) == 0 {
			return nil
		}
		start := time.Now()
		if _, err := collection.InsertBatch(ids, docs); err != nil {
			return err
		}
		insertElapsed += time.Since(start)
		batches++
		ids = ids[:0]
		docs = docs[:0]
		return nil
	}

	for _, path := range files {
		if out.Rows >= cfg.Rows {
			break
		}
		readBytes, compressedBytes, err := scanInputFile(path, func(raw []byte) error {
			if out.Rows >= cfg.Rows {
				return errStopScan
			}
			genStart := time.Now()
			doc, err := buildDocument(raw, format, cfg.Projection, &encoder)
			if err != nil {
				return err
			}
			id := documentID(uint64(out.Rows + 1))
			generationElapsed += time.Since(genStart)
			ids = append(ids, id)
			docs = append(docs, doc)
			out.Rows++
			if len(ids) >= cfg.BatchSize {
				if err := flushBatch(); err != nil {
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
			return loadResult{}, fmt.Errorf("read %s: %w", path, err)
		}
		out.Files = append(out.Files, path)
		out.BytesRead += readBytes
		out.CompressedBytes += compressedBytes
	}
	if err := flushBatch(); err != nil {
		return loadResult{}, err
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
	out.Batches = batches
	out.GenerationSec = generationElapsed.Seconds()
	out.InsertSec = insertElapsed.Seconds()
	out.FlushSec = flushElapsed.Seconds()
	out.CheckpointSec = checkpointElapsed.Seconds()
	out.WallSec = wallElapsed.Seconds()
	if out.Rows > 0 && wallElapsed > 0 {
		out.RowsPerSec = float64(out.Rows) / wallElapsed.Seconds()
	}
	return out, nil
}

var errStopScan = errors.New("stop scan")

func scanInputFile(path string, fn func(raw []byte) error) (readBytes int64, compressedBytes int64, err error) {
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
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024*1024)
	for scanner.Scan() {
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
		return counting.n, compressedBytes, err
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

func buildDocument(raw []byte, format collections.DocumentFormat, projection string, encoder *collections.TemplateV1Encoder) ([]byte, error) {
	if projection == "full" {
		if format == collections.DocumentFormatTemplateV1 {
			return collections.EncodeTemplateV1DocumentJSON(raw)
		}
		return bytes.Clone(raw), nil
	}
	fields, err := projectionFields(projection)
	if err != nil {
		return nil, err
	}
	extracted := extractFullJSONFields(raw, fields)
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
		return []string{"q1", "q2", "q3", "q4", "q5"}, nil
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
	case "q4", "q5", "minimal":
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
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
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
		out.TotalBytes += info.Size()
		out.FileCount++
		return nil
	})
	if err != nil {
		return out, err
	}
	if rows > 0 {
		out.BytesPerRow = float64(out.TotalBytes) / float64(rows)
	}
	return out, nil
}

func documentID(row uint64) []byte {
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, row)
	return out
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
