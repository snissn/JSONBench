package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const schemaVersion = "jsonbench-raw-treedb/v1"

type config struct {
	DataDir                string
	DBDir                  string
	Out                    string
	Rows                   int
	MaxFiles               int
	BatchSize              int
	Profile                string
	PointerThreshold       int
	ForcePointers          bool
	Compact                bool
	CompactBatchSize       int
	CompactMaxSegmentBytes int64
	Reset                  bool
	Progress               bool
}

type result struct {
	SchemaVersion          string            `json:"schema_version"`
	GeneratedAt            string            `json:"generated_at"`
	System                 string            `json:"system"`
	Engine                 string            `json:"engine"`
	Profile                string            `json:"profile"`
	RowsRequested          int               `json:"rows_requested"`
	RowsLoaded             int               `json:"rows_loaded"`
	DataDir                string            `json:"data_dir"`
	DBDir                  string            `json:"db_dir"`
	Files                  []string          `json:"files"`
	BatchSize              int               `json:"batch_size"`
	PointerThreshold       int               `json:"pointer_threshold"`
	ForcePointers          bool              `json:"force_pointers"`
	CompactMaxSegmentBytes int64             `json:"compact_max_segment_bytes,omitempty"`
	Load                   loadResult        `json:"load"`
	Storage                []storageSnapshot `json:"storage"`
	Compaction             *compactionResult `json:"compaction,omitempty"`
	Baselines              []baseline        `json:"baselines"`
}

type loadResult struct {
	Rows              int     `json:"rows"`
	Batches           int     `json:"batches"`
	WallSec           float64 `json:"wall_seconds"`
	RowsPerSec        float64 `json:"rows_per_second"`
	RawValueBytes     int64   `json:"raw_value_bytes"`
	InputBytesRead    int64   `json:"input_bytes_read"`
	CompressedBytes   int64   `json:"compressed_bytes"`
	CheckpointSec     float64 `json:"checkpoint_seconds"`
	CheckpointWallSec float64 `json:"checkpoint_wall_seconds"`
}

type storageSnapshot struct {
	Label       string  `json:"label"`
	TotalBytes  int64   `json:"total_bytes"`
	MiB         float64 `json:"mib"`
	BytesPerRow float64 `json:"bytes_per_row,omitempty"`
	FileCount   int     `json:"file_count"`
}

type compactionResult struct {
	Enabled                 bool                                     `json:"enabled"`
	Mode                    string                                   `json:"mode"`
	WallSec                 float64                                  `json:"wall_seconds"`
	CompactStorageStats     backenddb.CompactStorageStats            `json:"compact_storage_stats"`
	ValueLogRewriteSec      float64                                  `json:"value_log_rewrite_seconds"`
	ValueLogRewriteStats    backenddb.ValueLogRewriteStats           `json:"value_log_rewrite_stats"`
	ValueLogGCSec           float64                                  `json:"value_log_gc_seconds"`
	ValueLogGCStats         backenddb.ValueLogGCStats                `json:"value_log_gc_stats"`
	LeafGenerationPackSec   float64                                  `json:"leaf_generation_pack_seconds"`
	LeafGenerationPackStats backenddb.LeafGenerationPackRunOnceStats `json:"leaf_generation_pack_stats"`
	LeafGenerationGCSec     float64                                  `json:"leaf_generation_gc_seconds"`
	LeafGenerationGCStats   backenddb.LeafGenerationGCStats          `json:"leaf_generation_gc_stats"`
	IndexVacuumSec          float64                                  `json:"index_vacuum_seconds"`
	CheckpointSec           float64                                  `json:"checkpoint_seconds"`
}

type baseline struct {
	System string  `json:"system"`
	MiB    float64 `json:"mib"`
	Source string  `json:"source"`
}

type scanStats struct {
	ReadBytes       int64
	CompressedBytes int64
}

var errStopScan = errors.New("stop scan")

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "jsonbench_raw_treedb: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := parseFlags(os.Args[1:])
	if err != nil {
		return err
	}
	res, err := runExperiment(cfg)
	if err != nil {
		return err
	}
	if err := writeResult(cfg.Out, res); err != nil {
		return err
	}
	printSummary(res)
	return nil
}

func parseFlags(args []string) (config, error) {
	cfg := config{
		DataDir:          "~/data/bluesky",
		Rows:             1_000_000,
		MaxFiles:         1,
		BatchSize:        16_000,
		Profile:          "fast",
		PointerThreshold: 1,
		ForcePointers:    true,
		Compact:          true,
		CompactBatchSize: 16_000,
	}
	fs := flag.NewFlagSet("jsonbench_raw_treedb", flag.ContinueOnError)
	fs.StringVar(&cfg.DataDir, "data-dir", cfg.DataDir, "Directory containing JSONBench file_*.json.gz or .json files")
	fs.StringVar(&cfg.DBDir, "db-dir", cfg.DBDir, "TreeDB directory; defaults to a timestamped directory under treedb/results")
	fs.StringVar(&cfg.Out, "out", cfg.Out, "Result JSON path; defaults to <db-dir>/result.json")
	fs.IntVar(&cfg.Rows, "rows", cfg.Rows, "Exact number of rows to load")
	fs.IntVar(&cfg.MaxFiles, "max-files", cfg.MaxFiles, "Maximum input files to scan")
	fs.IntVar(&cfg.BatchSize, "batch-size", cfg.BatchSize, "Raw TreeDB batch size")
	fs.StringVar(&cfg.Profile, "profile", cfg.Profile, "TreeDB profile: fast, wal_on_fast, durable, bench")
	fs.IntVar(&cfg.PointerThreshold, "pointer-threshold", cfg.PointerThreshold, "TreeDB value-log pointer threshold; 1 stores JSON values out-of-line")
	fs.BoolVar(&cfg.ForcePointers, "force-pointers", cfg.ForcePointers, "Force all values into the value log")
	fs.BoolVar(&cfg.Compact, "compact", cfg.Compact, "Run full TreeDB CompactStorage after loading")
	fs.IntVar(&cfg.CompactBatchSize, "compact-batch-size", cfg.CompactBatchSize, "Value-log rewrite pointer-swap batch size")
	fs.Int64Var(&cfg.CompactMaxSegmentBytes, "compact-max-segment-bytes", cfg.CompactMaxSegmentBytes, "Maximum value-log segment bytes during rewrite; 0 uses TreeDB default")
	fs.BoolVar(&cfg.Reset, "reset", cfg.Reset, "Remove -db-dir before loading")
	fs.BoolVar(&cfg.Progress, "progress", cfg.Progress, "Print load progress to stderr")
	if err := fs.Parse(args); err != nil {
		return cfg, err
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
	if cfg.PointerThreshold < 0 {
		return cfg, errors.New("-pointer-threshold must be >= 0")
	}
	profile, err := parseProfile(cfg.Profile)
	if err != nil {
		return cfg, err
	}
	cfg.Profile = string(profile)
	cfg.DataDir, err = expandPath(cfg.DataDir)
	if err != nil {
		return cfg, err
	}
	if cfg.DBDir == "" {
		stamp := time.Now().UTC().Format("20060102_150405")
		cfg.DBDir = filepath.Join("results", "raw_treedb_"+stamp, "db")
	}
	cfg.DBDir, err = expandPath(cfg.DBDir)
	if err != nil {
		return cfg, err
	}
	if cfg.Out == "" {
		cfg.Out = filepath.Join(filepath.Dir(cfg.DBDir), "result.json")
	}
	cfg.Out, err = expandPath(cfg.Out)
	if err != nil {
		return cfg, err
	}
	return cfg, nil
}

func runExperiment(cfg config) (result, error) {
	if cfg.Reset {
		if err := os.RemoveAll(cfg.DBDir); err != nil {
			return result{}, fmt.Errorf("reset db dir: %w", err)
		}
	}
	if err := os.MkdirAll(cfg.DBDir, 0o755); err != nil {
		return result{}, fmt.Errorf("create db dir: %w", err)
	}
	files, err := inputFiles(cfg.DataDir, cfg.MaxFiles)
	if err != nil {
		return result{}, err
	}

	profile, _ := parseProfile(cfg.Profile)
	opts := treedb.OptionsFor(profile, cfg.DBDir)
	opts.ValueLog.PointerThreshold = cfg.PointerThreshold
	opts.ValueLog.ForcePointers = cfg.ForcePointers

	db, err := treedb.Open(opts)
	if err != nil {
		return result{}, fmt.Errorf("open treedb: %w", err)
	}

	load, usedFiles, err := loadRawJSON(db, cfg, files)
	if err != nil {
		_ = db.Close()
		return result{}, err
	}
	if load.Rows != cfg.Rows {
		_ = db.Close()
		return result{}, fmt.Errorf("loaded %d rows, requested %d", load.Rows, cfg.Rows)
	}

	var snapshots []storageSnapshot
	checkpointStart := time.Now()
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		return result{}, fmt.Errorf("checkpoint after load: %w", err)
	}
	load.CheckpointSec = time.Since(checkpointStart).Seconds()
	load.CheckpointWallSec = load.CheckpointSec
	if err := db.Close(); err != nil {
		return result{}, fmt.Errorf("close after load checkpoint: %w", err)
	}
	snap, err := directoryUsage("after_load_checkpoint", cfg.DBDir, load.Rows)
	if err != nil {
		return result{}, err
	}
	snapshots = append(snapshots, snap)

	var compaction *compactionResult
	if cfg.Compact {
		comp, compactSnapshots, err := compactRaw(context.Background(), opts, cfg, load.Rows)
		if err != nil {
			return result{}, err
		}
		compaction = &comp
		snapshots = append(snapshots, compactSnapshots...)
	}

	return result{
		SchemaVersion:          schemaVersion,
		GeneratedAt:            time.Now().UTC().Format(time.RFC3339),
		System:                 "TreeDB",
		Engine:                 "raw-kv-json",
		Profile:                cfg.Profile,
		RowsRequested:          cfg.Rows,
		RowsLoaded:             load.Rows,
		DataDir:                cfg.DataDir,
		DBDir:                  cfg.DBDir,
		Files:                  usedFiles,
		BatchSize:              cfg.BatchSize,
		PointerThreshold:       cfg.PointerThreshold,
		ForcePointers:          cfg.ForcePointers,
		CompactMaxSegmentBytes: cfg.CompactMaxSegmentBytes,
		Load:                   load,
		Storage:                snapshots,
		Compaction:             compaction,
		Baselines: []baseline{
			{System: "ClickHouse json/full 1m", MiB: 97.07, Source: "fresh_1m_20260508_082835"},
			{System: "DuckDB json/full 1m", MiB: 462.75, Source: "fresh_1m_20260508_082835"},
		},
	}, nil
}

func loadRawJSON(db *treedb.DB, cfg config, files []string) (loadResult, []string, error) {
	var out loadResult
	usedFiles := make([]string, 0, len(files))
	batch := db.NewBatchWithSize(cfg.BatchSize)
	if batch == nil {
		return out, nil, errors.New("NewBatchWithSize returned nil")
	}
	defer func() { _ = batch.Close() }()
	pending := 0

	flush := func() error {
		if pending == 0 {
			return nil
		}
		if err := batch.Write(); err != nil {
			return err
		}
		if err := batch.Close(); err != nil {
			return err
		}
		out.Batches++
		batch = db.NewBatchWithSize(cfg.BatchSize)
		if batch == nil {
			return errors.New("NewBatchWithSize returned nil after flush")
		}
		pending = 0
		return nil
	}

	start := time.Now()
	lastProgress := time.Now()
	for _, path := range files {
		if out.Rows >= cfg.Rows {
			break
		}
		stats, err := scanInputFile(path, func(raw []byte) error {
			if out.Rows >= cfg.Rows {
				return errStopScan
			}
			key := documentID(uint64(out.Rows + 1))
			if err := batch.Set(key, raw); err != nil {
				return err
			}
			out.Rows++
			pending++
			out.RawValueBytes += int64(len(raw))
			if pending >= cfg.BatchSize {
				if err := flush(); err != nil {
					return err
				}
			}
			if cfg.Progress && time.Since(lastProgress) >= time.Second {
				fmt.Fprintf(os.Stderr, "loaded %d/%d raw JSON rows into %s\n", out.Rows, cfg.Rows, cfg.DBDir)
				lastProgress = time.Now()
			}
			return nil
		})
		if err != nil {
			return out, usedFiles, fmt.Errorf("read %s: %w", path, err)
		}
		usedFiles = append(usedFiles, path)
		out.InputBytesRead += stats.ReadBytes
		out.CompressedBytes += stats.CompressedBytes
	}
	if err := flush(); err != nil {
		return out, usedFiles, err
	}
	out.WallSec = time.Since(start).Seconds()
	if out.WallSec > 0 {
		out.RowsPerSec = float64(out.Rows) / out.WallSec
	}
	return out, usedFiles, nil
}

func compactRaw(ctx context.Context, opts treedb.Options, cfg config, rows int) (compactionResult, []storageSnapshot, error) {
	var out compactionResult
	out.Enabled = true
	out.Mode = "backend-compact-storage-full"
	var snapshots []storageSnapshot
	wallStart := time.Now()

	db, cleanup, err := treedb.OpenBackend(opts)
	if err != nil {
		return out, snapshots, fmt.Errorf("open backend for compaction: %w", err)
	}
	defer func() {
		_ = cleanup()
	}()

	appendSnapshot := func(label string) error {
		snap, err := directoryUsage(label, cfg.DBDir, rows)
		if err != nil {
			return err
		}
		snapshots = append(snapshots, snap)
		return nil
	}

	if err := appendSnapshot("before_compact_storage"); err != nil {
		return out, snapshots, err
	}
	stats, err := db.CompactStorage(ctx, backenddb.CompactStorageOptions{
		SyncEachPhase:                   true,
		ValueLogRewriteBatchSize:        cfg.CompactBatchSize,
		ValueLogRewriteMaxSegmentBytes:  cfg.CompactMaxSegmentBytes,
		LeafPackMinExpectedReclaimBytes: 1,
		LeafPackMinReclaimPerCopyPPM:    1,
	})
	if err != nil {
		return out, snapshots, fmt.Errorf("compact storage: %w", err)
	}
	if !stats.FullyCompacted {
		return out, snapshots, fmt.Errorf("compact storage left remaining debt: %+v", stats.RemainingDebt)
	}
	out.CompactStorageStats = stats
	out.ValueLogRewriteStats = stats.ValueLogRewrite
	out.ValueLogGCStats = stats.ValueLogGC
	if len(stats.LeafGenerationPacks) > 0 {
		out.LeafGenerationPackStats = stats.LeafGenerationPacks[0]
	}
	out.LeafGenerationGCStats = stats.LeafGenerationGC
	if err := appendSnapshot("after_compact_storage"); err != nil {
		return out, snapshots, err
	}

	out.WallSec = time.Since(wallStart).Seconds()
	return out, snapshots, nil
}

func inputFiles(dir string, maxFiles int) ([]string, error) {
	matches, err := filepath.Glob(filepath.Join(dir, "file_*.json.gz"))
	if err != nil {
		return nil, err
	}
	files := append([]string(nil), matches...)
	matches, err = filepath.Glob(filepath.Join(dir, "file_*.json"))
	if err != nil {
		return nil, err
	}
	files = append(files, matches...)
	sort.Strings(files)
	if len(files) == 0 {
		return nil, fmt.Errorf("no file_*.json.gz or file_*.json files found in %s", dir)
	}
	if maxFiles > 0 && len(files) > maxFiles {
		files = files[:maxFiles]
	}
	return files, nil
}

func scanInputFile(path string, fn func(raw []byte) error) (scanStats, error) {
	var out scanStats
	if stat, err := os.Stat(path); err == nil {
		out.CompressedBytes = stat.Size()
	}
	file, err := os.Open(path)
	if err != nil {
		return out, err
	}
	defer func() { _ = file.Close() }()
	var reader io.Reader = file
	var gz *gzip.Reader
	if strings.HasSuffix(path, ".gz") {
		gz, err = gzip.NewReader(file)
		if err != nil {
			return out, err
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
				out.ReadBytes = counting.n
				return out, nil
			}
			out.ReadBytes = counting.n
			return out, err
		}
	}
	if err := scanner.Err(); err != nil {
		out.ReadBytes = counting.n
		return out, err
	}
	out.ReadBytes = counting.n
	return out, nil
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

func directoryUsage(label, dir string, rows int) (storageSnapshot, error) {
	var out storageSnapshot
	out.Label = label
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
	out.MiB = float64(out.TotalBytes) / (1024 * 1024)
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

func parseProfile(raw string) (treedb.Profile, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "fast":
		return treedb.ProfileFast, nil
	case "wal_on_fast":
		return treedb.ProfileWALOnFast, nil
	case "durable":
		return treedb.ProfileDurable, nil
	case "bench":
		return treedb.ProfileBench, nil
	default:
		return "", fmt.Errorf("unsupported -profile %q", raw)
	}
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

func writeResult(path string, res result) error {
	data, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func printSummary(res result) {
	fmt.Printf("raw TreeDB JSON load: rows=%d profile=%s pointer_threshold=%d force_pointers=%t\n",
		res.RowsLoaded, res.Profile, res.PointerThreshold, res.ForcePointers)
	fmt.Printf("raw JSON bytes: %.2f MiB; load %.3fs; checkpoint %.3fs\n",
		float64(res.Load.RawValueBytes)/(1024*1024), res.Load.WallSec, res.Load.CheckpointSec)
	for _, snap := range res.Storage {
		fmt.Printf("%-30s %10.2f MiB  (%d bytes, %.2f bytes/row, %d files)\n",
			snap.Label+":", snap.MiB, snap.TotalBytes, snap.BytesPerRow, snap.FileCount)
	}
	for _, base := range res.Baselines {
		fmt.Printf("%-30s %10.2f MiB  (%s)\n", base.System+":", base.MiB, base.Source)
	}
	fmt.Printf("result: %s\n", res.DBDir)
}
