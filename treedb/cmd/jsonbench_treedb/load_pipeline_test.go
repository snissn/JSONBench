package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestRunPreparedLoadPipelinePreparesNextBatchWhileInsertBlocked(t *testing.T) {
	insertStarted := make(chan struct{})
	preparedSecond := make(chan struct{})
	releaseInsert := make(chan struct{})
	done := make(chan struct{})

	var (
		stats loadPipelineStats
		err   error
	)
	go func() {
		defer close(done)
		stats, err = runPreparedLoadPipeline(
			context.Background(),
			1,
			func(ctx context.Context, emit func(context.Context, preparedLoadBatch) error) error {
				if err := emit(ctx, preparedLoadBatch{ordinal: 1, logicalBytes: 4}); err != nil {
					return err
				}
				select {
				case <-insertStarted:
				case <-ctx.Done():
					return ctx.Err()
				}
				close(preparedSecond)
				return emit(ctx, preparedLoadBatch{ordinal: 2, logicalBytes: 4})
			},
			func(batch preparedLoadBatch) error {
				if batch.ordinal == 1 {
					close(insertStarted)
					<-releaseInsert
				}
				return nil
			},
		)
	}()

	select {
	case <-preparedSecond:
		// The producer reached the second batch while the first insert was held.
	case <-time.After(2 * time.Second):
		t.Fatal("second batch was not prepared while first insert was blocked")
	}
	close(releaseInsert)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("pipeline did not finish")
	}
	if err != nil {
		t.Fatalf("runPreparedLoadPipeline: %v", err)
	}
	if stats.Depth != 1 {
		t.Fatalf("depth=%d want 1", stats.Depth)
	}
	if stats.MaxQueuedBatches != 0 {
		t.Fatalf("max queued batches=%d want 0 for depth one", stats.MaxQueuedBatches)
	}
	if stats.MaxBatchBytes != 4 {
		t.Fatalf("max batch bytes=%d want 4", stats.MaxBatchBytes)
	}
	if stats.MaxInFlightBytesBound != 8 {
		t.Fatalf("max in-flight bytes bound=%d want 8", stats.MaxInFlightBytesBound)
	}
}

func TestResetPreparedLoadBuffersReusesOnlySerialBuffers(t *testing.T) {
	ids := make([][]byte, 1, 4)
	docs := make([][]byte, 1, 4)
	originalIDSlot := &ids[:cap(ids)][0]
	originalDocSlot := &docs[:cap(docs)][0]

	serialIDs, serialDocs := resetPreparedLoadBuffers(ids, docs, 4, true)
	serialIDs = append(serialIDs, nil)
	serialDocs = append(serialDocs, nil)
	if &serialIDs[0] != originalIDSlot || &serialDocs[0] != originalDocSlot {
		t.Fatal("serial reset replaced reusable batch buffers")
	}

	pipelinedIDs, pipelinedDocs := resetPreparedLoadBuffers(ids, docs, 4, false)
	pipelinedIDs = append(pipelinedIDs, nil)
	pipelinedDocs = append(pipelinedDocs, nil)
	if &pipelinedIDs[0] == originalIDSlot || &pipelinedDocs[0] == originalDocSlot {
		t.Fatal("pipelined reset reused buffers still owned by the consumer")
	}
}

func TestRunPreparedLoadPipelinePreservesBatchOrder(t *testing.T) {
	var got []int
	stats, err := runPreparedLoadPipeline(
		context.Background(),
		1,
		func(ctx context.Context, emit func(context.Context, preparedLoadBatch) error) error {
			for i := 1; i <= 3; i++ {
				if err := emit(ctx, preparedLoadBatch{ordinal: i, logicalBytes: int64(i)}); err != nil {
					return err
				}
			}
			return nil
		},
		func(batch preparedLoadBatch) error {
			got = append(got, batch.ordinal)
			return nil
		},
	)
	if err != nil {
		t.Fatalf("runPreparedLoadPipeline: %v", err)
	}
	if want := []int{1, 2, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("inserted order=%v want %v", got, want)
	}
	if stats.MaxQueuedBatches > 1 {
		t.Fatalf("max queued batches=%d want <=1", stats.MaxQueuedBatches)
	}
}

func TestRunPreparedLoadPipelineCancelsProducerAfterInsertError(t *testing.T) {
	wantErr := errors.New("insert failed")
	producerStopped := make(chan struct{})
	inserted := 0

	_, err := runPreparedLoadPipeline(
		context.Background(),
		1,
		func(ctx context.Context, emit func(context.Context, preparedLoadBatch) error) error {
			defer close(producerStopped)
			if err := emit(ctx, preparedLoadBatch{ordinal: 1}); err != nil {
				return err
			}
			<-ctx.Done()
			return ctx.Err()
		},
		func(batch preparedLoadBatch) error {
			inserted++
			return wantErr
		},
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("error=%v want %v", err, wantErr)
	}
	if inserted != 1 {
		t.Fatalf("insert calls=%d want 1", inserted)
	}
	select {
	case <-producerStopped:
	case <-time.After(2 * time.Second):
		t.Fatal("producer goroutine did not stop after insert failure")
	}
}

func TestRunPreparedLoadPipelineDiscardsQueuedBatchAfterFirstError(t *testing.T) {
	wantErr := errors.New("first commit failed")
	queued := make(chan struct{})
	var inserted, discarded []int
	_, err := runPreparedLoadPipeline(
		context.Background(), 2,
		func(ctx context.Context, emit func(context.Context, preparedLoadBatch) error) error {
			if err := emit(ctx, preparedLoadBatch{ordinal: 1}); err != nil {
				return err
			}
			if err := emit(ctx, preparedLoadBatch{ordinal: 2}); err != nil {
				return err
			}
			close(queued)
			return nil
		},
		func(batch preparedLoadBatch) error {
			inserted = append(inserted, batch.ordinal)
			<-queued
			return wantErr
		},
		func(batch preparedLoadBatch) { discarded = append(discarded, batch.ordinal) },
	)
	if !errors.Is(err, wantErr) || !reflect.DeepEqual(inserted, []int{1}) || !reflect.DeepEqual(discarded, []int{2}) {
		t.Fatalf("error=%v inserted=%v discarded=%v", err, inserted, discarded)
	}
}

func TestRunPreparedLoadPipelineDepthZeroIsSerial(t *testing.T) {
	inserted := false
	stats, err := runPreparedLoadPipeline(
		context.Background(),
		0,
		func(ctx context.Context, emit func(context.Context, preparedLoadBatch) error) error {
			if err := emit(ctx, preparedLoadBatch{ordinal: 1, logicalBytes: 7}); err != nil {
				return err
			}
			if !inserted {
				t.Fatal("serial emit returned before insert completed")
			}
			return nil
		},
		func(batch preparedLoadBatch) error {
			inserted = true
			return nil
		},
	)
	if err != nil {
		t.Fatalf("runPreparedLoadPipeline: %v", err)
	}
	if stats.Depth != 0 || stats.Overlap != 0 {
		t.Fatalf("serial stats=%+v want depth=0 overlap=0", stats)
	}
}

func TestParseRunFlagsDefaultsToOneBatchAheadLoadPipeline(t *testing.T) {
	cfg, err := parseRunFlags([]string{"-scale", "1m"})
	if err != nil {
		t.Fatalf("parseRunFlags: %v", err)
	}
	if cfg.LoadPipelineDepth != 1 {
		t.Fatalf("load pipeline depth=%d want 1", cfg.LoadPipelineDepth)
	}
}

func TestParseRunFlagsRejectsNegativeLoadPipelineDepth(t *testing.T) {
	_, err := parseRunFlags([]string{"-scale", "1m", "-load-pipeline-depth", "-1"})
	if err == nil {
		t.Fatal("parseRunFlags err=nil want negative depth failure")
	}
}

func TestParseRunFlagsRejectsOversizedEnginePreparedBatch(t *testing.T) {
	if _, err := parseRunFlags([]string{"-scale", "1m", "-storage-layout", "column-store-full-prepared", "-engine-prepare-depth", "1", "-batch-size", "16385"}); err == nil {
		t.Fatal("oversized engine prepared batch accepted")
	}
}

func TestParseRunFlagsRejectsDeepTargetInputQueue(t *testing.T) {
	if _, err := parseRunFlags([]string{"-scale", "1m", "-storage-layout", "column-store-full-prepared", "-engine-prepare-depth", "0", "-load-pipeline-depth", "2"}); err == nil {
		t.Fatal("deep target input queue accepted")
	}
}

func TestDirectEnginePreparedBatchSizeFailsBeforeLoad(t *testing.T) {
	cfg := malformedJSONBenchRunConfig(t, writeMalformedJSONBenchFixture(t))
	cfg.BatchSize = 16385
	cfg.EnginePrepareMaxBytes = 512 << 20
	cfg.AllowErrors = true
	if _, err := runTreeDBBenchmark(cfg); !errors.Is(err, collections.ErrPreparedInsertResourceLimit) {
		t.Fatalf("direct oversized batch error=%v, want resource limit", err)
	}
	assertNoRowAfterRejectedEnginePrepare(t, cfg)
}

func TestDirectEnginePreparedDepthFailsBeforeLoad(t *testing.T) {
	for _, tc := range []struct {
		name              string
		depth, inputDepth int
	}{
		{name: "unsupported engine depth", depth: 2, inputDepth: 1},
		{name: "ahead without input queue", depth: 1, inputDepth: 0},
		{name: "deep input queue", depth: 0, inputDepth: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := malformedJSONBenchRunConfig(t, writeMalformedJSONBenchFixture(t))
			cfg.EnginePrepareMaxBytes = 512 << 20
			cfg.EnginePrepareDepth = tc.depth
			cfg.LoadPipelineDepth = tc.inputDepth
			cfg.AllowErrors = true
			if _, err := runTreeDBBenchmark(cfg); err == nil {
				t.Fatal("invalid direct engine depth accepted")
			}
			assertNoRowAfterRejectedEnginePrepare(t, cfg)
		})
	}
}

func TestPreparedInputScannerFailsClosedOnLargeLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "input.json")
	if err := os.WriteFile(path, []byte(`{"value":"`+strings.Repeat("x", 2<<20)+`"}`+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := scanInputFile(path, 1<<20, func([]byte) error {
		t.Fatal("oversized line reached document builder")
		return nil
	}); err == nil || !strings.Contains(err.Error(), "source line 1") || !strings.Contains(err.Error(), path) {
		t.Fatalf("oversized line error=%v, want path and line", err)
	}
}

func TestRunTreeDBBenchmarkPipelinedLoadMatchesSerialReconstruction(t *testing.T) {
	dataDir := writeMalformedJSONBenchFixture(t)
	serialCfg := malformedJSONBenchRunConfig(t, dataDir)
	serialCfg.AllowErrors = true
	serialCfg.LoadPipelineDepth = 0
	serial, err := runTreeDBBenchmark(serialCfg)
	if err != nil {
		t.Fatalf("serial runTreeDBBenchmark: %v", err)
	}

	pipelinedCfg := malformedJSONBenchRunConfig(t, dataDir)
	pipelinedCfg.AllowErrors = true
	pipelinedCfg.LoadPipelineDepth = 1
	pipelined, err := runTreeDBBenchmark(pipelinedCfg)
	if err != nil {
		t.Fatalf("pipelined runTreeDBBenchmark: %v", err)
	}

	if serial.Load.InputRows != pipelined.Load.InputRows ||
		serial.Load.Rows != pipelined.Load.Rows ||
		serial.Load.SkippedInvalidJSONRows != pipelined.Load.SkippedInvalidJSONRows {
		t.Fatalf("row accounting differs: serial=%+v pipelined=%+v", serial.Load, pipelined.Load)
	}
	if serial.Load.SourceCanonicalJSONHash != pipelined.Load.SourceCanonicalJSONHash {
		t.Fatalf("source hashes differ: serial=%s pipelined=%s", serial.Load.SourceCanonicalJSONHash, pipelined.Load.SourceCanonicalJSONHash)
	}
	if serial.Reconstruction == nil || pipelined.Reconstruction == nil {
		t.Fatalf("missing reconstruction: serial=%+v pipelined=%+v", serial.Reconstruction, pipelined.Reconstruction)
	}
	if !serial.Reconstruction.Valid || !pipelined.Reconstruction.Valid ||
		serial.Reconstruction.StoredCanonicalJSONHash != pipelined.Reconstruction.StoredCanonicalJSONHash {
		t.Fatalf("reconstruction differs: serial=%+v pipelined=%+v", serial.Reconstruction, pipelined.Reconstruction)
	}
	if pipelined.Load.PipelineDepth != 1 || pipelined.Load.MaxQueuedBatches > 1 {
		t.Fatalf("pipelined load accounting=%+v", pipelined.Load)
	}
	if pipelined.Load.AllocatedBytes == 0 || pipelined.Load.Allocations == 0 ||
		pipelined.Load.AllocatedBytesPerRow <= 0 || pipelined.Load.AllocationsPerRow <= 0 {
		t.Fatalf("missing pipelined allocation accounting: %+v", pipelined.Load)
	}
}

func TestRunTreeDBBenchmarkEnginePrepareDepthMatchesControl(t *testing.T) {
	const engineByteLimit = 9 << 29
	dataDir := writeMalformedJSONBenchFixture(t)
	controlCfg := malformedJSONBenchRunConfig(t, dataDir)
	controlCfg.AllowErrors = true
	controlCfg.BatchSize = 1
	controlCfg.LoadPipelineDepth = 1
	controlCfg.EnginePrepareDepth = 0
	controlCfg.EnginePrepareMaxBytes = engineByteLimit
	control, err := runTreeDBBenchmark(controlCfg)
	if err != nil {
		t.Fatal(err)
	}
	enabledCfg := controlCfg
	enabledCfg.DBDir = t.TempDir()
	enabledCfg.EnginePrepareDepth = 1
	enabled, err := runTreeDBBenchmark(enabledCfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range []runResult{control, enabled} {
		if result.Load.InputRows != 3 || result.Load.Rows != 2 || result.Load.SkippedInvalidJSONRows != 1 {
			t.Fatalf("load accounting=%+v", result.Load)
		}
		if result.Load.EnginePreparePath != "prepared" || result.Load.EnginePreparedBatches != 2 ||
			result.Load.EngineCommittedBatches != 2 || result.Load.EngineAbandonedBatches != 0 ||
			result.Load.EnginePeakOwnedBytes <= 0 || result.Load.EnginePeakOwnedBytes > 2*engineByteLimit ||
			result.Load.EnginePeakReservedBytes <= 0 || result.Load.EnginePeakReservedBytes > engineByteLimit ||
			result.Load.EngineIdleScratchReserveBytes != 32<<20 ||
			result.Load.EnginePeakOwnedBatches < 1 || result.Load.EnginePeakOwnedBatches > 2 {
			t.Fatalf("engine accounting=%+v", result.Load)
		}
		if result.Reconstruction == nil || !result.Reconstruction.Valid {
			t.Fatalf("reconstruction=%+v", result.Reconstruction)
		}
	}
	if control.Load.SourceCanonicalJSONHash != enabled.Load.SourceCanonicalJSONHash ||
		control.Reconstruction.StoredCanonicalJSONHash != enabled.Reconstruction.StoredCanonicalJSONHash ||
		len(control.Queries) != len(enabled.Queries) {
		t.Fatalf("control/enabled hashes or query count differ")
	}
	for i := range control.Queries {
		if control.Queries[i].Name != enabled.Queries[i].Name || control.Queries[i].ResultHash != enabled.Queries[i].ResultHash {
			t.Fatalf("query %d differs: control=%+v enabled=%+v", i, control.Queries[i], enabled.Queries[i])
		}
	}
}

func TestRunTreeDBBenchmarkEnginePrepareResourceLimitFailsClosed(t *testing.T) {
	dataDir := writeMalformedJSONBenchFixture(t)
	cfg := malformedJSONBenchRunConfig(t, dataDir)
	cfg.AllowErrors = true
	cfg.BatchSize = 1
	cfg.LoadPipelineDepth = 1
	cfg.EnginePrepareDepth = 1
	cfg.EnginePrepareMaxBytes = 1
	_, err := runTreeDBBenchmark(cfg)
	if !errors.Is(err, collections.ErrPreparedInsertResourceLimit) {
		t.Fatalf("load error=%v, want resource limit", err)
	}
	assertNoRowAfterRejectedEnginePrepare(t, cfg)
}

func TestBuildDocumentFullPreparedChargesExactCloneCapacity(t *testing.T) {
	for _, size := range []int{1, 8, 63, 1023, 128 << 10} {
		raw := []byte(strings.Repeat("x", size))
		doc, err := buildDocument(raw, collections.DocumentFormatJSON, "full", storageLayoutColumnStoreFullPrepared, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(doc) != size || cap(doc) != size || !reflect.DeepEqual(doc, raw) {
			t.Fatalf("size %d: len=%d cap=%d equal=%v", size, len(doc), cap(doc), reflect.DeepEqual(doc, raw))
		}
		doc[0] = 'y'
		if raw[0] != 'x' {
			t.Fatalf("size %d: clone aliases source", size)
		}
	}
}

func TestRunTreeDBBenchmarkEnginePrepareDocumentAboveEligibilityFailsClosed(t *testing.T) {
	dataDir := t.TempDir()
	row := `{"did":"did:plc:large","time_us":1700000000000000,"kind":"commit","commit":{"operation":"create","collection":"app.bsky.feed.post"},"extra":"` + strings.Repeat("x", 130<<10) + `"}` + "\n"
	if err := os.WriteFile(filepath.Join(dataDir, "file_0001.json"), []byte(row), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := malformedJSONBenchRunConfig(t, dataDir)
	cfg.Rows = 1
	cfg.BatchSize = 1
	cfg.LoadPipelineDepth = 1
	cfg.EnginePrepareDepth = 1
	cfg.EnginePrepareMaxBytes = 512 << 20
	_, err := runTreeDBBenchmark(cfg)
	if !errors.Is(err, collections.ErrPreparedInsertResourceLimit) {
		t.Fatalf("load error=%v, want resource limit", err)
	}
	assertNoRowAfterRejectedEnginePrepare(t, cfg)
}

func assertNoRowAfterRejectedEnginePrepare(t *testing.T, cfg runConfig) {
	t.Helper()
	backend, cleanup, err := openBackend(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	col, err := collections.NewCollectionManager(backend).OpenCollection(cfg.Collection)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := col.Get(documentID(1)); err != nil || got != nil {
		t.Fatalf("resource-rejected row was inserted: %s, %v", got, err)
	}
}

func TestRunTreeDBBenchmarkNonTargetLargeRowKeepsOrdinarySourcePolicy(t *testing.T) {
	dataDir := t.TempDir()
	row := `{"did":"did:plc:large","time_us":1700000000000000,"kind":"commit","commit":{"operation":"create","collection":"app.bsky.feed.post"},"extra":"` + strings.Repeat("x", 1<<20) + `"}` + "\n"
	if err := os.WriteFile(filepath.Join(dataDir, "file_0001.json"), []byte(row), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := malformedJSONBenchRunConfig(t, dataDir)
	cfg.Rows = 1
	cfg.BatchSize = 1
	cfg.StorageLayout = storageLayoutRow
	cfg.ValidateReconstruction = false
	cfg.LoadPipelineDepth = 1
	cfg.EnginePrepareDepth = 1
	result, err := runTreeDBBenchmark(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if result.Load.Rows != 1 || result.Load.EnginePreparePath != "ordinary" || result.Load.EnginePreparedBatches != 0 {
		t.Fatalf("ordinary non-target load=%+v", result.Load)
	}
	parsed, err := parseRunFlags([]string{"-storage-layout", "row", "-load-pipeline-depth", "0"})
	if err != nil {
		t.Fatalf("historical serial row-store flags: %v", err)
	}
	if parsed.LoadPipelineDepth != 0 || parsed.EnginePrepareDepth != 1 {
		t.Fatalf("parsed historical serial row-store flags=%+v", parsed)
	}
}

func TestCollectTreeDBRowsExportsLoadPipelineAccounting(t *testing.T) {
	rows := collectTreeDBRowsForMetadataCostTest(t, loadResult{
		EnginePrepareDepth:            1,
		EnginePreparePath:             "prepared",
		EnginePreparedBatches:         4,
		EngineCommittedBatches:        4,
		EngineFallbackBatches:         1,
		EnginePrepareSec:              1.1,
		EngineCommitSec:               3.2,
		EngineOverlapSec:              0.7,
		EnginePeakOwnedBytes:          123456,
		EnginePeakReservedBytes:       654321,
		EngineIdleScratchReserveBytes: 32 << 20,
		EnginePeakOwnedBatches:        2,
		PipelineDepth:                 1,
		ProducerElapsedSec:            3.5,
		ProducerWorkSec:               3.0,
		ProducerWaitSec:               0.5,
		ConsumerWaitSec:               0.25,
		OverlapSec:                    2.0,
		MaxQueuedBatches:              1,
		MaxBatchBytes:                 8_000_000,
		MaxInFlightBytesBound:         16_000_000,
		AllocatedBytes:                24_000_000,
		Allocations:                   12_000,
		AllocatedBytesPerRow:          4_000_000,
		AllocationsPerRow:             2_000,
	})
	if len(rows) != 1 {
		t.Fatalf("report rows=%d want 1", len(rows))
	}
	row := rows[0]
	if row.LoadEnginePrepareDepth != 1 || row.LoadEnginePreparePath != "prepared" ||
		row.LoadEnginePreparedBatches != 4 || row.LoadEngineCommittedBatches != 4 || row.LoadEngineFallbackBatches != 1 ||
		row.LoadEnginePrepareSec != 1.1 || row.LoadEngineCommitSec != 3.2 || row.LoadEngineOverlapSec != 0.7 ||
		row.LoadEnginePeakOwnedBytes != 123456 || row.LoadEnginePeakReservedBytes != 654321 ||
		row.LoadEngineIdleScratchReserveBytes != 32<<20 || row.LoadEnginePeakOwnedBatches != 2 {
		t.Fatalf("engine prepare report row=%+v", row)
	}
	if row.LoadPipelineDepth != 1 || row.LoadProducerWorkSec != 3.0 ||
		row.LoadProducerWaitSec != 0.5 || row.LoadOverlapSec != 2.0 ||
		row.LoadMaxQueuedBatches != 1 || row.LoadMaxInFlightBytesBound != 16_000_000 {
		t.Fatalf("load pipeline report row=%+v", row)
	}
	if row.LoadAllocatedBytes != 24_000_000 || row.LoadAllocations != 12_000 ||
		row.LoadAllocatedBytesPerRow != 4_000_000 || row.LoadAllocationsPerRow != 2_000 {
		t.Fatalf("load allocation report row=%+v", row)
	}
}
