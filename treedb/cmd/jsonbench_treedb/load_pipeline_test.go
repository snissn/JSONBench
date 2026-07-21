package main

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
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

func TestCollectTreeDBRowsExportsLoadPipelineAccounting(t *testing.T) {
	rows := collectTreeDBRowsForMetadataCostTest(t, loadResult{
		PipelineDepth:         1,
		ProducerElapsedSec:    3.5,
		ProducerWorkSec:       3.0,
		ProducerWaitSec:       0.5,
		ConsumerWaitSec:       0.25,
		OverlapSec:            2.0,
		MaxQueuedBatches:      1,
		MaxBatchBytes:         8_000_000,
		MaxInFlightBytesBound: 16_000_000,
		AllocatedBytes:        24_000_000,
		Allocations:           12_000,
		AllocatedBytesPerRow:  4_000_000,
		AllocationsPerRow:     2_000,
	})
	if len(rows) != 1 {
		t.Fatalf("report rows=%d want 1", len(rows))
	}
	row := rows[0]
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
