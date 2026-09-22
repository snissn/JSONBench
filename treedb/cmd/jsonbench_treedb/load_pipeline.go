package main

import (
	"context"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
)

// preparedLoadBatch owns all IDs and documents for one ordered InsertBatch
// handoff. The producer must not reuse these slices after emit returns.
type preparedLoadBatch struct {
	ordinal            int
	ids                [][]byte
	docs               [][]byte
	logicalBytes       int64
	engine             *collections.PreparedInsertBatch
	enginePrepareStart time.Time
	enginePrepareEnd   time.Time
}

type loadPipelineStats struct {
	Depth                 int
	ProducerElapsed       time.Duration
	ProducerWait          time.Duration
	ProducerWork          time.Duration
	ConsumerWait          time.Duration
	InsertElapsed         time.Duration
	Overlap               time.Duration
	MaxQueuedBatches      int
	MaxBatchBytes         int64
	MaxInFlightBytesBound int64
}

type loadProducerOutcome struct {
	err              error
	elapsed          time.Duration
	wait             time.Duration
	maxQueuedBatches int
	maxBatchBytes    int64
}

// runPreparedLoadPipeline preserves producer order and uses depth as the
// maximum number of batches being prepared or waiting ahead of the batch being
// inserted. Depth zero is the exact serial control: emit does not return until
// insert completes.
func runPreparedLoadPipeline(
	ctx context.Context,
	depth int,
	prepare func(context.Context, func(context.Context, preparedLoadBatch) error) error,
	insert func(preparedLoadBatch) error,
	discard ...func(preparedLoadBatch),
) (loadPipelineStats, error) {
	stats := loadPipelineStats{Depth: depth}
	if depth <= 0 {
		start := time.Now()
		err := prepare(ctx, func(_ context.Context, batch preparedLoadBatch) error {
			if batch.logicalBytes > stats.MaxBatchBytes {
				stats.MaxBatchBytes = batch.logicalBytes
			}
			insertStart := time.Now()
			err := insert(batch)
			stats.InsertElapsed += time.Since(insertStart)
			return err
		})
		stats.ProducerElapsed = time.Since(start)
		stats.ProducerWork = stats.ProducerElapsed - stats.InsertElapsed
		if stats.ProducerWork < 0 {
			stats.ProducerWork = 0
		}
		stats.MaxInFlightBytesBound = stats.MaxBatchBytes
		return stats, err
	}

	pipeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	// The producer itself may own one complete batch while blocked in emit, so
	// reserve only depth-1 channel slots to keep the configured ahead bound exact.
	batches := make(chan preparedLoadBatch, depth-1)
	outcomes := make(chan loadProducerOutcome, 1)
	wallStart := time.Now()
	go func() {
		out := loadProducerOutcome{}
		start := time.Now()
		out.err = prepare(pipeCtx, func(emitCtx context.Context, batch preparedLoadBatch) error {
			if batch.logicalBytes > out.maxBatchBytes {
				out.maxBatchBytes = batch.logicalBytes
			}
			waitStart := time.Now()
			select {
			case batches <- batch:
				out.wait += time.Since(waitStart)
				if queued := len(batches); queued > out.maxQueuedBatches {
					out.maxQueuedBatches = queued
				}
				return nil
			case <-emitCtx.Done():
				out.wait += time.Since(waitStart)
				return emitCtx.Err()
			case <-pipeCtx.Done():
				out.wait += time.Since(waitStart)
				return pipeCtx.Err()
			}
		})
		out.elapsed = time.Since(start)
		close(batches)
		outcomes <- out
	}()

	var insertErr error
	for {
		waitStart := time.Now()
		batch, ok := <-batches
		stats.ConsumerWait += time.Since(waitStart)
		if !ok {
			break
		}
		if insertErr != nil {
			// Drain already-prepared work so the producer can observe cancellation
			// and terminate; never insert another batch after the first failure.
			if len(discard) != 0 {
				discard[0](batch)
			}
			continue
		}
		insertStart := time.Now()
		if err := insert(batch); err != nil {
			insertErr = err
			cancel()
		}
		stats.InsertElapsed += time.Since(insertStart)
	}
	outcome := <-outcomes
	stats.ProducerElapsed = outcome.elapsed
	stats.ProducerWait = outcome.wait
	stats.ProducerWork = stats.ProducerElapsed - stats.ProducerWait
	if stats.ProducerWork < 0 {
		stats.ProducerWork = 0
	}
	stats.MaxQueuedBatches = outcome.maxQueuedBatches
	stats.MaxBatchBytes = outcome.maxBatchBytes
	stats.MaxInFlightBytesBound = int64(depth+1) * stats.MaxBatchBytes
	wallElapsed := time.Since(wallStart)
	stats.Overlap = stats.ProducerWork + stats.InsertElapsed - wallElapsed
	if stats.Overlap < 0 {
		stats.Overlap = 0
	}
	if insertErr != nil {
		return stats, insertErr
	}
	return stats, outcome.err
}

func resetPreparedLoadBuffers(ids, docs [][]byte, batchSize int, reuse bool) ([][]byte, [][]byte) {
	if reuse {
		return ids[:0], docs[:0]
	}
	return make([][]byte, 0, batchSize), make([][]byte, 0, batchSize)
}
