package main

import (
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
)

type insertStatsResult struct {
	RetainedPayloadPrepareSec            float64 `json:"retained_payload_prepare_seconds,omitempty"`
	RetainedPayloadRows                  int     `json:"retained_payload_rows,omitempty"`
	RetainedPayloadDeclaredRows          int     `json:"retained_payload_declared_rows,omitempty"`
	RetainedPayloadSemanticStreamBlocks  int     `json:"retained_payload_semantic_stream_blocks,omitempty"`
	RetainedPayloadValueLogPointerizeSec float64 `json:"retained_payload_value_log_pointerize_seconds,omitempty"`
	RetainedPayloadValueLogValues        int     `json:"retained_payload_value_log_values,omitempty"`
	RetainedPayloadValueLogBytes         int64   `json:"retained_payload_value_log_bytes,omitempty"`
	RetainedStreamValueLogPointerizeSec  float64 `json:"retained_stream_value_log_pointerize_seconds,omitempty"`
	RetainedStreamValueLogValues         int     `json:"retained_stream_value_log_values,omitempty"`
	RetainedStreamValueLogBytes          int64   `json:"retained_stream_value_log_bytes,omitempty"`
}

type insertStatsAccounting struct {
	retainedPayloadPrepare            time.Duration
	retainedPayloadRows               int
	retainedPayloadDeclaredRows       int
	retainedPayloadSemanticBlocks     int
	retainedPayloadValueLogPointerize time.Duration
	retainedPayloadValueLogValues     int
	retainedPayloadValueLogBytes      int64
	retainedStreamValueLogPointerize  time.Duration
	retainedStreamValueLogValues      int
	retainedStreamValueLogBytes       int64
}

func (a *insertStatsAccounting) add(stats collections.CollectionInsertStats) {
	a.retainedPayloadPrepare += stats.RetainedPayloadPrepare
	a.retainedPayloadRows += stats.RetainedPayloadRows
	a.retainedPayloadDeclaredRows += stats.RetainedPayloadDeclaredRows
	a.retainedPayloadSemanticBlocks += stats.RetainedPayloadSemanticStreamBlocks
	a.retainedPayloadValueLogPointerize += stats.RetainedPayloadValueLogPointerize
	a.retainedPayloadValueLogValues += stats.RetainedPayloadValueLogValues
	a.retainedPayloadValueLogBytes += stats.RetainedPayloadValueLogBytes
	a.retainedStreamValueLogPointerize += stats.RetainedStreamValueLogPointerize
	a.retainedStreamValueLogValues += stats.RetainedStreamValueLogValues
	a.retainedStreamValueLogBytes += stats.RetainedStreamValueLogBytes
}

func (a insertStatsAccounting) result() *insertStatsResult {
	if a.retainedPayloadPrepare <= 0 &&
		a.retainedPayloadRows == 0 &&
		a.retainedPayloadDeclaredRows == 0 &&
		a.retainedPayloadSemanticBlocks == 0 &&
		a.retainedPayloadValueLogPointerize <= 0 &&
		a.retainedPayloadValueLogValues == 0 &&
		a.retainedPayloadValueLogBytes == 0 &&
		a.retainedStreamValueLogPointerize <= 0 &&
		a.retainedStreamValueLogValues == 0 &&
		a.retainedStreamValueLogBytes == 0 {
		return nil
	}
	return &insertStatsResult{
		RetainedPayloadPrepareSec:            a.retainedPayloadPrepare.Seconds(),
		RetainedPayloadRows:                  a.retainedPayloadRows,
		RetainedPayloadDeclaredRows:          a.retainedPayloadDeclaredRows,
		RetainedPayloadSemanticStreamBlocks:  a.retainedPayloadSemanticBlocks,
		RetainedPayloadValueLogPointerizeSec: a.retainedPayloadValueLogPointerize.Seconds(),
		RetainedPayloadValueLogValues:        a.retainedPayloadValueLogValues,
		RetainedPayloadValueLogBytes:         a.retainedPayloadValueLogBytes,
		RetainedStreamValueLogPointerizeSec:  a.retainedStreamValueLogPointerize.Seconds(),
		RetainedStreamValueLogValues:         a.retainedStreamValueLogValues,
		RetainedStreamValueLogBytes:          a.retainedStreamValueLogBytes,
	}
}
