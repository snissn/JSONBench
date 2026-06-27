package main

import (
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
)

const (
	metadataCostInsertBasisAggregateMetadataSplit = "aggregate_metadata_prepare_duration_plus_byte_weighted_share_of_shared_batched_asset_append"
	metadataCostInsertBasisFullLoadUpperBound     = "full_load_insert_seconds_current_upper_bound"
)

type aggregateMetadataLoadAccounting struct {
	prepareElapsed     time.Duration
	appendShareElapsed time.Duration
	metadataBytes      int64
	sharedAppendBytes  int64
}

func (a *aggregateMetadataLoadAccounting) addInsertStats(stats collections.CollectionInsertStats) {
	a.prepareElapsed += stats.ColumnPublishAggregateMetadataPrepare
	a.appendShareElapsed += metadataCostDurationShare(
		stats.ColumnPublishAssetAppend,
		stats.ColumnPublishAggregateMetadataBytes,
		stats.ColumnPublishSharedAppendBytes,
	)
	if stats.ColumnPublishAggregateMetadataBytes > 0 {
		a.metadataBytes += stats.ColumnPublishAggregateMetadataBytes
	}
	if stats.ColumnPublishSharedAppendBytes > 0 {
		a.sharedAppendBytes += stats.ColumnPublishSharedAppendBytes
	}
}

func (a aggregateMetadataLoadAccounting) apply(out *loadResult) {
	if a.metadataBytes > 0 {
		out.AggregateMetadataBytes = a.metadataBytes
	}
	if a.sharedAppendBytes > 0 {
		out.AggregateMetadataSharedAppendBytes = a.sharedAppendBytes
	}
	out.AggregateMetadataPrepareSec = a.prepareElapsed.Seconds()
	out.AggregateMetadataAppendShareSec = a.appendShareElapsed.Seconds()
	insertCost := a.prepareElapsed + a.appendShareElapsed
	if insertCost <= 0 || (a.metadataBytes <= 0 && a.prepareElapsed <= 0) {
		return
	}
	out.AggregateMetadataInsertCostSec = insertCost.Seconds()
	out.AggregateMetadataInsertCostBasis = metadataCostInsertBasisAggregateMetadataSplit
}

func metadataCostDurationShare(total time.Duration, partBytes, totalBytes int64) time.Duration {
	if total <= 0 || partBytes <= 0 || totalBytes <= 0 {
		return 0
	}
	if partBytes >= totalBytes {
		return total
	}
	return time.Duration(float64(total) * (float64(partBytes) / float64(totalBytes)))
}

func aggregateMetadataSplitInsertCost(load loadResult) (float64, string, bool) {
	if load.AggregateMetadataInsertCostSec <= 0 {
		return 0, "", false
	}
	if load.AggregateMetadataBytes <= 0 && load.AggregateMetadataPrepareSec <= 0 {
		return 0, "", false
	}
	basis := load.AggregateMetadataInsertCostBasis
	if basis == "" {
		basis = metadataCostInsertBasisAggregateMetadataSplit
	}
	return load.AggregateMetadataInsertCostSec, basis, true
}
