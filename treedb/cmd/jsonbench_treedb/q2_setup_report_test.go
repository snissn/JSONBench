package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestQ2SetupReportRowsExposePostPrepareSplit3324(t *testing.T) {
	row := reportRow{
		System: "TreeDB",
		Query:  "q2",
		TypedColumnPrepareQ2DenseGroupGlobalRankNanos:         11,
		TypedColumnPrepareQ2DenseDistinctGlobalRankNanos:      12,
		TypedColumnPrepareQ2DensePartLocalRankNanos:           13,
		TypedColumnPrepareQ2DenseDistinctRankPlanNanos:        14,
		TypedColumnPrepareQ2DenseDistinctRankCollectRefsNanos: 15,
		TypedColumnPrepareQ2DenseDistinctRankBuildShardsNanos: 16,
		TypedColumnPrepareQ2DenseDistinctRankShardCount:       17,
		TypedColumnPrepareQ2DenseDistinctRankRefs:             18,
		TypedColumnPrepareQ2DenseDistinctRankMaxShardRefs:     19,
		TypedColumnPrepareQ2DenseDistinctGlobalRanks:          20,
		TypedColumnPrepareQ2GroupGlobalDictionaryRankNanos:    21,
		TypedColumnPrepareQ2DistinctGlobalDictionaryRankNanos: 22,
		TypedColumnPrepareQ2GroupGlobalCodeRemapNanos:         23,
		TypedColumnPrepareQ2DistinctGlobalCodeRemapNanos:      24,
	}
	raw, err := json.Marshal(row)
	if err != nil {
		t.Fatalf("marshal report row: %v", err)
	}
	for _, want := range []string{
		`"typed_column_prepare_q2_dense_group_global_rank_nanos":11`,
		`"typed_column_prepare_q2_dense_distinct_global_rank_nanos":12`,
		`"typed_column_prepare_q2_dense_part_local_rank_nanos":13`,
		`"typed_column_prepare_q2_dense_distinct_rank_plan_nanos":14`,
		`"typed_column_prepare_q2_dense_distinct_rank_collect_refs_nanos":15`,
		`"typed_column_prepare_q2_dense_distinct_rank_build_shards_nanos":16`,
		`"typed_column_prepare_q2_dense_distinct_rank_shard_count":17`,
		`"typed_column_prepare_q2_dense_distinct_rank_refs":18`,
		`"typed_column_prepare_q2_dense_distinct_rank_max_shard_refs":19`,
		`"typed_column_prepare_q2_dense_distinct_global_ranks":20`,
		`"typed_column_prepare_q2_group_global_dictionary_rank_nanos":21`,
		`"typed_column_prepare_q2_distinct_global_dictionary_rank_nanos":22`,
		`"typed_column_prepare_q2_group_global_code_remap_nanos":23`,
		`"typed_column_prepare_q2_distinct_global_code_remap_nanos":24`,
	} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("report JSON missing %s\n%s", want, raw)
		}
	}
	if !reportRowHasTypedColumnSetupDiagnostics(row) {
		t.Fatalf("q2 split-only row should render typed-column setup diagnostics")
	}
}

func TestRenderMarkdownReportIncludesQ2PostPrepareSplit3324(t *testing.T) {
	doc := reportDocument{GeneratedAt: "2026-06-01T00:00:00Z", Rows: []reportRow{{
		System: "TreeDB",
		Scale:  "1M",
		Query:  "q2",
		TypedColumnPrepareQ2DenseGroupGlobalRankNanos:         11,
		TypedColumnPrepareQ2DenseDistinctGlobalRankNanos:      12,
		TypedColumnPrepareQ2DensePartLocalRankNanos:           13,
		TypedColumnPrepareQ2DenseDistinctRankPlanNanos:        14,
		TypedColumnPrepareQ2DenseDistinctRankCollectRefsNanos: 15,
		TypedColumnPrepareQ2DenseDistinctRankBuildShardsNanos: 16,
		TypedColumnPrepareQ2DenseDistinctRankShardCount:       17,
		TypedColumnPrepareQ2DenseDistinctRankRefs:             18,
		TypedColumnPrepareQ2DenseDistinctRankMaxShardRefs:     19,
		TypedColumnPrepareQ2DenseDistinctGlobalRanks:          20,
		TypedColumnPrepareQ2GroupGlobalDictionaryRankNanos:    21,
		TypedColumnPrepareQ2DistinctGlobalDictionaryRankNanos: 22,
		TypedColumnPrepareQ2GroupGlobalCodeRemapNanos:         23,
		TypedColumnPrepareQ2DistinctGlobalCodeRemapNanos:      24,
	}}}
	got := string(renderMarkdownReport(doc))
	for _, want := range []string{
		"## TreeDB Typed Column Setup Diagnostics",
		"q2 dense group global rank ns",
		"q2 dense distinct global rank ns",
		"q2 dense part local rank ns",
		"q2 dense distinct rank plan ns",
		"q2 dense distinct rank collect refs ns",
		"q2 dense distinct rank build shards ns",
		"q2 dense distinct rank shards",
		"q2 dense distinct rank refs",
		"q2 dense distinct rank max shard refs",
		"q2 dense distinct global ranks",
		"q2 group global dict/rank ns",
		"q2 distinct global dict/rank ns",
		"q2 group global-code remap ns",
		"q2 distinct global-code remap ns",
		"| 1M |",
		"| q2 |",
		"| 11 | 12 | 13 | 14 | 15 | 16 | 17 | 18 | 19 | 20 | 21 | 22 | 23 | 24 |",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("markdown report missing %q\n%s", want, got)
		}
	}
}
