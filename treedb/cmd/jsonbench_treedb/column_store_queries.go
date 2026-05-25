package main

import (
	"sort"

	"github.com/snissn/gomap/TreeDB/collections"
)

func runColumnQ1(collection *collections.Collection, _ runConfig, rows int) (queryComputation, error) {
	result, err := collection.RunColumnPhysicalQuery(columnPhysicalRequest(collections.ColumnPhysicalQueryGroupCount, "event", "", ""))
	if err != nil {
		return queryComputation{}, err
	}
	out := make([]queryRow, 0, len(result.Groups))
	for _, group := range result.Groups {
		out = append(out, queryRow{"event": group.Key, "count": int64(group.Count)})
	}
	sort.Slice(out, func(i, j int) bool {
		ci := out[i]["count"].(int64)
		cj := out[j]["count"].(int64)
		if ci != cj {
			return ci > cj
		}
		return out[i]["event"].(string) < out[j]["event"].(string)
	})
	return queryComputation{RowsScanned: columnPhysicalRowsScanned(rows, result), Rows: out}, nil
}

func runColumnQ2(collection *collections.Collection, _ runConfig, rows int) (queryComputation, error) {
	countResult, err := collection.RunColumnPhysicalQuery(columnPhysicalRequest(collections.ColumnPhysicalQueryGroupCount, "event", "", ""))
	if err != nil {
		return queryComputation{}, err
	}
	distinctResult, err := collection.RunColumnPhysicalQuery(columnPhysicalRequest(collections.ColumnPhysicalQueryGroupCountDistinct, "event", "", "did"))
	if err != nil {
		return queryComputation{}, err
	}
	counts := make(map[string]int64, len(countResult.Groups))
	for _, group := range countResult.Groups {
		if group.Key == "" {
			continue
		}
		counts[group.Key] = int64(group.Count)
	}
	users := make(map[string]int64, len(distinctResult.Groups))
	for _, group := range distinctResult.Groups {
		if group.Key == "" {
			continue
		}
		users[group.Key] = int64(group.Count)
	}
	out := make([]queryRow, 0, len(counts))
	for event, count := range counts {
		out = append(out, queryRow{"event": event, "count": count, "users": users[event]})
	}
	sort.Slice(out, func(i, j int) bool {
		ci := out[i]["count"].(int64)
		cj := out[j]["count"].(int64)
		if ci != cj {
			return ci > cj
		}
		return out[i]["event"].(string) < out[j]["event"].(string)
	})
	return queryComputation{RowsScanned: maxColumnPhysicalRowsScanned(rows, countResult, distinctResult), Rows: out}, nil
}

func runColumnQ4(collection *collections.Collection, _ runConfig, rows int) (queryComputation, error) {
	result, err := collection.RunColumnPhysicalQuery(columnPhysicalRequest(collections.ColumnPhysicalQueryGroupMinInt64, "did", "time_us", ""))
	if err != nil {
		return queryComputation{}, err
	}
	out := make([]queryRow, 0, len(result.Groups))
	for _, group := range result.Groups {
		if group.Key == "" {
			continue
		}
		out = append(out, queryRow{"user_id": group.Key, "first_post_time_us": group.Int64})
	}
	sort.Slice(out, func(i, j int) bool {
		ti := out[i]["first_post_time_us"].(int64)
		tj := out[j]["first_post_time_us"].(int64)
		if ti != tj {
			return ti < tj
		}
		return out[i]["user_id"].(string) < out[j]["user_id"].(string)
	})
	if len(out) > 3 {
		out = out[:3]
	}
	return queryComputation{RowsScanned: columnPhysicalRowsScanned(rows, result), Rows: out}, nil
}

func runColumnQ5(collection *collections.Collection, _ runConfig, rows int) (queryComputation, error) {
	result, err := collection.RunColumnPhysicalQuery(columnPhysicalRequest(collections.ColumnPhysicalQueryGroupInt64Span, "did", "time_us", ""))
	if err != nil {
		return queryComputation{}, err
	}
	out := make([]queryRow, 0, len(result.Groups))
	for _, group := range result.Groups {
		if group.Key == "" {
			continue
		}
		out = append(out, queryRow{"user_id": group.Key, "activity_span_ms": group.Int64 / 1000})
	}
	sort.Slice(out, func(i, j int) bool {
		si := out[i]["activity_span_ms"].(int64)
		sj := out[j]["activity_span_ms"].(int64)
		if si != sj {
			return si > sj
		}
		return out[i]["user_id"].(string) < out[j]["user_id"].(string)
	})
	if len(out) > 3 {
		out = out[:3]
	}
	return queryComputation{RowsScanned: columnPhysicalRowsScanned(rows, result), Rows: out}, nil
}

func columnPhysicalRequest(kind collections.ColumnPhysicalQueryKind, groupColumn, valueColumn, distinctColumn string) collections.ColumnPhysicalQueryRequest {
	return collections.ColumnPhysicalQueryRequest{
		Kind:                     kind,
		GroupColumn:              groupColumn,
		ValueColumn:              valueColumn,
		DistinctColumn:           distinctColumn,
		ColumnAssetReadIntegrity: collections.ColumnAssetReadIntegritySkipChecksums,
	}
}

func columnPhysicalRowsScanned(fallback int, result collections.ColumnPhysicalQueryResult) int {
	if result.Diagnostics.RowsScanned > 0 {
		return result.Diagnostics.RowsScanned
	}
	return fallback
}

func maxColumnPhysicalRowsScanned(fallback int, results ...collections.ColumnPhysicalQueryResult) int {
	maxRows := fallback
	for _, result := range results {
		if result.Diagnostics.RowsScanned > maxRows {
			maxRows = result.Diagnostics.RowsScanned
		}
	}
	return maxRows
}
