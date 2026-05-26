package main

import (
	"errors"
	"fmt"
	"sort"

	"github.com/snissn/gomap/TreeDB/collections"
)

func runColumnQ1(collection *collections.Collection, cfg runConfig, rows int) (queryComputation, error) {
	result, err := collection.RunColumnPhysicalQuery(columnPhysicalRequest(cfg, "q1", collections.ColumnPhysicalQueryGroupCount, "event", "", ""))
	if err != nil {
		return queryComputation{}, err
	}
	return renderColumnQ1(rows, result), nil
}

func runColumnQ2(collection *collections.Collection, cfg runConfig, rows int) (queryComputation, error) {
	countResult, err := collection.RunColumnPhysicalQuery(columnPhysicalRequest(cfg, "q2", collections.ColumnPhysicalQueryGroupCount, "event", "", ""))
	if err != nil {
		return queryComputation{}, err
	}
	distinctResult, err := collection.RunColumnPhysicalQuery(columnPhysicalRequest(cfg, "q2", collections.ColumnPhysicalQueryGroupCountDistinct, "event", "", "did"))
	if err != nil {
		return queryComputation{}, err
	}
	return renderColumnQ2(rows, countResult, distinctResult), nil
}

func runColumnQ3(collection *collections.Collection, cfg runConfig, rows int) (queryComputation, error) {
	result, err := collection.RunColumnPhysicalQuery(columnPhysicalRequest(cfg, "q3", collections.ColumnPhysicalQueryGroupHourCount, "event", "time_us", ""))
	if err != nil {
		return queryComputation{}, err
	}
	return renderColumnQ3(rows, result), nil
}

func runColumnQ4(collection *collections.Collection, cfg runConfig, rows int) (queryComputation, error) {
	result, err := collection.RunColumnPhysicalQuery(columnPhysicalRequest(cfg, "q4", collections.ColumnPhysicalQueryGroupMinInt64, "did", "time_us", ""))
	if err != nil {
		return queryComputation{}, err
	}
	return renderColumnQ4(rows, result), nil
}

func runColumnQ5(collection *collections.Collection, cfg runConfig, rows int) (queryComputation, error) {
	result, err := collection.RunColumnPhysicalQuery(columnPhysicalRequest(cfg, "q5", collections.ColumnPhysicalQueryGroupInt64Span, "did", "time_us", ""))
	if err != nil {
		return queryComputation{}, err
	}
	return renderColumnQ5(rows, result), nil
}

func columnPhysicalRequest(cfg runConfig, query string, kind collections.ColumnPhysicalQueryKind, groupColumn, valueColumn, distinctColumn string) collections.ColumnPhysicalQueryRequest {
	req := collections.ColumnPhysicalQueryRequest{
		Kind:                     kind,
		GroupColumn:              groupColumn,
		ValueColumn:              valueColumn,
		DistinctColumn:           distinctColumn,
		ColumnAssetReadIntegrity: collections.ColumnAssetReadIntegritySkipChecksums,
	}
	if columnStoreUsesAggregateMetadata(cfg.StorageLayout, query) {
		req.AggregateMetadataName = columnStoreAggregateMetadataName
		switch query {
		case "q4":
			req.TopK = 3
			req.TopKOrder = collections.ColumnPhysicalQueryTopKInt64Asc
			req.SkipEmptyGroupKey = true
		case "q5":
			req.TopK = 3
			req.TopKOrder = collections.ColumnPhysicalQueryTopKInt64Desc
			req.SkipEmptyGroupKey = true
		}
	}
	switch query {
	case "q3":
		req.Predicates = []collections.ColumnPhysicalQueryPredicate{
			{Column: "kind", Value: "commit"},
			{Column: "operation", Value: "create"},
			{Column: "event", Kind: collections.ColumnPhysicalQueryPredicateInList, Values: columnStoreQ3Events()},
		}
	case "q4", "q5":
		if req.AggregateMetadataName == "" {
			req.Predicates = []collections.ColumnPhysicalQueryPredicate{
				{Column: "kind", Value: "commit"},
				{Column: "operation", Value: "create"},
				{Column: "event", Value: "app.bsky.feed.post"},
			}
		}
	}
	return req
}

func columnStoreQ3Events() []string {
	return []string{"app.bsky.feed.post", "app.bsky.feed.repost", "app.bsky.feed.like"}
}

type preparedColumnQuery struct {
	name     string
	count    *collections.ColumnPhysicalQueryRunner
	distinct *collections.ColumnPhysicalQueryRunner
}

func prepareColumnQueryIfNeeded(collection *collections.Collection, cfg runConfig, name string) (*preparedColumnQuery, error) {
	if !isPreparedColumnStoreLayout(cfg.StorageLayout) {
		return nil, nil
	}
	prepare := func(req collections.ColumnPhysicalQueryRequest) (*collections.ColumnPhysicalQueryRunner, error) {
		return collection.PrepareColumnPhysicalQuery(req)
	}
	switch name {
	case "q1":
		runner, err := prepare(columnPhysicalRequest(cfg, "q1", collections.ColumnPhysicalQueryGroupCount, "event", "", ""))
		if err != nil {
			return nil, err
		}
		return &preparedColumnQuery{name: name, count: runner}, nil
	case "q2":
		count, err := prepare(columnPhysicalRequest(cfg, "q2", collections.ColumnPhysicalQueryGroupCount, "event", "", ""))
		if err != nil {
			return nil, err
		}
		distinct, err := prepare(columnPhysicalRequest(cfg, "q2", collections.ColumnPhysicalQueryGroupCountDistinct, "event", "", "did"))
		if err != nil {
			if closeErr := count.Close(); closeErr != nil {
				return nil, errors.Join(err, closeErr)
			}
			return nil, err
		}
		return &preparedColumnQuery{name: name, count: count, distinct: distinct}, nil
	case "q3":
		runner, err := prepare(columnPhysicalRequest(cfg, "q3", collections.ColumnPhysicalQueryGroupHourCount, "event", "time_us", ""))
		if err != nil {
			return nil, err
		}
		return &preparedColumnQuery{name: name, count: runner}, nil
	case "q4":
		runner, err := prepare(columnPhysicalRequest(cfg, "q4", collections.ColumnPhysicalQueryGroupMinInt64, "did", "time_us", ""))
		if err != nil {
			return nil, err
		}
		return &preparedColumnQuery{name: name, count: runner}, nil
	case "q5":
		runner, err := prepare(columnPhysicalRequest(cfg, "q5", collections.ColumnPhysicalQueryGroupInt64Span, "did", "time_us", ""))
		if err != nil {
			return nil, err
		}
		return &preparedColumnQuery{name: name, count: runner}, nil
	default:
		return nil, nil
	}
}

func (p *preparedColumnQuery) Close() error {
	if p == nil {
		return nil
	}
	var errs []error
	if p.distinct != nil {
		if err := p.distinct.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if p.count != nil {
		if err := p.count.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (p *preparedColumnQuery) Run(rows int) (queryComputation, error) {
	if p == nil || p.count == nil {
		return queryComputation{}, errors.New("prepared column query is not initialized")
	}
	countResult, err := p.count.Run()
	if err != nil {
		return queryComputation{}, err
	}
	switch p.name {
	case "q1":
		return renderColumnQ1(rows, countResult), nil
	case "q2":
		if p.distinct == nil {
			return queryComputation{}, errors.New("prepared column q2 query is missing distinct runner")
		}
		distinctResult, err := p.distinct.Run()
		if err != nil {
			return queryComputation{}, err
		}
		return renderColumnQ2(rows, countResult, distinctResult), nil
	case "q3":
		return renderColumnQ3(rows, countResult), nil
	case "q4":
		return renderColumnQ4(rows, countResult), nil
	case "q5":
		return renderColumnQ5(rows, countResult), nil
	default:
		return queryComputation{}, fmt.Errorf("prepared column query %q is unsupported", p.name)
	}
}

func renderColumnQ1(rows int, result collections.ColumnPhysicalQueryResult) queryComputation {
	// JSONBench q1 groups every document by collection, including an empty event
	// bucket when the source document has no commit.collection field. Keep that
	// behavior aligned with the row-layout q1 implementation.
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
	return queryComputation{RowsScanned: columnPhysicalRowsScanned(rows, result), Rows: out}
}

func renderColumnQ2(rows int, countResult, distinctResult collections.ColumnPhysicalQueryResult) queryComputation {
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
	return queryComputation{RowsScanned: maxColumnPhysicalRowsScanned(rows, countResult, distinctResult), Rows: out}
}

func renderColumnQ3(rows int, result collections.ColumnPhysicalQueryResult) queryComputation {
	out := make([]queryRow, 0, len(result.Groups))
	for _, group := range result.Groups {
		if group.Key == "" {
			continue
		}
		out = append(out, queryRow{"event": group.Key, "hour_of_day": int64(group.Hour), "count": int64(group.Count)})
	}
	sort.Slice(out, func(i, j int) bool {
		hi := out[i]["hour_of_day"].(int64)
		hj := out[j]["hour_of_day"].(int64)
		if hi != hj {
			return hi < hj
		}
		return out[i]["event"].(string) < out[j]["event"].(string)
	})
	return queryComputation{RowsScanned: columnPhysicalRowsScanned(rows, result), Rows: out}
}

func renderColumnQ4(rows int, result collections.ColumnPhysicalQueryResult) queryComputation {
	out := make([]queryRow, 0, 3)
	for _, group := range result.Groups {
		if group.Key == "" {
			continue
		}
		row := queryRow{"user_id": group.Key, "first_post_time_us": group.Int64}
		insertTopQueryRow(&out, row, 3, lessQ4Row)
	}
	return queryComputation{RowsScanned: columnPhysicalRowsScanned(rows, result), Rows: out}
}

func renderColumnQ5(rows int, result collections.ColumnPhysicalQueryResult) queryComputation {
	out := make([]queryRow, 0, 3)
	for _, group := range result.Groups {
		if group.Key == "" {
			continue
		}
		row := queryRow{"user_id": group.Key, "activity_span_ms": group.Int64 / 1000}
		insertTopQueryRow(&out, row, 3, lessQ5Row)
	}
	return queryComputation{RowsScanned: columnPhysicalRowsScanned(rows, result), Rows: out}
}

func insertTopQueryRow(rows *[]queryRow, row queryRow, limit int, less func(queryRow, queryRow) bool) {
	if limit <= 0 {
		return
	}
	out := *rows
	if len(out) < limit {
		out = append(out, row)
		sort.Slice(out, func(i, j int) bool { return less(out[i], out[j]) })
		*rows = out
		return
	}
	if !less(row, out[len(out)-1]) {
		return
	}
	out[len(out)-1] = row
	sort.Slice(out, func(i, j int) bool { return less(out[i], out[j]) })
}

func lessQ4Row(a, b queryRow) bool {
	at := a["first_post_time_us"].(int64)
	bt := b["first_post_time_us"].(int64)
	if at != bt {
		return at < bt
	}
	return a["user_id"].(string) < b["user_id"].(string)
}

func lessQ5Row(a, b queryRow) bool {
	as := a["activity_span_ms"].(int64)
	bs := b["activity_span_ms"].(int64)
	if as != bs {
		return as > bs
	}
	return a["user_id"].(string) < b["user_id"].(string)
}

func columnPhysicalRowsScanned(fallback int, result collections.ColumnPhysicalQueryResult) int {
	if result.Diagnostics.MetadataHits > 0 || result.Diagnostics.DecodedMetadataBytes > 0 {
		return 0
	}
	if result.Diagnostics.RowsScanned > 0 {
		return result.Diagnostics.RowsScanned
	}
	// Older or fallback physical reducers may not populate RowsScanned even when
	// they reduce all loaded rows. Report the loaded-row fallback in that case so
	// scan-shaped cells are conservatively accounted instead of undercounted.
	return fallback
}

func maxColumnPhysicalRowsScanned(fallback int, results ...collections.ColumnPhysicalQueryResult) int {
	if len(results) == 0 {
		return fallback
	}
	maxRows := 0
	for _, result := range results {
		rows := columnPhysicalRowsScanned(fallback, result)
		if rows > maxRows {
			maxRows = rows
		}
	}
	return maxRows
}
