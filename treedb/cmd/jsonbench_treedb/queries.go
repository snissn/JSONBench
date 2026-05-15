package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/tidwall/gjson"
)

var querySQL = map[string]string{
	"q1": "SELECT data.commit.collection AS event, count() AS count FROM bluesky GROUP BY event ORDER BY count DESC",
	"q2": "SELECT data.commit.collection AS event, count() AS count, count(DISTINCT data.did) AS users FROM bluesky WHERE data.kind = 'commit' AND data.commit.operation = 'create' GROUP BY event ORDER BY count DESC",
	"q3": "SELECT data.commit.collection AS event, hour(to_timestamp(data.time_us / 1000000)) AS hour_of_day, count() AS count FROM bluesky WHERE data.kind = 'commit' AND data.commit.operation = 'create' AND data.commit.collection IN (...) GROUP BY event, hour_of_day ORDER BY hour_of_day, event",
	"q4": "SELECT data.did AS user_id, to_timestamp(min(data.time_us) / 1000000) AS first_post_date FROM bluesky WHERE data.kind = 'commit' AND data.commit.operation = 'create' AND data.commit.collection = 'app.bsky.feed.post' GROUP BY user_id ORDER BY first_post_date ASC LIMIT 3",
	"q5": "SELECT data.did AS user_id, date_diff('milliseconds', min(data.time_us), max(data.time_us)) AS activity_span FROM bluesky WHERE data.kind = 'commit' AND data.commit.operation = 'create' AND data.commit.collection = 'app.bsky.feed.post' GROUP BY user_id ORDER BY activity_span DESC LIMIT 3",
}

func runQueries(collection *collections.Collection, cfg runConfig, rows int) ([]queryRun, error) {
	out := make([]queryRun, 0, len(cfg.Queries))
	for _, name := range cfg.Queries {
		var attempts []float64
		var final queryComputation
		for i := 0; i < cfg.Tries; i++ {
			start := time.Now()
			computed, err := runQueryOnce(collection, cfg, name, rows)
			if err != nil {
				return nil, fmt.Errorf("%s attempt %d: %w", name, i+1, err)
			}
			attempts = append(attempts, seconds(time.Since(start)))
			final = computed
		}
		best, median := bestMedian(attempts)
		hash, err := hashRows(final.Rows)
		if err != nil {
			return nil, err
		}
		out = append(out, queryRun{
			Name:        name,
			SQL:         querySQL[name],
			AttemptsSec: attempts,
			BestSec:     best,
			MedianSec:   median,
			RowsScanned: final.RowsScanned,
			ResultRows:  len(final.Rows),
			ResultHash:  hash,
			Preview:     previewRows(final.Rows, 5),
		})
	}
	return out, nil
}

type queryComputation struct {
	RowsScanned int
	Rows        []queryRow
}

func runQueryOnce(collection *collections.Collection, cfg runConfig, name string, rows int) (queryComputation, error) {
	switch name {
	case "q1":
		return runQ1(collection, cfg, rows)
	case "q2":
		return runQ2(collection, cfg, rows)
	case "q3":
		return runQ3(collection, cfg, rows)
	case "q4":
		return runQ4(collection, cfg, rows)
	case "q5":
		return runQ5(collection, cfg, rows)
	default:
		return queryComputation{}, fmt.Errorf("unknown query %q", name)
	}
}

func runQ1(collection *collections.Collection, cfg runConfig, rows int) (queryComputation, error) {
	counts := make(map[string]int64)
	scanned, err := scanCollectionJSON(collection, rows, func(raw []byte) error {
		event := jsonString(raw, cfg.Projection, "event")
		counts[event]++
		return nil
	})
	if err != nil {
		return queryComputation{}, err
	}
	out := make([]queryRow, 0, len(counts))
	for event, count := range counts {
		out = append(out, queryRow{"event": event, "count": count})
	}
	sort.Slice(out, func(i, j int) bool {
		ci := out[i]["count"].(int64)
		cj := out[j]["count"].(int64)
		if ci != cj {
			return ci > cj
		}
		return out[i]["event"].(string) < out[j]["event"].(string)
	})
	return queryComputation{RowsScanned: scanned, Rows: out}, nil
}

func runQ2(collection *collections.Collection, cfg runConfig, rows int) (queryComputation, error) {
	type aggregate struct {
		count int64
		users map[string]struct{}
	}
	counts := make(map[string]*aggregate)
	scanned, err := scanCollectionJSON(collection, rows, func(raw []byte) error {
		if jsonString(raw, cfg.Projection, "kind") != "commit" || jsonString(raw, cfg.Projection, "operation") != "create" {
			return nil
		}
		event := jsonString(raw, cfg.Projection, "event")
		agg := counts[event]
		if agg == nil {
			agg = &aggregate{users: make(map[string]struct{})}
			counts[event] = agg
		}
		agg.count++
		agg.users[jsonString(raw, cfg.Projection, "did")] = struct{}{}
		return nil
	})
	if err != nil {
		return queryComputation{}, err
	}
	out := make([]queryRow, 0, len(counts))
	for event, agg := range counts {
		out = append(out, queryRow{"event": event, "count": agg.count, "users": int64(len(agg.users))})
	}
	sort.Slice(out, func(i, j int) bool {
		ci := out[i]["count"].(int64)
		cj := out[j]["count"].(int64)
		if ci != cj {
			return ci > cj
		}
		return out[i]["event"].(string) < out[j]["event"].(string)
	})
	return queryComputation{RowsScanned: scanned, Rows: out}, nil
}

func runQ3(collection *collections.Collection, cfg runConfig, rows int) (queryComputation, error) {
	type key struct {
		event string
		hour  int
	}
	allowed := map[string]struct{}{
		"app.bsky.feed.post":   {},
		"app.bsky.feed.repost": {},
		"app.bsky.feed.like":   {},
	}
	counts := make(map[key]int64)
	scanned, err := scanCollectionJSON(collection, rows, func(raw []byte) error {
		if jsonString(raw, cfg.Projection, "kind") != "commit" || jsonString(raw, cfg.Projection, "operation") != "create" {
			return nil
		}
		event := jsonString(raw, cfg.Projection, "event")
		if _, ok := allowed[event]; !ok {
			return nil
		}
		hour := time.UnixMicro(jsonTimeUS(raw, cfg.Projection)).UTC().Hour()
		counts[key{event: event, hour: hour}]++
		return nil
	})
	if err != nil {
		return queryComputation{}, err
	}
	out := make([]queryRow, 0, len(counts))
	for k, count := range counts {
		out = append(out, queryRow{"event": k.event, "hour_of_day": int64(k.hour), "count": count})
	}
	sort.Slice(out, func(i, j int) bool {
		hi := out[i]["hour_of_day"].(int64)
		hj := out[j]["hour_of_day"].(int64)
		if hi != hj {
			return hi < hj
		}
		return out[i]["event"].(string) < out[j]["event"].(string)
	})
	return queryComputation{RowsScanned: scanned, Rows: out}, nil
}

func runQ4(collection *collections.Collection, cfg runConfig, rows int) (queryComputation, error) {
	first := make(map[string]int64)
	scanned, err := scanCollectionJSON(collection, rows, func(raw []byte) error {
		if !isCreatedPost(raw, cfg.Projection) {
			return nil
		}
		user := jsonString(raw, cfg.Projection, "did")
		ts := jsonTimeUS(raw, cfg.Projection)
		if current, ok := first[user]; !ok || ts < current {
			first[user] = ts
		}
		return nil
	})
	if err != nil {
		return queryComputation{}, err
	}
	out := make([]queryRow, 0, len(first))
	for user, ts := range first {
		out = append(out, queryRow{"user_id": user, "first_post_time_us": ts})
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
	return queryComputation{RowsScanned: scanned, Rows: out}, nil
}

func runQ5(collection *collections.Collection, cfg runConfig, rows int) (queryComputation, error) {
	type span struct {
		min int64
		max int64
	}
	spans := make(map[string]span)
	scanned, err := scanCollectionJSON(collection, rows, func(raw []byte) error {
		if !isCreatedPost(raw, cfg.Projection) {
			return nil
		}
		user := jsonString(raw, cfg.Projection, "did")
		ts := jsonTimeUS(raw, cfg.Projection)
		current, ok := spans[user]
		if !ok {
			spans[user] = span{min: ts, max: ts}
			return nil
		}
		if ts < current.min {
			current.min = ts
		}
		if ts > current.max {
			current.max = ts
		}
		spans[user] = current
		return nil
	})
	if err != nil {
		return queryComputation{}, err
	}
	out := make([]queryRow, 0, len(spans))
	for user, span := range spans {
		out = append(out, queryRow{"user_id": user, "activity_span_ms": (span.max - span.min) / 1000})
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
	return queryComputation{RowsScanned: scanned, Rows: out}, nil
}

func scanCollectionJSON(collection *collections.Collection, maxDocs int, fn func(raw []byte) error) (int, error) {
	if maxDocs <= 0 {
		maxDocs = int(^uint(0) >> 1)
	}
	materializer, err := collection.NewStoredDocumentJSONMaterializer()
	if err != nil {
		return 0, err
	}
	defer func() { _ = materializer.Close() }()
	scanned := 0
	_, err = collection.ScanDocumentsFunc(maxDocs, func(record collections.DocumentRecord) (bool, error) {
		raw, err := materializer.StoredDocumentJSON(record.Document)
		if err != nil {
			return false, err
		}
		scanned++
		if err := fn(raw); err != nil {
			return false, err
		}
		return true, nil
	})
	return scanned, err
}

func isCreatedPost(raw []byte, projection string) bool {
	return jsonString(raw, projection, "kind") == "commit" &&
		jsonString(raw, projection, "operation") == "create" &&
		jsonString(raw, projection, "event") == "app.bsky.feed.post"
}

func jsonString(raw []byte, projection, logical string) string {
	if projection == "full" {
		switch logical {
		case "event":
			return gjson.GetBytes(raw, "commit.collection").String()
		case "did":
			return gjson.GetBytes(raw, "did").String()
		case "kind":
			return gjson.GetBytes(raw, "kind").String()
		case "operation":
			return gjson.GetBytes(raw, "commit.operation").String()
		}
	}
	switch logical {
	case "event":
		return gjson.GetBytes(raw, "event").String()
	case "did":
		return gjson.GetBytes(raw, "did").String()
	case "kind":
		return gjson.GetBytes(raw, "kind").String()
	case "operation":
		return gjson.GetBytes(raw, "operation").String()
	default:
		return ""
	}
}

func jsonTimeUS(raw []byte, projection string) int64 {
	if projection == "full" {
		return jsonInt64(gjson.GetBytes(raw, "time_us"))
	}
	return jsonInt64(gjson.GetBytes(raw, "time_us"))
}

func previewRows(rows []queryRow, n int) []queryRow {
	if len(rows) == 0 || n <= 0 {
		return nil
	}
	if len(rows) < n {
		n = len(rows)
	}
	out := make([]queryRow, n)
	for i := 0; i < n; i++ {
		out[i] = cloneQueryRow(rows[i])
	}
	return out
}

func cloneQueryRow(row queryRow) queryRow {
	out := make(queryRow, len(row))
	for k, v := range row {
		out[k] = v
	}
	return out
}

func bestMedian(values []float64) (float64, float64) {
	if len(values) == 0 {
		return 0, 0
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	best := sorted[0]
	mid := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return best, sorted[mid]
	}
	return best, (sorted[mid-1] + sorted[mid]) / 2
}

func rowsJSON(rows []queryRow) []byte {
	raw, _ := json.Marshal(rows)
	return raw
}
