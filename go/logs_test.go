package openobserve

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

const fxStream = "sdk_logs_6moiazbq"

func TestIngest(t *testing.T) {
	m := newMock(t, map[string][]string{
		"POST /api/default/" + fxStream + "/_json":          {"ingestJson.ok"},
		"POST /api/default/sdk_logs_6moiazbq_partial/_json": {"ingestJson.200_partial_failure"},
		"POST /api/default/bad/_json":                       {"ingestJson.400_invalid_json"},
		"POST /api/default/sdk_mixed_6moiazbq/_json":        {"ingestJson.stream_name_normalized"},
		"POST /api/default/" + fxStream + "/_multi":         {"ingestMulti.ok"},
		"POST /api/default/badmulti/_multi":                 {"ingestMulti.400_invalid_line"},
		"POST /api/default/_bulk":                           {"ingestBulk.ok", "ingestBulk.200_item_error"},
	})
	c := newOOClient(t, m.URL, nil)
	logs := c.Logs()

	t.Run("ingestJson", func(t *testing.T) {
		res, err := logs.IngestJSON(t.Context(), fxStream, []map[string]any{{"message": "a", "n": 1}, {"message": "b"}})
		if err != nil || res.Successful != 3 || res.Failed != 0 || len(res.Streams) != 1 || res.Streams[0].Name != fxStream {
			t.Fatalf("%+v %v", res, err)
		}
		r := m.last()
		if r.Header.Get("Content-Type") != "application/json" {
			t.Fatal(r.Header)
		}
		if arr, ok := jsonBody(t, r).([]any); !ok || len(arr) != 2 {
			t.Fatalf("body %s", r.Body)
		}
	})
	t.Run("ingestJson partial failure stays 200", func(t *testing.T) {
		res, err := logs.IngestJSON(t.Context(), "sdk_logs_6moiazbq_partial", []map[string]any{{"message": "x"}})
		if err != nil || res.Successful != 1 || res.Failed != 1 || !strings.Contains(res.Streams[0].Error, "Too old") {
			t.Fatalf("%+v %v", res, err)
		}
	})
	t.Run("ingestJson errors", func(t *testing.T) {
		if _, err := logs.IngestJSON(t.Context(), "bad", []map[string]any{{"a": 1}}); !errors.Is(err, ErrValidation) {
			t.Fatal(err)
		}
		if _, err := logs.IngestJSON(t.Context(), "  ", []map[string]any{{"a": 1}}); !errors.Is(err, ErrValidation) {
			t.Fatal(err)
		}
		if _, err := logs.IngestJSON(t.Context(), "s", nil); !errors.Is(err, ErrValidation) {
			t.Fatal(err)
		}
		if _, err := logs.IngestJSON(t.Context(), "s", []map[string]any{{"f": func() {}}}); !errors.Is(err, ErrValidation) {
			t.Fatal(err)
		}
	})
	t.Run("stream names are normalized", func(t *testing.T) {
		res, err := logs.IngestJSON(t.Context(), "Sdk-Mixed.6moiazbq", []map[string]any{{"m": 1}})
		if err != nil || res.Streams[0].Name != "sdk_mixed_6moiazbq" || m.last().Path != "/api/default/sdk_mixed_6moiazbq/_json" {
			t.Fatalf("%+v %v", res, err)
		}
	})
	t.Run("ingestMulti", func(t *testing.T) {
		res, err := logs.IngestMulti(t.Context(), fxStream, []byte("{\"m\":1}\n{\"m\":2}\n"))
		if err != nil || res.Successful != 2 {
			t.Fatalf("%+v %v", res, err)
		}
		if m.last().Header.Get("Content-Type") != "application/x-ndjson" {
			t.Fatal("content type")
		}
		if _, err := logs.IngestMulti(t.Context(), "badmulti", []byte("{")); !errors.Is(err, ErrValidation) {
			t.Fatal(err)
		}
		if _, err := logs.IngestMulti(t.Context(), fxStream, []byte(" \n")); !errors.Is(err, ErrValidation) {
			t.Fatal(err)
		}
		if _, err := logs.IngestMulti(t.Context(), "", []byte("{}")); !errors.Is(err, ErrValidation) {
			t.Fatal(err)
		}
	})
	t.Run("ingestBulk", func(t *testing.T) {
		res, err := logs.IngestBulk(t.Context(), []BulkAction{
			{Index: "sdk_bulk_6moiazbq", Document: map[string]any{"message": "one"}},
			{Action: BulkCreate, Index: "sdk_bulk_6moiazbq", Document: map[string]any{"message": "two"}},
		})
		if err != nil || res.Errors || len(res.Items) != 2 || res.Items[0].Status != 200 || res.Items[0].Index != "sdk_bulk_6moiazbq" || res.Items[0].Action != "index" {
			t.Fatalf("%+v %v", res, err)
		}
		body := string(m.last().Body)
		if !strings.HasSuffix(body, "\n") || !strings.Contains(body, `{"create":{"_index":"sdk_bulk_6moiazbq"}}`) || strings.Count(body, "\n") != 4 {
			t.Fatalf("ndjson %q", body)
		}
		res, err = logs.IngestBulk(t.Context(), []BulkAction{{Index: "sdk_bulk_6moiazbq", Document: map[string]any{"message": "old"}}})
		if err != nil || !res.Errors || res.Items[0].Status != 422 || !strings.HasPrefix(res.Items[0].Error, "timestamp_parsing_failed: Too old") {
			t.Fatalf("%+v %v", res, err)
		}
	})
	t.Run("ingestBulk rejects delete and update client side", func(t *testing.T) {
		n := len(m.requests())
		for _, a := range []BulkAction{
			{Action: "delete", Index: "s", Document: map[string]any{}},
			{Action: "update", Index: "s", Document: map[string]any{}},
			{Index: "", Document: map[string]any{}},
			{Index: "s"},
			{Index: "s", Document: map[string]any{"f": func() {}}},
		} {
			if _, err := logs.IngestBulk(t.Context(), []BulkAction{a}); !errors.Is(err, ErrValidation) {
				t.Fatalf("%+v: %v", a, err)
			}
		}
		if _, err := logs.IngestBulk(t.Context(), nil); !errors.Is(err, ErrValidation) {
			t.Fatal(err)
		}
		if len(m.requests()) != n {
			t.Fatal("invalid actions must not reach the server")
		}
	})
	t.Run("invalid response JSON", func(t *testing.T) {
		bad := newMock(t, nil)
		bad.handle("POST /api/default/s/_json", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("x")) })
		bad.handle("POST /api/default/_bulk", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("x")) })
		bad.handle("POST /api/default/s/_multi", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(401) })
		bc := newOOClient(t, bad.URL, nil)
		if _, err := bc.Logs().IngestJSON(t.Context(), "s", []map[string]any{{"a": 1}}); !errors.Is(err, ErrUnavailable) {
			t.Fatal(err)
		}
		if _, err := bc.Logs().IngestBulk(t.Context(), []BulkAction{{Index: "s", Document: map[string]any{}}}); !errors.Is(err, ErrUnavailable) {
			t.Fatal(err)
		}
		if _, err := bc.Logs().IngestBulk(t.Context(), []BulkAction{{Index: "s", Document: map[string]any{}}}); err == nil {
			t.Fatal("expected error")
		}
		if _, err := bc.Logs().IngestMulti(t.Context(), "s", []byte("{}")); !errors.Is(err, ErrAuthentication) {
			t.Fatal(err)
		}
		if res, err := ingestResult("op", &response{Body: []byte(`{"code":200}`)}); err != nil || res.Streams == nil {
			t.Fatal(res, err)
		}
	})
}

func TestSearch(t *testing.T) {
	m := newMock(t, map[string][]string{"POST /api/default/_search": {"search.ok"}})
	c := newOOClient(t, m.URL, nil)
	start := time.UnixMicro(1795029600490792)
	end := time.UnixMicro(1795033260490792)

	t.Run("rows and request shape", func(t *testing.T) {
		res, err := c.Logs().Search(t.Context(), "SELECT * FROM x", SQLSearchOptions{Start: start, End: end})
		if err != nil || len(res.Hits) != 5 || res.Total != nil || res.SearchID == "" || res.ScanRecords != 5 || res.Took != 5 || res.Size != 100 {
			t.Fatalf("%+v %v", res, err)
		}
		if ts, ok := res.Hits[0]["_timestamp"].(int64); !ok || ts != 1795033200440568 {
			t.Fatalf("timestamp %T %v", res.Hits[0]["_timestamp"], res.Hits[0]["_timestamp"])
		}
		r := m.last()
		if r.Query["type"][0] != "logs" {
			t.Fatal(r.Query)
		}
		q := jsonBody(t, r).(map[string]any)["query"].(map[string]any)
		if q["start_time"].(json.Number).String() != "1795029600490792" || q["end_time"].(json.Number).String() != "1795033260490792" || q["sql"] != "SELECT * FROM x" || q["size"].(json.Number).String() != "100" {
			t.Fatalf("%v", q)
		}
		if _, ok := q["track_total_hits"]; ok {
			t.Fatal("no track_total_hits by default")
		}
	})
	t.Run("track total hits runs a count request", func(t *testing.T) {
		m := newMock(t, map[string][]string{"POST /api/default/_search": {"search.paging", "search.track_total_hits"}})
		res, err := newOOClient(t, m.URL, nil).Logs().Search(t.Context(), "SELECT * FROM x", SQLSearchOptions{From: 1, Size: 2, TrackTotalHits: true, StreamType: StreamTypeLogs})
		if err != nil || len(res.Hits) != 2 || res.Total == nil || *res.Total != 5 || res.From != 1 {
			t.Fatalf("%+v %v", res, err)
		}
		q := jsonBody(t, m.last()).(map[string]any)["query"].(map[string]any)
		if q["track_total_hits"] != true {
			t.Fatal(q)
		}
	})
	t.Run("count request failure", func(t *testing.T) {
		m := newMock(t, map[string][]string{"POST /api/default/_search": {"search.ok", "search.401"}})
		if _, err := newOOClient(t, m.URL, nil).Logs().Search(t.Context(), "SELECT 1", SQLSearchOptions{TrackTotalHits: true}); !errors.Is(err, ErrAuthentication) {
			t.Fatal(err)
		}
		m2 := newMock(t, nil)
		m2.handle("POST /api/default/_search", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"hits":[]}`)) })
		res, err := newOOClient(t, m2.URL, nil).Logs().Search(t.Context(), "SELECT 1", SQLSearchOptions{TrackTotalHits: true})
		if err != nil || *res.Total != 0 {
			t.Fatal(err)
		}
	})
	t.Run("default window is the last 15 minutes", func(t *testing.T) {
		before := time.Now()
		if _, err := c.Logs().Search(t.Context(), "SELECT 1", SQLSearchOptions{}); err != nil {
			t.Fatal(err)
		}
		q := jsonBody(t, m.last()).(map[string]any)["query"].(map[string]any)
		s, _ := q["start_time"].(json.Number).Int64()
		e, _ := q["end_time"].(json.Number).Int64()
		if e-s != DefaultSearchWindow.Microseconds() || e < before.UnixMicro() {
			t.Fatalf("window %d..%d", s, e)
		}
	})
	t.Run("validation", func(t *testing.T) {
		if _, err := c.Logs().Search(t.Context(), " ", SQLSearchOptions{}); !errors.Is(err, ErrValidation) {
			t.Fatal(err)
		}
		if _, err := c.Logs().Search(t.Context(), "SELECT 1", SQLSearchOptions{Start: end, End: start}); !errors.Is(err, ErrValidation) {
			t.Fatal(err)
		}
	})
	t.Run("server errors", func(t *testing.T) {
		for fx, want := range map[string]error{"search.400_stream_not_found": ErrNotFound, "search.400_bad_sql": ErrValidation, "search.400_invalid_time_range": ErrValidation} {
			m := newMock(t, map[string][]string{"POST /api/default/_search": {fx}})
			if _, err := newOOClient(t, m.URL, nil).Logs().Search(t.Context(), "SELECT 1", SQLSearchOptions{}); !errors.Is(err, want) {
				t.Fatalf("%s: %v", fx, err)
			}
		}
	})
}

func TestSearchAroundValuesHistogram(t *testing.T) {
	m := newMock(t, map[string][]string{
		"GET /api/default/" + fxStream + "/_around": {"searchAround.ok"},
		"GET /api/default/missing/_around":          {"searchAround.400_stream_not_found"},
		"GET /api/default/" + fxStream + "/_values": {"valuesOf.ok"},
		"GET /api/default/novalues/_values":         {"valuesOf.400_missing_time"},
		"POST /api/default/_search":                 {"histogram.ok"},
	})
	c := newOOClient(t, m.URL, nil)
	anchor := time.UnixMicro(1795033200242779)

	t.Run("searchAround", func(t *testing.T) {
		res, err := c.Logs().SearchAround(t.Context(), fxStream, anchor, 10)
		if err != nil || len(res.Hits) != 5 {
			t.Fatalf("%+v %v", res, err)
		}
		r := m.last()
		if r.Query["key"][0] != "1795033200242779" || r.Query["size"][0] != "10" {
			t.Fatal(r.Query)
		}
		if _, err := c.Logs().SearchAround(t.Context(), fxStream, anchor, 0); err != nil || m.last().Query["size"][0] != "10" {
			t.Fatal("default size", err)
		}
		if _, err := c.Logs().SearchAround(t.Context(), "missing", anchor, 5); !errors.Is(err, ErrNotFound) {
			t.Fatal(err)
		}
		if _, err := c.Logs().SearchAround(t.Context(), fxStream, time.Time{}, 5); !errors.Is(err, ErrValidation) {
			t.Fatal(err)
		}
		if _, err := c.Logs().SearchAround(t.Context(), "", anchor, 5); !errors.Is(err, ErrValidation) {
			t.Fatal(err)
		}
	})
	t.Run("valuesOf", func(t *testing.T) {
		vals, err := c.Logs().ValuesOf(t.Context(), fxStream, []string{"level", "run_id"}, ValuesOptions{Size: 10})
		if err != nil || len(vals) != 2 || vals[0].Field != "level" || len(vals[0].Values) != 4 || vals[0].Values[0].Value != "debug" || vals[0].Values[0].Count != 2 {
			t.Fatalf("%+v %v", vals, err)
		}
		r := m.last()
		if r.Query["fields"][0] != "level,run_id" || r.Query["start_time"][0] == "" || r.Query["end_time"][0] == "" {
			t.Fatal(r.Query)
		}
		if _, err := c.Logs().ValuesOf(t.Context(), fxStream, []string{"level"}, ValuesOptions{}); err != nil || m.last().Query["size"][0] != "10" {
			t.Fatal(err)
		}
		if _, err := c.Logs().ValuesOf(t.Context(), "novalues", []string{"level"}, ValuesOptions{}); !errors.Is(err, ErrValidation) {
			t.Fatal(err)
		}
		if _, err := c.Logs().ValuesOf(t.Context(), fxStream, nil, ValuesOptions{}); !errors.Is(err, ErrValidation) {
			t.Fatal(err)
		}
		if _, err := c.Logs().ValuesOf(t.Context(), "", []string{"a"}, ValuesOptions{}); !errors.Is(err, ErrValidation) {
			t.Fatal(err)
		}
		if _, err := c.Logs().ValuesOf(t.Context(), fxStream, []string{"a"}, ValuesOptions{Start: anchor, End: anchor.Add(-time.Second)}); !errors.Is(err, ErrValidation) {
			t.Fatal(err)
		}
	})
	t.Run("histogram", func(t *testing.T) {
		h, err := c.Logs().Histogram(t.Context(), fxStream, HistogramOptions{Interval: time.Minute, Filter: "run_id = '6moiazbq'"})
		want := time.Date(2026, 11, 18, 20, 20, 0, 0, time.UTC)
		if err != nil || h.Interval != time.Minute || len(h.Buckets) != 1 || h.Buckets[0].Count != 5 || !h.Buckets[0].Start.Equal(want) {
			t.Fatalf("%+v %v", h, err)
		}
		sql := jsonBody(t, m.last()).(map[string]any)["query"].(map[string]any)["sql"].(string)
		wantSQL := `SELECT histogram(_timestamp, '1 minute') AS zo_sql_key, count(*) AS zo_sql_num FROM "sdk_logs_6moiazbq" WHERE run_id = '6moiazbq' GROUP BY zo_sql_key ORDER BY zo_sql_key`
		if sql != wantSQL {
			t.Fatalf("sql %s", sql)
		}
		if _, err := c.Logs().Histogram(t.Context(), fxStream, HistogramOptions{}); err != nil {
			t.Fatal(err)
		}
		if sql := jsonBody(t, m.last()).(map[string]any)["query"].(map[string]any)["sql"].(string); !strings.Contains(sql, "histogram(_timestamp) AS") || strings.Contains(sql, "WHERE") {
			t.Fatal(sql)
		}
		if _, err := c.Logs().Histogram(t.Context(), fxStream, HistogramOptions{Interval: -time.Second}); !errors.Is(err, ErrValidation) {
			t.Fatal(err)
		}
		if _, err := c.Logs().Histogram(t.Context(), "", HistogramOptions{}); !errors.Is(err, ErrValidation) {
			t.Fatal(err)
		}
		if _, err := c.Logs().Histogram(t.Context(), fxStream, HistogramOptions{Start: anchor, End: anchor.Add(-time.Second)}); !errors.Is(err, ErrValidation) {
			t.Fatal(err)
		}
		bad := newMock(t, map[string][]string{"POST /api/default/_search": {"search.401"}})
		if _, err := newOOClient(t, bad.URL, nil).Logs().Histogram(t.Context(), fxStream, HistogramOptions{}); !errors.Is(err, ErrAuthentication) {
			t.Fatal(err)
		}
	})
	t.Run("interval text and bucket parsing", func(t *testing.T) {
		for d, want := range map[time.Duration]string{2 * time.Hour: "2 hour", 5 * time.Minute: "5 minute", 30 * time.Second: "30 second", 90 * time.Second: "90 second", time.Millisecond: "1 second"} {
			if got := intervalText(d); got != want {
				t.Errorf("%s: %s", d, got)
			}
		}
		if !parseBucketTime("2026-11-18T20:03:00Z").Equal(time.Date(2026, 11, 18, 20, 3, 0, 0, time.UTC)) {
			t.Fatal("rfc3339")
		}
		if !parseBucketTime("1795033200000000").Equal(time.UnixMicro(1795033200000000)) {
			t.Fatal("micros")
		}
		if !parseBucketTime("garbage").IsZero() {
			t.Fatal("garbage")
		}
	})
}

func TestHelpers(t *testing.T) {
	if normalizeStreamName(" Sdk-Mixed.x ") != "sdk_mixed_x" {
		t.Fatal("normalize")
	}
	if sqlIdent(`a"b`) != `"a""b"` || sqlString("it's") != "'it''s'" {
		t.Fatal("quote")
	}
	if !isSQL("  select 1") || isSQL("selec") || isSQL("hello") {
		t.Fatal("isSQL")
	}
	for _, v := range []any{json.Number("7"), json.Number("7.9"), int64(7), float64(7.2), "7"} {
		if n, ok := toInt64(v); !ok || n != 7 {
			t.Errorf("toInt64(%v)=%d", v, n)
		}
	}
	for _, v := range []any{json.Number("x"), "x", true} {
		if _, ok := toInt64(v); ok {
			t.Errorf("toInt64(%v) ok", v)
		}
	}
	for v, want := range map[any]string{nil: "", "s": "s", json.Number("1"): "1", true: "true", int64(3): "3", 1.5: "1.5"} {
		if toString(v) != want {
			t.Errorf("toString(%v)", v)
		}
	}
	if toString([]any{"a"}) != `["a"]` {
		t.Fatal("toString slice")
	}
	if toFloat(json.Number("1.5")) != 1.5 || toFloat("x") != 0 {
		t.Fatal("toFloat")
	}
}

func TestLogsIngestOTLP(t *testing.T) {
	m := newMock(t, map[string][]string{"POST /api/default/v1/logs": {"otlpLogs.json"}})
	c := newOOClient(t, m.URL, nil)
	if res, err := c.Logs().IngestOTLP(t.Context(), nil, OTLPIngestOptions{Stream: "s"}); err != nil || res != (OTLPIngestResult{}) {
		t.Fatalf("empty: %+v %v", res, err)
	}
	if len(m.requests()) != 0 {
		t.Fatal("empty batch must not send")
	}
	res, err := c.Logs().IngestOTLP(t.Context(), []LogRecord{sampleRecord(), sampleRecord()}, OTLPIngestOptions{Stream: "Otlp.S", Gzip: true})
	if err != nil || res.Accepted != 2 || res.Rejected != 0 {
		t.Fatalf("%+v %v", res, err)
	}
	if r := m.last(); r.Header.Get("Stream-Name") != "otlp_s" || !r.Gzip {
		t.Fatal(r.Header)
	}
	bm := newMock(t, map[string][]string{"POST /api/default/v1/logs": {"otlpLogs.401"}})
	if _, err := newOOClient(t, bm.URL, nil).Logs().IngestOTLP(t.Context(), []LogRecord{{}}, OTLPIngestOptions{}); !errors.Is(err, ErrAuthentication) {
		t.Fatal(err)
	}
}

func TestLogsTail(t *testing.T) {
	var mu sync.Mutex
	polls := 0
	base := time.Now().Add(time.Second)
	row := func(i int) string {
		return fmt.Sprintf(`{"_timestamp":%d,"message":"row %d"}`, base.Add(time.Duration(i)*time.Millisecond).UnixMicro(), i)
	}
	m := newMock(t, nil)
	m.handle("POST /api/default/_search", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		polls++
		n := polls
		mu.Unlock()
		switch n {
		case 1:
			old := fmt.Sprintf(`{"_timestamp":%d,"message":"old"}`, base.Add(-time.Hour).UnixMicro())
			_, _ = w.Write([]byte(`{"hits":[` + old + `,` + row(0) + `,` + row(1) + `]}`))
		case 2:
			_, _ = w.Write([]byte(`{"hits":[` + row(1) + `,` + row(2) + `]}`))
		case 3:
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"code":400,"message":"bad sql"}`))
		default:
			_, _ = w.Write([]byte(`{"hits":[]}`))
		}
	})
	c := newOOClient(t, m.URL, nil)
	got := make(chan string, 10)
	errs := make(chan error, 10)
	h, err := c.Logs().Tail(t.Context(), "Tail.S", func(r map[string]any) { got <- r["message"].(string) }, LogTailOptions{
		Filter: "src = 'x'", Start: base, Interval: 5 * time.Millisecond, Overlap: 2 * time.Second, PageSize: 5,
		StreamType: StreamTypeLogs, OnError: func(err error) { errs <- err },
	})
	if err != nil {
		t.Fatal(err)
	}
	var rows []string
	for len(rows) < 3 {
		select {
		case b := <-got:
			rows = append(rows, b)
		case <-time.After(5 * time.Second):
			t.Fatalf("got %v", rows)
		}
	}
	select {
	case err := <-errs:
		if !errors.Is(err, ErrValidation) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no error reported")
	}
	h.Stop()
	h.Stop()
	if strings.Join(rows, ",") != "row 0,row 1,row 2" {
		t.Fatal(rows)
	}
	reqs := m.requests()
	q := jsonBody(t, reqs[0]).(map[string]any)["query"].(map[string]any)
	want := fmt.Sprintf(`SELECT * FROM "tail_s" WHERE _timestamp >= %d AND (src = 'x') ORDER BY _timestamp ASC`, base.UnixMicro())
	if q["sql"] != want || q["start_time"].(json.Number).String() != fmt.Sprint(base.UnixMicro()) || q["size"].(json.Number).String() != "5" {
		t.Fatalf("first poll never reaches before start: %v", q)
	}
	if _, err := c.Logs().Tail(t.Context(), " ", func(map[string]any) {}, LogTailOptions{}); !errors.Is(err, ErrValidation) {
		t.Fatal(err)
	}
	if _, err := c.Logs().Tail(t.Context(), "s", nil, LogTailOptions{}); !errors.Is(err, ErrValidation) {
		t.Fatal(err)
	}
	quiet, err := c.Logs().Tail(t.Context(), "s", func(map[string]any) {}, LogTailOptions{})
	if err != nil {
		t.Fatal(err)
	}
	quiet.Stop()
}
