package openobserve

import (
	"encoding/json"
	"errors"
	"net/http"
	"testing"
)

func TestStreams(t *testing.T) {
	m := newMock(t, map[string][]string{
		"GET /api/default/streams":                            {"listStreams.logs"},
		"GET /api/default/streams/" + fxStream + "/schema":    {"getStreamSchema.after_settings"},
		"GET /api/default/streams/missing/schema":             {"getStreamSchema.404"},
		"PUT /api/default/streams/" + fxStream + "/settings":  {"updateStreamSettings.ok"},
		"DELETE /api/default/streams/sdk_bulk_6moiazbq":       {"deleteStream.ok", "deleteStream.404"},
		"POST /api/default/streams/sdk_logs_6moiazbq_created": {"createStream.ok"},
	})
	c := newOOClient(t, m.URL, nil)
	s := c.Streams()

	t.Run("listStreams", func(t *testing.T) {
		list, err := s.ListStreams(t.Context(), ListStreamsOptions{Keyword: "sdk", Offset: 1, Limit: 5})
		if err != nil || list.Total == 0 || len(list.Streams) == 0 {
			t.Fatalf("%+v %v", list, err)
		}
		first := list.Streams[0]
		if first.Name != "passcode_6moiazbq" || first.Type != StreamTypeLogs || first.StorageType != "disk" || first.Stats.CreatedAt.IsZero() || first.Schema == nil {
			t.Fatalf("%+v", first)
		}
		q := m.last().Query
		if q["type"][0] != "logs" || q["keyword"][0] != "sdk" || q["offset"][0] != "1" || q["limit"][0] != "5" {
			t.Fatal(q)
		}
		if _, err := s.ListStreams(t.Context(), ListStreamsOptions{Type: StreamTypeTraces}); err != nil || m.last().Query["type"][0] != "traces" {
			t.Fatal(err)
		}
	})
	t.Run("getStreamSchema", func(t *testing.T) {
		info, err := s.GetStreamSchema(t.Context(), fxStream, "")
		if err != nil || info.Name != fxStream || len(info.Schema) != 8 || info.Schema[1] != (SchemaField{Name: "level", Type: "Utf8"}) || info.TotalFields != 8 {
			t.Fatalf("%+v %v", info, err)
		}
		if info.Settings.RetentionDays != 7 || len(info.Settings.PartitionKeys) != 1 || info.Settings.PartitionKeys[0] != (PartitionKey{Field: "run_id", Types: "value", Disabled: true}) {
			t.Fatalf("%+v", info.Settings)
		}
		if _, err := s.GetStreamSchema(t.Context(), "missing", StreamTypeLogs); !errors.Is(err, ErrNotFound) {
			t.Fatal(err)
		}
		if _, err := s.GetStreamSchema(t.Context(), "", StreamTypeLogs); !errors.Is(err, ErrValidation) {
			t.Fatal(err)
		}
	})
	t.Run("updateStreamSettings sends deltas", func(t *testing.T) {
		days := 7
		err := s.UpdateStreamSettings(t.Context(), fxStream, StreamSettingsUpdate{
			RetentionDays: &days, FullTextSearchKeys: []string{"message", "message"}, PartitionKeys: []PartitionKey{{Field: "run_id"}},
		})
		if err != nil {
			t.Fatal(err)
		}
		r := m.last()
		if r.Method != http.MethodPut || r.Query["type"][0] != "logs" {
			t.Fatal(r.Method, r.Query)
		}
		body := jsonBody(t, r).(map[string]any)
		raw, _ := json.Marshal(body)
		want := `{"data_retention":7,"full_text_search_keys":{"add":["message"],"remove":[]},"partition_keys":{"add":[{"field":"run_id","types":"value"}],"remove":[]}}`
		if string(raw) != want {
			t.Fatalf("body %s", raw)
		}
	})
	t.Run("updateStreamSettings removal and no-op", func(t *testing.T) {
		current := []PartitionKey{{Field: "a", Types: "value"}, {Field: "b", Types: "value", Disabled: true}}
		pk := partitionDelta(current, []PartitionKey{})
		raw, _ := json.Marshal(pk)
		if string(raw) != `{"add":[],"remove":[{"field":"a","types":"value"}]}` {
			t.Fatal(string(raw))
		}
		if partitionDelta(current, []PartitionKey{{Field: "a"}}) != nil {
			t.Fatal("no change")
		}
		add, remove := stringDelta([]string{"x", "y"}, []string{"y", "z"})
		if len(add) != 1 || add[0] != "z" || len(remove) != 1 || remove[0] != "x" {
			t.Fatal(add, remove)
		}
		n := len(m.requests())
		if err := s.UpdateStreamSettings(t.Context(), fxStream, StreamSettingsUpdate{}); err != nil || len(m.requests()) != n {
			t.Fatal("empty update sends nothing", err)
		}
		// Current schema already has no FTS keys and a disabled partition key: nothing to send.
		if err := s.UpdateStreamSettings(t.Context(), fxStream, StreamSettingsUpdate{FullTextSearchKeys: []string{}, PartitionKeys: []PartitionKey{}}); err != nil {
			t.Fatal(err)
		}
		if last := m.last(); last.Method != http.MethodGet {
			t.Fatal("no PUT expected")
		}
	})
	t.Run("updateStreamSettings errors", func(t *testing.T) {
		neg := -1
		if err := s.UpdateStreamSettings(t.Context(), fxStream, StreamSettingsUpdate{RetentionDays: &neg}); !errors.Is(err, ErrValidation) {
			t.Fatal(err)
		}
		if err := s.UpdateStreamSettings(t.Context(), "", StreamSettingsUpdate{}); !errors.Is(err, ErrValidation) {
			t.Fatal(err)
		}
		if err := s.UpdateStreamSettings(t.Context(), "missing", StreamSettingsUpdate{FullTextSearchKeys: []string{"a"}}); !errors.Is(err, ErrNotFound) {
			t.Fatal(err)
		}
		bad := newMock(t, map[string][]string{"PUT /api/default/streams/x/settings": {"updateStreamSettings.404"}})
		one := 1
		if err := newOOClient(t, bad.URL, nil).Streams().UpdateStreamSettings(t.Context(), "x", StreamSettingsUpdate{RetentionDays: &one}); !errors.Is(err, ErrNotFound) {
			t.Fatal(err)
		}
	})
	t.Run("deleteStream", func(t *testing.T) {
		if err := s.DeleteStream(t.Context(), "sdk_bulk_6moiazbq", ""); err != nil {
			t.Fatal(err)
		}
		if q := m.last().Query; q["type"][0] != "logs" || q["delete_all"] != nil {
			t.Fatal(q)
		}
		if err := s.DeleteStream(t.Context(), "sdk_bulk_6moiazbq", StreamTypeLogs); !errors.Is(err, ErrNotFound) {
			t.Fatal(err)
		}
		if err := s.DeleteStream(t.Context(), "", StreamTypeLogs); !errors.Is(err, ErrValidation) {
			t.Fatal(err)
		}
	})
	t.Run("createStream", func(t *testing.T) {
		err := s.CreateStream(t.Context(), "sdk_logs_6moiazbq_created", CreateStreamOptions{
			Fields:   []SchemaField{{Name: "message", Type: "Utf8"}},
			Settings: StreamSettings{RetentionDays: 3, PartitionKeys: []PartitionKey{{Field: "run_id"}}},
		})
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(jsonBody(t, m.last()))
		want := `{"fields":[{"name":"message","type":"Utf8"}],"settings":{"bloom_filter_fields":[],"data_retention":3,"full_text_search_keys":[],"index_fields":[],"partition_keys":[{"field":"run_id","types":"value"}]}}`
		if string(raw) != want {
			t.Fatal(string(raw))
		}
		if err := s.CreateStream(t.Context(), "sdk_logs_6moiazbq_created", CreateStreamOptions{}); err != nil {
			t.Fatal(err)
		}
		if err := s.CreateStream(t.Context(), "", CreateStreamOptions{}); !errors.Is(err, ErrValidation) {
			t.Fatal(err)
		}
	})
	t.Run("listStreams error", func(t *testing.T) {
		bad := newMock(t, map[string][]string{"GET /api/default/streams": {"search.401"}})
		if _, err := newOOClient(t, bad.URL, nil).Streams().ListStreams(t.Context(), ListStreamsOptions{}); !errors.Is(err, ErrAuthentication) {
			t.Fatal(err)
		}
	})
}

func TestWireStreamDecoding(t *testing.T) {
	var w wireStream
	raw := `{"name":"s","settings":{"partition_keys":{"L1":{"field":"b","types":{"hash":8}},"L0":{"field":"a","types":"value"},"Lx":"bad"},
		"distinct_value_fields":["f1",{"name":"f2"},{"other":1}]},"stats":{"created_at":0}}`
	if err := json.Unmarshal([]byte(raw), &w); err != nil {
		t.Fatal(err)
	}
	info := w.info()
	pk := info.Settings.PartitionKeys
	if len(pk) != 2 || pk[0].Field != "a" || pk[1].Field != "b" || pk[1].Types != `{"hash":8}` {
		t.Fatalf("%+v", pk)
	}
	if len(info.Settings.DistinctValueFields) != 2 || info.Settings.DistinctValueFields[1] != "f2" || !info.Stats.CreatedAt.IsZero() {
		t.Fatalf("%+v", info.Settings)
	}
}
