package openobserve

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Streams manages OpenObserve streams (Client.Streams).
type Streams struct{ c *Client }

// Streams returns the stream management API.
func (c *Client) Streams() *Streams { return &Streams{c: c} }

// SchemaField is one field of a stream schema.
type SchemaField struct {
	Name string `json:"name"`
	// Type is the Arrow type name ("Utf8", "Int64", "Float64", "Boolean", …).
	Type string `json:"type"`
}

// PartitionKey is a partition key of a stream.
type PartitionKey struct {
	Field string `json:"field"`
	// Types is the partition type, "value" (default) or a hash spec as JSON text.
	Types string `json:"types"`
	// Disabled is true for a key that was removed (OpenObserve keeps it disabled).
	Disabled bool `json:"disabled"`
}

// StreamSettings are the settings of a stream.
type StreamSettings struct {
	PartitionKeys       []PartitionKey `json:"partitionKeys"`
	FullTextSearchKeys  []string       `json:"fullTextSearchKeys"`
	IndexFields         []string       `json:"indexFields"`
	BloomFilterFields   []string       `json:"bloomFilterFields"`
	DistinctValueFields []string       `json:"distinctValueFields"`
	// RetentionDays is the data retention in days (0 = server default).
	RetentionDays int `json:"retentionDays"`
	// MaxQueryRange is the maximum query range in hours (0 = unlimited).
	MaxQueryRange int `json:"maxQueryRange"`
}

// StreamStats are the storage statistics of a stream.
type StreamStats struct {
	CreatedAt      time.Time `json:"createdAt"`
	DocCount       int64     `json:"docCount"`
	FileCount      int64     `json:"fileCount"`
	StorageSize    float64   `json:"storageSize"`
	CompressedSize float64   `json:"compressedSize"`
}

// StreamInfo describes a stream with its schema and settings.
type StreamInfo struct {
	Name        string         `json:"name"`
	Type        StreamType     `json:"type"`
	StorageType string         `json:"storageType"`
	Stats       StreamStats    `json:"stats"`
	Schema      []SchemaField  `json:"schema"`
	Settings    StreamSettings `json:"settings"`
	TotalFields int            `json:"totalFields"`
}

type wireStream struct {
	Name        string         `json:"name"`
	StreamType  string         `json:"stream_type"`
	StorageType string         `json:"storage_type"`
	Stats       map[string]any `json:"stats"`
	Schema      []SchemaField  `json:"schema"`
	Settings    struct {
		PartitionKeys       map[string]json.RawMessage `json:"partition_keys"`
		FullTextSearchKeys  []string                   `json:"full_text_search_keys"`
		IndexFields         []string                   `json:"index_fields"`
		BloomFilterFields   []string                   `json:"bloom_filter_fields"`
		DistinctValueFields []json.RawMessage          `json:"distinct_value_fields"`
		DataRetention       int                        `json:"data_retention"`
		MaxQueryRange       int                        `json:"max_query_range"`
	} `json:"settings"`
	TotalFields int `json:"total_fields"`
}

func (w wireStream) info() StreamInfo {
	out := StreamInfo{
		Name: w.Name, Type: StreamType(w.StreamType), StorageType: w.StorageType, TotalFields: w.TotalFields,
		Schema: w.Schema,
		Settings: StreamSettings{
			FullTextSearchKeys: nonNilStrings(w.Settings.FullTextSearchKeys), IndexFields: nonNilStrings(w.Settings.IndexFields),
			BloomFilterFields: nonNilStrings(w.Settings.BloomFilterFields), DistinctValueFields: []string{},
			RetentionDays: w.Settings.DataRetention, MaxQueryRange: w.Settings.MaxQueryRange,
			PartitionKeys: partitionKeys(w.Settings.PartitionKeys),
		},
	}
	if out.Schema == nil {
		out.Schema = []SchemaField{}
	}
	for _, raw := range w.Settings.DistinctValueFields {
		var name string
		if json.Unmarshal(raw, &name) != nil {
			var obj struct {
				Name string `json:"name"`
			}
			_ = json.Unmarshal(raw, &obj)
			name = obj.Name
		}
		if name != "" {
			out.Settings.DistinctValueFields = append(out.Settings.DistinctValueFields, name)
		}
	}
	if us, ok := toInt64(w.Stats["created_at"]); ok && us > 0 {
		out.Stats.CreatedAt = fromMicros(us)
	}
	out.Stats.DocCount, _ = toInt64(w.Stats["doc_num"])
	out.Stats.FileCount, _ = toInt64(w.Stats["file_num"])
	out.Stats.StorageSize = toFloat(w.Stats["storage_size"])
	out.Stats.CompressedSize = toFloat(w.Stats["compressed_size"])
	return out
}

func nonNilStrings(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func toFloat(v any) float64 {
	if n, ok := v.(json.Number); ok {
		f, _ := n.Float64()
		return f
	}
	return 0
}

// partitionKeys decodes {"L0": {...}, "L1": {...}} in level order.
func partitionKeys(m map[string]json.RawMessage) []PartitionKey {
	levels := make([]string, 0, len(m))
	for k := range m {
		levels = append(levels, k)
	}
	sort.Slice(levels, func(i, j int) bool {
		a, _ := strconv.Atoi(strings.TrimPrefix(levels[i], "L"))
		b, _ := strconv.Atoi(strings.TrimPrefix(levels[j], "L"))
		return a < b
	})
	out := make([]PartitionKey, 0, len(m))
	for _, k := range levels {
		var raw struct {
			Field    string          `json:"field"`
			Types    json.RawMessage `json:"types"`
			Disabled bool            `json:"disabled"`
		}
		if json.Unmarshal(m[k], &raw) != nil {
			continue
		}
		pk := PartitionKey{Field: raw.Field, Disabled: raw.Disabled}
		if json.Unmarshal(raw.Types, &pk.Types) != nil {
			pk.Types = string(raw.Types)
		}
		out = append(out, pk)
	}
	return out
}

// ListStreamsOptions narrow Streams.ListStreams.
type ListStreamsOptions struct {
	// Type is the stream type (default logs; always sent, since the server
	// otherwise mixes every type).
	Type StreamType
	// Keyword keeps streams whose name contains it.
	Keyword string
	// Offset and Limit page through the list (0 = server default).
	Offset int
	Limit  int
}

// StreamList is one page of streams; Total counts every match.
type StreamList struct {
	Streams []StreamInfo `json:"streams"`
	Total   int          `json:"total"`
}

// ListStreams lists streams of one type (GET /api/{org}/streams).
func (s *Streams) ListStreams(ctx context.Context, opts ListStreamsOptions) (StreamList, error) {
	q := url.Values{"type": {string(opts.Type.orLogs())}}
	if opts.Keyword != "" {
		q.Set("keyword", opts.Keyword)
	}
	if opts.Offset > 0 {
		q.Set("offset", strconv.Itoa(opts.Offset))
	}
	if opts.Limit > 0 {
		q.Set("limit", strconv.Itoa(opts.Limit))
	}
	var body struct {
		List  []wireStream `json:"list"`
		Total int          `json:"total"`
	}
	if err := s.c.getJSON(ctx, "streams.listStreams", &request{Method: http.MethodGet, Path: s.c.orgPath("/streams"), Query: q}, &body); err != nil {
		return StreamList{}, err
	}
	out := StreamList{Streams: make([]StreamInfo, 0, len(body.List)), Total: body.Total}
	for _, w := range body.List {
		out.Streams = append(out.Streams, w.info())
	}
	return out, nil
}

// GetStreamSchema returns the schema, settings and statistics of a stream (GET
// /api/{org}/streams/{stream}/schema). NOT_FOUND when the stream (of that type)
// does not exist.
func (s *Streams) GetStreamSchema(ctx context.Context, stream string, streamType StreamType) (StreamInfo, error) {
	const op = "streams.getStreamSchema"
	name, err := streamArg(op, stream)
	if err != nil {
		return StreamInfo{}, err
	}
	var w wireStream
	err = s.c.getJSON(ctx, op, &request{
		Method: http.MethodGet, Path: s.c.orgPath("/streams/" + name + "/schema"),
		Query: url.Values{"type": {string(streamType.orLogs())}},
	}, &w)
	if err != nil {
		return StreamInfo{}, err
	}
	return w.info(), nil
}

// StreamSettingsUpdate changes stream settings. A nil field is left unchanged; a
// non-nil slice replaces the current list (the SDK sends the add/remove deltas
// OpenObserve expects). Removed partition keys stay in the schema as disabled.
type StreamSettingsUpdate struct {
	// Type is the stream type (default logs).
	Type StreamType
	// RetentionDays sets the data retention in days.
	RetentionDays *int
	// PartitionKeys replaces the enabled partition keys (Types defaults to "value").
	PartitionKeys []PartitionKey
	// FullTextSearchKeys replaces the full-text search keys.
	FullTextSearchKeys []string
	// IndexFields replaces the secondary index fields.
	IndexFields []string
	// BloomFilterFields replaces the bloom filter fields.
	BloomFilterFields []string
}

// UpdateStreamSettings applies upd (PUT /api/{org}/streams/{stream}/settings).
func (s *Streams) UpdateStreamSettings(ctx context.Context, stream string, upd StreamSettingsUpdate) error {
	const op = "streams.updateStreamSettings"
	name, err := streamArg(op, stream)
	if err != nil {
		return err
	}
	if upd.RetentionDays != nil && *upd.RetentionDays < 0 {
		return ValidationError(op, "retentionDays must not be negative")
	}
	body := map[string]any{}
	if upd.RetentionDays != nil {
		body["data_retention"] = *upd.RetentionDays
	}
	if upd.PartitionKeys != nil || upd.FullTextSearchKeys != nil || upd.IndexFields != nil || upd.BloomFilterFields != nil {
		current, err := s.GetStreamSchema(ctx, name, upd.Type)
		if err != nil {
			return err
		}
		lists := []struct {
			key           string
			current, want []string
		}{
			{"full_text_search_keys", current.Settings.FullTextSearchKeys, upd.FullTextSearchKeys},
			{"index_fields", current.Settings.IndexFields, upd.IndexFields},
			{"bloom_filter_fields", current.Settings.BloomFilterFields, upd.BloomFilterFields},
		}
		for _, l := range lists {
			if l.want == nil {
				continue
			}
			if add, remove := stringDelta(l.current, l.want); len(add)+len(remove) > 0 {
				body[l.key] = map[string]any{"add": add, "remove": remove}
			}
		}
		if upd.PartitionKeys != nil {
			if pk := partitionDelta(current.Settings.PartitionKeys, upd.PartitionKeys); pk != nil {
				body["partition_keys"] = pk
			}
		}
	}
	if len(body) == 0 {
		return nil
	}
	_, err = s.c.do(ctx, &request{
		Method: http.MethodPut, Path: s.c.orgPath("/streams/" + name + "/settings"),
		Query: url.Values{"type": {string(upd.Type.orLogs())}}, JSON: body, Operation: op,
	})
	return err
}

// stringDelta returns what to add to and remove from current to get want.
func stringDelta(current, want []string) (add, remove []string) {
	add, remove = []string{}, []string{}
	have := map[string]bool{}
	for _, c := range current {
		have[c] = true
	}
	wanted := map[string]bool{}
	for _, w := range want {
		if !wanted[w] && !have[w] {
			add = append(add, w)
		}
		wanted[w] = true
	}
	for _, c := range current {
		if !wanted[c] {
			remove = append(remove, c)
		}
	}
	return add, remove
}

func partitionDelta(current, want []PartitionKey) map[string]any {
	type wirePK struct {
		Field string `json:"field"`
		Types string `json:"types"`
	}
	enabled := map[string]PartitionKey{}
	for _, c := range current {
		if !c.Disabled {
			enabled[c.Field] = c
		}
	}
	wanted := map[string]bool{}
	add, remove := []wirePK{}, []wirePK{}
	for _, w := range want {
		types := w.Types
		if types == "" {
			types = "value"
		}
		if _, ok := enabled[w.Field]; !ok && !wanted[w.Field] {
			add = append(add, wirePK{Field: w.Field, Types: types})
		}
		wanted[w.Field] = true
	}
	for _, c := range current {
		if !c.Disabled && !wanted[c.Field] {
			remove = append(remove, wirePK{Field: c.Field, Types: c.Types})
		}
	}
	if len(add)+len(remove) == 0 {
		return nil
	}
	return map[string]any{"add": add, "remove": remove}
}

// DeleteStream deletes a stream and its data (DELETE /api/{org}/streams/{stream}).
// NOT_FOUND when it does not exist. Related alerts and dashboards are kept.
func (s *Streams) DeleteStream(ctx context.Context, stream string, streamType StreamType) error {
	const op = "streams.deleteStream"
	name, err := streamArg(op, stream)
	if err != nil {
		return err
	}
	_, err = s.c.do(ctx, &request{
		Method: http.MethodDelete, Path: s.c.orgPath("/streams/" + name),
		Query: url.Values{"type": {string(streamType.orLogs())}}, Operation: op,
	})
	return err
}

// CreateStreamOptions configure Streams.CreateStream.
type CreateStreamOptions struct {
	// Type is the stream type (default logs).
	Type StreamType
	// Fields pre-declare schema fields.
	Fields []SchemaField
	// Settings are the initial settings (PartitionKeys, FullTextSearchKeys,
	// IndexFields, BloomFilterFields and RetentionDays are sent).
	Settings StreamSettings
}

// CreateStream creates a stream ahead of ingestion (POST /api/{org}/streams/{stream}).
func (s *Streams) CreateStream(ctx context.Context, stream string, opts CreateStreamOptions) error {
	const op = "streams.createStream"
	name, err := streamArg(op, stream)
	if err != nil {
		return err
	}
	pks := make([]map[string]string, 0, len(opts.Settings.PartitionKeys))
	for _, pk := range opts.Settings.PartitionKeys {
		types := pk.Types
		if types == "" {
			types = "value"
		}
		pks = append(pks, map[string]string{"field": pk.Field, "types": types})
	}
	fields := opts.Fields
	if fields == nil {
		fields = []SchemaField{}
	}
	body := map[string]any{
		"fields": fields,
		"settings": map[string]any{
			"partition_keys":        pks,
			"full_text_search_keys": nonNilStrings(opts.Settings.FullTextSearchKeys),
			"index_fields":          nonNilStrings(opts.Settings.IndexFields),
			"bloom_filter_fields":   nonNilStrings(opts.Settings.BloomFilterFields),
			"data_retention":        opts.Settings.RetentionDays,
		},
	}
	_, err = s.c.do(ctx, &request{
		Method: http.MethodPost, Path: s.c.orgPath("/streams/" + name),
		Query: url.Values{"type": {string(opts.Type.orLogs())}}, JSON: body, Operation: op,
	})
	return err
}
