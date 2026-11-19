package openobserve

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

// normalizeStreamName applies the server's stream-name rule: lower-case, every
// character other than [a-z0-9_] becomes "_" ("Sdk-Mixed.x" → "sdk_mixed_x").
func normalizeStreamName(name string) string {
	return normalizeKey(name)
}

// normalizeKey lower-cases a field or stream name and replaces every character
// other than [a-z0-9_] with "_" (OpenObserve's flattening rule).
func normalizeKey(name string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(strings.TrimSpace(name)) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	return b.String()
}

// streamArg validates and normalizes a stream argument.
func streamArg(op, stream string) (string, error) {
	s := normalizeStreamName(stream)
	if s == "" {
		return "", ValidationError(op, "stream name is required")
	}
	return s, nil
}

// sqlIdent quotes an identifier for OpenObserve SQL ("default" is reserved).
func sqlIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// sqlString quotes a string literal (single quotes doubled).
func sqlString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// isSQL reports whether query is raw SQL (starts with SELECT, case-insensitive).
func isSQL(query string) bool {
	q := strings.TrimSpace(query)
	return len(q) >= 6 && strings.EqualFold(q[:6], "select")
}

// DefaultSearchWindow is the time window searched when a call gives no start.
const DefaultSearchWindow = 15 * time.Minute

// timeWindow resolves [start, end]: end defaults to now and start to
// end - fallback. start after end is a VALIDATION error.
func timeWindow(op string, start, end time.Time, fallback time.Duration) (time.Time, time.Time, error) {
	if end.IsZero() {
		end = time.Now()
	}
	if start.IsZero() {
		start = end.Add(-fallback)
	}
	if start.After(end) {
		return start, end, ValidationError(op, "start must not be after end")
	}
	return start, end, nil
}

// micros formats t as Unix microseconds (the unit of every OpenObserve time
// parameter).
func micros(t time.Time) string { return strconv.FormatInt(t.UnixMicro(), 10) }

// gzipBytes compresses data.
func gzipBytes(data []byte) []byte {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, _ = zw.Write(data) // writes to a bytes.Buffer never fail
	_ = zw.Close()
	return buf.Bytes()
}

// toInt64 converts a decoded JSON number (json.Number, float64, int64, numeric
// string) to int64.
func toInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return i, true
		}
		if f, err := x.Float64(); err == nil {
			return int64(f), true
		}
	case int64:
		return x, true
	case float64:
		return int64(x), true
	case string:
		if i, err := strconv.ParseInt(x, 10, 64); err == nil {
			return i, true
		}
	}
	return 0, false
}

// toString renders a decoded JSON scalar as text ("" for nil).
func toString(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case json.Number:
		return x.String()
	case bool:
		return strconv.FormatBool(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	}
	raw, _ := json.Marshal(v)
	return string(raw)
}

// fromMicros converts Unix microseconds to a UTC time.
func fromMicros(us int64) time.Time { return time.UnixMicro(us).UTC() }
