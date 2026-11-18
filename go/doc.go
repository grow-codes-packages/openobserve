// Package openobserve is the Grow Codes SDK for OpenObserve (logs and traces).
//
// Create a client from the environment (GROW_OPENOBSERVE_URL / OPENOBSERVE_URL,
// OPENOBSERVE_ORG, OPENOBSERVE_USER plus OPENOBSERVE_PASSWORD or
// OPENOBSERVE_TOKEN, OPENOBSERVE_STREAM, OPENOBSERVE_TRACES_STREAM) or from a
// Config literal:
//
//	client, err := openobserve.NewClientFromEnv()
//	if err != nil {
//		return err
//	}
//	defer client.Close()
//	status, err := client.Health(ctx)
//
// The service APIs hang off the client:
//
//   - Client.Logs: IngestJSON, IngestMulti, IngestBulk, Search (SQL),
//     SearchAround, ValuesOf, Histogram.
//   - Client.LogQuery: the LogQueryClient (Search, Tail, ListStreams,
//     ListLabels, WaitForLog) over one stream.
//   - Client.NewLogClient / NewLogClientWith: a batching LogClient with Logger,
//     slog and zap adapters, writing through NewJSONLogExporter (default) or
//     NewOTLPLogExporter.
//   - Client.Streams: ListStreams, GetStreamSchema, UpdateStreamSettings,
//     DeleteStream, CreateStream.
//   - InitTracing / Client.InitTracing: OpenTelemetry tracing exported over
//     OTLP to /api/{org}/v1/traces; Client.Traces is the TraceQueryClient
//     (GetTrace, SearchTraces, ListServices, ListOperations, TraceTimeRange).
//   - Client.Admin: ListOrganizations, service accounts and the ingest-only
//     passcode.
//
// OpenObserve authenticates with HTTP Basic only: User with Password, or a
// service-account e-mail as User with its Token. Every time parameter is sent
// in microseconds; the SDK takes time.Time values and converts them.
//
// Every error is an *Error; compare codes with errors.Is against the Err*
// sentinels (errors.Is(err, openobserve.ErrNotFound)). OpenObserve's HTTP 400
// "stream not found" maps to NOT_FOUND and "already exists" to CONFLICT.
// Credentials are wrapped in Secret and never appear in errors, hooks or logs.
//
// OpenObserve is a trademark of ZincLabs, Inc. This package is not affiliated
// with or endorsed by ZincLabs, Inc.
package openobserve
