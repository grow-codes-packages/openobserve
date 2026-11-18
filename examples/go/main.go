// Command example shows the OpenObserve Go SDK against a real server: health
// and version, ingesting and searching logs, a batching LogClient, tracing with
// a query back, and stream management.
//
//	OPENOBSERVE_URL=http://127.0.0.1:5080 OPENOBSERVE_USER=root@example.com \
//	OPENOBSERVE_PASSWORD=... go run .
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	openobserve "github.com/grow-codes-packages/openobserve/go"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client, err := openobserve.NewClientFromEnv()
	if err != nil {
		return err // CONFIG error naming the variable, never its value
	}
	defer func() { _ = client.Close() }()
	fmt.Println("config:", client.Config()) // credentials print as [REDACTED]

	health, err := client.Health(ctx)
	if err != nil {
		return err
	}
	version, err := client.ServerVersion(ctx)
	if errors.Is(err, openobserve.ErrAuthentication) {
		return fmt.Errorf("check OPENOBSERVE_USER / OPENOBSERVE_PASSWORD: %w", err)
	} else if err != nil {
		return err
	}
	fmt.Printf("health=%s version=%s\n", health.Status, version)

	// 1. Ingest a few rows and read them back with SQL.
	stream := fmt.Sprintf("example_%d", time.Now().Unix())
	defer func() { _ = client.Streams().DeleteStream(context.Background(), stream, openobserve.StreamTypeLogs) }()
	res, err := client.Logs().IngestJSON(ctx, stream, []map[string]any{
		{"message": "user signed in", "level": "info", "user": map[string]any{"id": 7}},
		{"message": "payment failed", "level": "error", "amount": 12.5},
	})
	if err != nil {
		return err
	}
	fmt.Printf("ingested %d (failed %d)\n", res.Successful, res.Failed)

	sql := fmt.Sprintf(`SELECT * FROM "%s" WHERE level = 'error'`, stream)
	found, err := client.Logs().Search(ctx, sql, openobserve.SQLSearchOptions{TrackTotalHits: true})
	if err != nil {
		return err
	}
	fmt.Printf("errors: %d, first: %v\n", *found.Total, found.Hits[0]["message"])

	// 2. A batching LogClient with the Logger facade; Flush before reading back.
	logs, err := client.NewLogClient(stream, openobserve.LogClientOptions{
		Resource: map[string]openobserve.AttrValue{"service.name": "example"},
	})
	if err != nil {
		return err
	}
	logs.Logger("checkout", nil).Warn(ctx, "cart abandoned", "cart_id", "c-42", "password", "never-sent")
	if err := logs.Flush(ctx); err != nil {
		return err
	}
	rec, err := client.LogQuery(stream).WaitForLog(ctx, "cart abandoned", openobserve.WaitForLogOptions{Timeout: 30 * time.Second})
	if err != nil {
		return err
	}
	fmt.Printf("found %q severity=%s password=%v\n", rec.Body, rec.SeverityText, rec.Attributes["password"])

	// 3. Tracing: export spans over OTLP and query the trace back.
	tracing, err := client.InitTracing(ctx, openobserve.TracingConfig{ServiceName: "example", DisableSetGlobal: true})
	if err != nil {
		return err
	}
	defer func() { _ = tracing.Shutdown(context.Background()) }()
	sctx, span := tracing.Tracer("example").Start(ctx, "checkout")
	_, child := tracing.Tracer("example").Start(sctx, "charge card")
	child.End()
	span.End()
	if err := tracing.ForceFlush(ctx); err != nil {
		return err
	}
	trace, err := client.Traces().WaitForTrace(ctx, span.SpanContext().TraceID().String(), openobserve.WaitForTraceOptions{MinSpans: 2})
	if err != nil {
		return err
	}
	fmt.Printf("trace %s has %d spans\n", trace.TraceID, len(trace.Spans))

	// 4. Stream settings.
	days := 7
	if err := client.Streams().UpdateStreamSettings(ctx, stream, openobserve.StreamSettingsUpdate{RetentionDays: &days}); err != nil {
		return err
	}
	info, err := client.Streams().GetStreamSchema(ctx, stream, openobserve.StreamTypeLogs)
	if err != nil {
		return err
	}
	fmt.Printf("stream %s: %d fields, retention %d days\n", info.Name, len(info.Schema), info.Settings.RetentionDays)
	return nil
}
