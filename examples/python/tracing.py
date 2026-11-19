"""Send OpenTelemetry spans to OpenObserve and read the trace back."""

from __future__ import annotations

import grow_openobserve as sdk


def main() -> None:
    config = sdk.OpenObserveConfig.from_env()
    # OTLP/HTTP to <url>/api/{org}/v1/traces with Basic auth and the stream-name header.
    handle = sdk.init_tracing(
        sdk.TracingConfig(service_name="example-checkout", environment="dev"), openobserve=config
    )
    tracer = handle.tracer("example")
    with sdk.with_span("GET /checkout", kind="server", tracer=tracer) as root:
        trace_id = format(root.get_span_context().trace_id, "032x")
        with tracer.start_as_current_span("charge card"):
            sdk.add_event("card.validated", {"brand": "visa"})
            print("log correlation fields:", sdk.trace_fields())
    handle.force_flush(10)
    handle.shutdown()

    with sdk.OpenObserveClient(config) as client:
        trace = client.traces.wait_for_trace(trace_id, timeout_s=30)
        for s in trace.spans:
            print(f"{s.name} [{s.kind}] {s.duration_micros} µs status={s.status.code}")
        summaries = client.traces.search_traces(sdk.TraceSearchQuery(service="example-checkout"))
        print(f"{len(summaries)} recent traces; services: {client.traces.list_services()}")


if __name__ == "__main__":
    main()
