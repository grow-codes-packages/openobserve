"""client.traces + init_tracing with recorded responses."""

from __future__ import annotations

import gzip
import time
from typing import Any
from urllib.parse import parse_qs, urlsplit

import pytest
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
)

import grow_openobserve as sdk
from _support import MockServer, Reply, make_config, replay

TID = "038a1ed5af73678e47809f854dabaae9"
DETAILS = f"/api/default/sdk_traces/traces/{TID}/details"
LATEST = "/api/default/sdk_traces/traces/latest"
SEARCH = "/api/default/_search"


def cfg(server: MockServer, **kw: Any) -> sdk.OpenObserveConfig:
    return make_config(
        server.url, user="u@example.com", password="pw-secret", traces_stream="sdk_traces", **kw
    )


def oo(server: MockServer) -> sdk.OpenObserveClient:
    return sdk.OpenObserveClient(cfg(server))


def q(seen: Any) -> dict[str, list[str]]:
    return parse_qs(urlsplit(seen.path).query)


def test_get_trace_maps_rows(server: MockServer) -> None:
    server.enqueue(DETAILS, replay("getTrace.details"))
    trace = oo(server).traces.get_trace(TID.upper(), start=1, end=2)
    assert trace is not None and trace.trace_id == TID
    assert q(server.seen[-1]) == {"start_time": ["1"], "end_time": ["2"], "size": ["1000"]}
    root, db, decode = trace.spans
    assert (root.name, root.kind, root.status.code, root.parent_span_id) == (
        "GET /checkout",
        "server",
        "ok",
        None,
    )
    assert root.service_name == "sdk-a-6moiazbq"
    assert root.start_micros == 1795033200603151 and root.duration_micros == 50000
    assert root.attributes["http_status_code"] == "200"
    assert "_timestamp" not in root.attributes and "events" not in root.attributes
    assert (db.kind, db.status.code, db.status.message) == ("client", "error", "boom")
    assert db.parent_span_id == root.span_id
    assert db.events[0] == sdk.SpanEvent("cache.miss", 1795033200623151, {"key": "u1"})
    assert db.links[0].trace_id == "4bf92f3577b34da6a3ce929d0e0e4736"
    assert db.links[0].attributes == {"kind": "follows"}
    assert not any(k.startswith(("o2_db_", "infer_service_")) for k in db.attributes)
    assert decode.kind == "internal" and decode.status.code == "unset"


def test_get_trace_paginates_and_handles_missing(server: MockServer) -> None:
    server.enqueue(DETAILS, replay("getTrace.page1"), replay("getTrace.details"))
    trace = oo(server).traces.get_trace(TID)
    assert trace is not None and len(trace.spans) == 3
    second = q(server.seen[-1])
    assert second["after_start_time"] == ["1795033200603151382"]
    assert second["after_span_id"] == ["b7ad6b7169203331"]
    missing = f"/api/default/sdk_traces/traces/{'f' * 32}/details"
    server.enqueue(missing, replay("getTrace.not_found"), replay("getTrace.400_stream_not_found"))
    c = oo(server)
    assert c.traces.get_trace("f" * 32) is None
    assert c.traces.get_trace("f" * 32) is None
    with pytest.raises(sdk.ValidationError):
        c.traces.get_trace("not-hex")


def test_row_to_span_edge_cases() -> None:
    span = sdk.row_to_span(
        {
            "trace_id": TID,
            "span_id": "00f067aa0ba902b7",
            "start_time": "2000",
            "end_time": 1000,
            "duration": 7,
            "span_kind": "9",
            "status_code": 2,
            "events": "not json",
            "links": [
                {"context": {"trace_id": TID, "span_id": "00f067aa0ba902b8"}},
                {"context": {}},
            ],
            "extra": {"nested": 1},
            "skip": None,
        }
    )
    assert span.duration_micros == 7
    assert span.kind == "unspecified"
    assert span.status.code == "error"
    assert span.events == ()
    assert len(span.links) == 1
    assert span.attributes == {"extra": '{"nested":1}'}


def test_search_traces(server: MockServer) -> None:
    server.enqueue(
        LATEST,
        replay("searchTraces.latest"),
        replay("searchTraces.filter_service"),
        replay("searchTraces.filter_no_match"),
        Reply(400, {"code": 20002, "message": "Search stream not found: sdk_traces"}),
    )
    c = oo(server)
    found = c.traces.search_traces()
    assert [s.trace_id for s in found] == ["138a1ed5af73678e47809f854dabaae9", TID]
    assert found[1] == sdk.TraceSummary(
        TID, 1795033200603151, 50000, 3, "sdk-a-6moiazbq", "GET /checkout"
    )
    assert "filter" not in q(server.seen[-1])
    query = sdk.TraceSearchQuery(
        service="svc's",
        operation="op",
        tags={"db.system": "postgresql"},
        min_duration_micros=10,
        max_duration_micros=20,
        limit=1,
    )
    assert len(c.traces.search_traces(query)) == 1
    sent = q(server.seen[-1])
    assert sent["filter"] == [
        "service_name = 'svc''s' AND operation_name = 'op' AND db_system = 'postgresql' "
        "AND duration >= 10 AND duration <= 20"
    ]
    assert sent["size"] == ["1"]
    assert c.traces.search_traces(sdk.TraceSearchQuery(service="none")) == []
    assert c.traces.search_traces() == []
    for bad in (
        sdk.TraceSearchQuery(limit=0),
        sdk.TraceSearchQuery(tags={" ": "x"}),
        sdk.TraceSearchQuery(min_duration_micros=-1),
    ):
        with pytest.raises(sdk.ValidationError):
            c.traces.search_traces(bad)


def test_list_services_operations_and_dependencies(server: MockServer) -> None:
    server.enqueue(
        SEARCH,
        replay("listServices.ok"),
        replay("listOperations.ok"),
        Reply(400, {"code": 20002, "message": "Search stream not found: x"}),
    )
    c = oo(server)
    assert c.traces.list_services(start=1, end=2) == ["sdk-a-6moiazbq", "sdk-b-6moiazbq"]
    assert q(server.seen[-1])["type"] == ["traces"]
    assert c.traces.list_operations("o'k") == ["GET /checkout", "db.query", "row.decode"]
    assert b"service_name = 'o''k'" in server.seen[-1].body
    assert c.trace_query("x").list_services() == []
    with pytest.raises(sdk.UnsupportedError):
        c.traces.get_dependencies()


def test_trace_time_range_and_wait(server: MockServer) -> None:
    server.enqueue(
        "/api/default/traces/time_range",
        replay("traceTimeRange.ok"),
        replay("traceTimeRange.not_found"),
    )
    server.enqueue(DETAILS, replay("getTrace.not_found"), replay("getTrace.details"))
    c = oo(server)
    found = c.traces.trace_time_range(TID)
    assert found == sdk.TraceTimeRange(
        True, "sdk_traces_6moiazbq", 1795033200603151, 1795033200653152
    )
    assert not c.traces.trace_time_range("f" * 32).found
    trace = c.traces.wait_for_trace(TID, timeout_s=5, interval_s=0.01, min_spans=3)
    assert len(trace.spans) == 3


def test_endpoints_and_defaults() -> None:
    config = sdk.OpenObserveConfig(
        url="https://oo.example:5080/base/", org="acme", user="u@x.io", password="p"
    )
    assert sdk.traces_endpoint(config) == "https://oo.example:5080/base/api/acme/v1/traces"
    assert sdk.grpc_endpoint(config) == "https://oo.example:5081"
    assert sdk.grpc_endpoint(sdk.OpenObserveConfig(url="http://[::1]:9000")) == "http://[::1]:9000"
    defaults = sdk.tracing_defaults(config, "Spans")
    assert defaults["headers"]["stream-name"] == "spans"
    assert defaults["headers"]["authorization"].startswith("Basic ")
    assert "authorization" not in sdk.tracing_defaults(sdk.OpenObserveConfig())["headers"]


def test_init_tracing_exports_to_openobserve(server: MockServer) -> None:
    server.defaults["/api/default/v1/traces"] = Reply(
        200, b"", headers={"Content-Type": "application/x-protobuf"}
    )
    handle = sdk.init_tracing(
        sdk.TracingConfig(service_name="svc", set_global=False, headers={"x-extra": "1"}),
        openobserve=cfg(server),
    )
    with handle.tracer("t").start_as_current_span("root"):
        pass
    handle.force_flush(10)
    handle.shutdown()
    seen = next(s for s in server.seen if s.path == "/api/default/v1/traces")
    assert seen.headers["stream-name"] == "sdk_traces"
    assert seen.headers["authorization"].startswith("Basic ")
    assert seen.headers["x-extra"] == "1"
    assert seen.headers["Content-Encoding"] == "gzip"
    request = ExportTraceServiceRequest.FromString(gzip.decompress(seen.body))
    assert request.resource_spans[0].scope_spans[0].spans[0].name == "root"


def test_client_init_tracing_without_gzip_and_insecure(
    server: MockServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    server.defaults["/custom"] = Reply(200, b"", headers={"Content-Type": "application/x-protobuf"})
    c = sdk.OpenObserveClient(cfg(server, tls=sdk.TlsConfig(insecure_skip_verify=True)))
    handle = c.init_tracing(
        sdk.TracingConfig(
            service_name="svc",
            set_global=False,
            compression="none",
            endpoint=server.url + "/custom",
        ),
        stream="other",
    )
    with handle.tracer("t").start_as_current_span("x"):
        pass
    handle.shutdown()
    seen = next(s for s in server.seen if s.path == "/custom")
    assert seen.headers["stream-name"] == "other"
    assert "Content-Encoding" not in seen.headers
    for key, value in {
        "GROW_OPENOBSERVE_URL": server.url,
        "GROW_OPENOBSERVE_USER": "u@example.com",
        "GROW_OPENOBSERVE_PASSWORD": "pw",
    }.items():
        monkeypatch.setenv(key, value)
    env_handle = sdk.init_tracing(sdk.TracingConfig(service_name="svc", set_global=False))
    env_handle.shutdown()


def test_init_tracing_grpc_exporter() -> None:
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

    config = sdk.OpenObserveConfig(url="http://oo:5080", user="u@x.io", password="p")
    exporter = sdk.create_span_exporter(
        sdk.TracingConfig(service_name="s", protocol="grpc", headers={"Stream-Name": "t"}), config
    )
    assert isinstance(exporter, OTLPSpanExporter)
    exporter.shutdown()
    plain = sdk.create_span_exporter(
        sdk.TracingConfig(
            service_name="s", protocol="grpc", compression="none", endpoint="https://g:443"
        ),
        config,
    )
    plain.shutdown()
    handle = sdk.init_tracing(
        sdk.TracingConfig(service_name="s", protocol="grpc", set_global=False), openobserve=config
    )
    assert handle.config.endpoint == "http://oo:5081"
    handle.shutdown()
    with pytest.raises(sdk.ConfigError):
        sdk.init_tracing(sdk.TracingConfig(service_name=" "), openobserve=config)


async def test_async_traces(server: MockServer) -> None:
    server.enqueue(DETAILS, replay("getTrace.details"), replay("getTrace.details"))
    server.enqueue(LATEST, replay("searchTraces.latest"))
    server.enqueue(SEARCH, replay("listServices.ok"), replay("listOperations.ok"))
    server.enqueue("/api/default/traces/time_range", replay("traceTimeRange.ok"))
    async with sdk.AsyncOpenObserveClient(cfg(server)) as c:
        assert (await c.traces.get_trace(TID)) is not None
        assert len(await c.traces.search_traces()) == 2
        assert len(await c.traces.list_services()) == 2
        assert len(await c.traces.list_operations("s")) == 3
        assert (await c.traces.trace_time_range(TID)).found
        assert (await c.trace_query().wait_for_trace(TID, timeout_s=5)).trace_id == TID
        with pytest.raises(sdk.UnsupportedError):
            await c.traces.get_dependencies()
        handle = c.init_tracing(sdk.TracingConfig(service_name="svc", set_global=False))
        handle.shutdown()
    time.sleep(0)
