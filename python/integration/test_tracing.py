"""init_tracing + client.traces against the real service, plus the tracing conformance suite."""

from __future__ import annotations

import time
from typing import Any

import pytest
from factories import SERVICE_NAME, TRACING_CAPABILITIES, tracing_target

import grow_openobserve as sdk
from grow_openobserve.testing import ConformanceRunner, assert_scenario, pytest_params


@pytest.fixture
def traced(client: sdk.OpenObserveClient, rid: str) -> dict[str, Any]:
    """One exported trace: root (server) → db.query (client, error, event, link) → decode."""
    service = f"py-svc-{rid}"
    handle = client.init_tracing(sdk.TracingConfig(service_name=service, set_global=False))
    tracer = handle.tracer("integration")
    with tracer.start_as_current_span(
        "GET /checkout", kind=sdk.span_kind("server"), attributes={"run": rid}
    ) as root:
        with tracer.start_as_current_span(
            "db.query", kind=sdk.span_kind("client"), attributes={"db.system": "postgresql"}
        ) as db:
            db.add_event("cache.miss", {"key": "u1"})
            sdk.mark_span_error(db, "boom")
            with tracer.start_as_current_span("row.decode"):
                time.sleep(0.01)
    handle.force_flush(30)
    handle.shutdown()
    handle.shutdown()
    return {"service": service, "trace_id": format(root.get_span_context().trace_id, "032x")}


def test_get_trace_round_trip(client: sdk.OpenObserveClient, traced: dict[str, Any]) -> None:
    trace = client.traces.wait_for_trace(
        traced["trace_id"], timeout_s=30, interval_s=0.25, min_spans=3
    )
    assert trace.trace_id == traced["trace_id"]
    by_name = {s.name: s for s in trace.spans}
    assert set(by_name) == {"GET /checkout", "db.query", "row.decode"}
    root, db, decode = by_name["GET /checkout"], by_name["db.query"], by_name["row.decode"]
    assert root.parent_span_id is None
    assert db.parent_span_id == root.span_id
    assert decode.parent_span_id == db.span_id
    assert root.kind == "server" and db.kind == "client" and decode.kind == "internal"
    assert db.status.code == "error"
    assert db.attributes["db_system"] == "postgresql"
    assert [e.name for e in db.events] == ["cache.miss"]
    assert root.service_name == traced["service"]
    assert root.duration_micros >= decode.duration_micros
    assert client.traces.get_trace("ffffffffffffffffffffffffffffffff") is None


def test_search_and_list(client: sdk.OpenObserveClient, traced: dict[str, Any]) -> None:
    service = traced["service"]
    client.traces.wait_for_trace(traced["trace_id"], timeout_s=30, interval_s=0.25, min_spans=3)
    found = client.traces.search_traces(sdk.TraceSearchQuery(service=service))
    assert [s.trace_id for s in found] == [traced["trace_id"]]
    assert found[0].root_name == "GET /checkout"
    assert found[0].span_count == 3
    by_tag = client.traces.search_traces(
        sdk.TraceSearchQuery(service=service, tags={"db.system": "postgresql"})
    )
    assert [s.trace_id for s in by_tag] == [traced["trace_id"]]
    assert client.traces.search_traces(sdk.TraceSearchQuery(service=f"{service}-none")) == []
    recent = client.traces.search_traces(sdk.TraceSearchQuery(limit=50))
    assert traced["trace_id"] in [s.trace_id for s in recent]

    assert service in client.traces.list_services()
    assert client.traces.list_operations(service) == ["GET /checkout", "db.query", "row.decode"]

    time_range = client.traces.trace_time_range(traced["trace_id"])
    assert time_range.found
    assert time_range.stream == client.traces.stream
    assert time_range.start_micros and time_range.end_micros
    assert not client.traces.trace_time_range("ffffffffffffffffffffffffffffffff").found

    with pytest.raises(sdk.UnsupportedError):
        client.traces.get_dependencies()


def test_trace_query_on_missing_stream(client: sdk.OpenObserveClient, rid: str) -> None:
    missing = client.trace_query(f"no_traces_{rid}")
    assert missing.get_trace("4bf92f3577b34da6a3ce929d0e0e4736") is None
    assert missing.search_traces() == []
    assert missing.list_services() == []


@pytest.mark.parametrize("scenario", pytest_params(categories=["tracing"]))
def test_tracing_conformance(env: dict[str, str], scenario: dict[str, Any]) -> None:
    runner = ConformanceRunner(
        {"tracing": lambda ctx: tracing_target(env, ctx)},
        capabilities=TRACING_CAPABILITIES,
        service_name=SERVICE_NAME,
    )
    assert_scenario(runner, scenario)
