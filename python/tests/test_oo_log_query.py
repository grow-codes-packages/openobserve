"""Logging category on OpenObserve: records mapping, exporters, log client, log query."""

from __future__ import annotations

import asyncio
import gzip
import json
import threading
import time
from datetime import datetime, timezone
from typing import Any

import pytest

import grow_openobserve as sdk
from _support import MockServer, Reply, make_config, replay

SEARCH = "/api/default/_search"
JSON = "/api/default/app/_json"


def cfg(server: MockServer, **kw: Any) -> sdk.OpenObserveConfig:
    return make_config(server.url, user="u@example.com", password="pw", stream="app", **kw)


def rows(*items: dict[str, Any]) -> Reply:
    return Reply(200, {"hits": list(items), "total": len(items)})


def test_record_row_round_trip() -> None:
    ts = datetime(2026, 1, 2, 3, 4, 5, 678901, tzinfo=timezone.utc)
    record = sdk.LogRecord(
        body="hello",
        severity=sdk.Severity.ERROR,
        timestamp=ts,
        attributes={"user.id": 7, "tags": ["a", "b"], "map": {"k": [1]}},
        resource={"service.name": "svc", "host.name": "h"},
        trace_id="4bf92f3577b34da6a3ce929d0e0e4736",
        span_id="00f067aa0ba902b7",
        trace_flags=1,
    )
    row = sdk.log_record_to_row(record)
    assert row == {
        "service_name": "svc",
        "host_name": "h",
        "user_id": 7,
        "tags": '["a","b"]',
        "map": {"k": "[1]"},
        "_timestamp": 1767323045678901,
        "body": "hello",
        "severity": "ERROR",
        "severity_number": 17,
        "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
        "span_id": "00f067aa0ba902b7",
        "trace_flags": 1,
    }
    back = sdk.row_to_log_record({**row, "map": {"k": 1}, "none": None})
    assert back.timestamp == ts
    assert back.severity == sdk.Severity.ERROR and back.severity_text == "ERROR"
    assert back.resource == {"service.name": "svc", "host.name": "h"}
    assert back.attributes == {"user_id": 7, "tags": '["a","b"]', "map": '{"k":1}'}
    assert back.trace_flags == 1


@pytest.mark.parametrize(
    ("row", "severity", "text", "body"),
    [
        ({"message": "m", "level": "warn"}, sdk.Severity.WARN, "warn", "m"),
        ({"log": {"a": 1}, "severity_number": 10}, sdk.Severity.INFO, "INFO", '{"a":1}'),
        ({"msg": "x", "level": "verbose"}, sdk.Severity.INFO, "verbose", "x"),
        ({"_timestamp": "12"}, sdk.Severity.INFO, "INFO", ""),
        (
            {"body": "B", "severity_text": "DEBUG", "trace_flags": True},
            sdk.Severity.DEBUG,
            "DEBUG",
            "B",
        ),
    ],
)
def test_row_to_log_record_variants(
    row: dict[str, Any], severity: sdk.Severity, text: str, body: str
) -> None:
    record = sdk.row_to_log_record(row)
    assert (record.severity, record.severity_text, record.body) == (severity, text, body)
    assert record.trace_flags is None


def test_sql_helpers() -> None:
    assert sdk.is_sql_query("  select * from x")
    assert not sdk.is_sql_query("selected items")
    assert sdk.text_query_sql("s", "it's", "asc") == (
        "SELECT * FROM \"s\" WHERE match_all('it''s') ORDER BY _timestamp ASC"
    )
    assert sdk.text_query_sql("s", " ", "desc") == 'SELECT * FROM "s" ORDER BY _timestamp DESC'


def test_log_client_exports_with_json_ingest(server: MockServer) -> None:
    server.enqueue(JSON, replay("ingestJson.ok"), replay("ingestJson.200_partial_failure"))
    errors: list[Exception] = []
    with sdk.OpenObserveClient(cfg(server)) as c:
        log_client = c.log_client(
            resource={"service.name": "svc"},
            options=sdk.BatchOptions(max_export_retries=0, on_error=lambda e, r: errors.append(e)),
        )
        log_client.logger("orders").info("hello", {"password": "hunter2", "n": 1})
        log_client.flush(5)
        assert log_client.stats().sent == 1
        sent = json.loads(gzip.decompress(server.seen[-1].body))[0]
        assert sent["body"] == "hello" and sent["service_name"] == "svc"
        assert sent["password"] == "[REDACTED]" and sent["logger_name"] == "orders"
        log_client.send(sdk.LogInput(message="old"))
        log_client.send(sdk.LogInput(message="new"))
    stats = log_client.stats()
    assert stats.failed == 1 and stats.sent == 2
    assert "rejected" in str(errors[0])


def test_exporters_direct(server: MockServer) -> None:
    server.enqueue(JSON, replay("ingestJson.ok"))
    server.enqueue(
        "/api/default/v1/logs",
        replay("otlpLogs.json"),
        Reply(200, {"partialSuccess": {"rejectedLogRecords": 1}}),
    )
    c = sdk.OpenObserveClient(cfg(server))
    exporter = c.log_exporter()
    assert isinstance(exporter, sdk.OpenObserveLogExporter) and exporter.stream == "app"
    assert exporter.export([], gzip=False, timeout_s=1) == sdk.ExportResult(0, 0)
    record = sdk.LogRecord(body="x")
    assert exporter.export([record], gzip=False, timeout_s=1).accepted == 1
    exporter.shutdown()
    otlp = c.log_exporter("other", protocol="otlp")
    assert isinstance(otlp, sdk.OpenObserveOtlpLogExporter)
    assert otlp.export([record], gzip=True, timeout_s=1) == sdk.ExportResult(1, 0)
    rejected = otlp.export([record], gzip=False, timeout_s=1)
    assert rejected.rejected == 1 and isinstance(rejected.error, sdk.ValidationError)
    otlp.shutdown()
    with pytest.raises(sdk.ConfigError):
        c.log_exporter(protocol="xml")  # type: ignore[arg-type]
    c.close()


def test_log_query_search(server: MockServer) -> None:
    hit = {"_timestamp": 1_000_000, "message": "hello", "level": "info"}
    server.enqueue(SEARCH, rows(hit, hit), rows(hit), replay("search.track_total_hits"))
    q = sdk.OpenObserveClient(cfg(server)).log_query()
    page = q.search("hello", limit=2)
    assert len(page.records) == 2 and page.next_cursor == "2" and page.total is None
    sent = json.loads(server.seen[-1].body)["query"]
    assert sent["sql"] == "SELECT * FROM \"app\" WHERE match_all('hello') ORDER BY _timestamp DESC"
    assert (sent["from"], sent["size"]) == (0, 2)
    last = q.search("SELECT * FROM x", cursor="2", limit=5, track_total_hits=True)
    assert last.next_cursor is None and last.total == 5
    assert json.loads(server.seen[-2].body)["query"]["sql"] == "SELECT * FROM x"
    for kwargs in ({"limit": 0}, {"cursor": "-1"}, {"cursor": "abc"}):
        with pytest.raises(sdk.ValidationError):
            q.search("x", **kwargs)  # type: ignore[arg-type]


def test_log_query_streams_labels_wait(server: MockServer) -> None:
    server.enqueue("/api/default/streams", replay("listStreams.traces"))
    server.enqueue("/api/default/streams/app/schema", replay("getStreamSchema.ok"))
    server.enqueue(
        SEARCH,
        replay("search.400_stream_not_found"),
        rows({"_timestamp": 1, "message": "other"}),
        rows({"_timestamp": 2, "message": "wanted"}),
    )
    q = sdk.OpenObserveClient(cfg(server)).log_query()
    assert q.list_streams() == ["sdk_traces_6moiazbq"]
    assert "_timestamp" in q.list_labels()
    found = q.wait_for_log(
        "x", timeout_s=5, interval_s=0.01, predicate=lambda r: r.body == "wanted"
    )
    assert found.body == "wanted"
    assert q.stream == "app"


def test_log_query_tail_text_and_sql(server: MockServer) -> None:
    server.defaults[SEARCH] = rows({"_timestamp": 2_000_000, "message": "tailed"})
    q = sdk.OpenObserveClient(cfg(server)).log_query()
    got: list[sdk.LogRecord] = []
    done = threading.Event()

    def on_record(record: sdk.LogRecord) -> None:
        got.append(record)
        done.set()

    handle = q.tail("tailed", on_record, interval_s=0.01, start=1)
    assert done.wait(5)
    handle.stop()
    assert "match_all('tailed')" in json.loads(server.seen[-1].body)["query"]["sql"]
    everything = q.tail(" ", lambda r: None, interval_s=0.01, start=1)
    time.sleep(0.05)
    everything.stop()
    done.clear()
    sql_tail = q.tail(
        "SELECT * FROM app",
        on_record,
        interval_s=0.01,
        start=datetime(1970, 1, 1, tzinfo=timezone.utc),
    )
    assert done.wait(5)
    sql_tail.stop()


async def test_async_log_side(server: MockServer) -> None:
    server.enqueue(JSON, replay("ingestJson.ok"))
    server.enqueue("/api/default/v1/logs", replay("otlpLogs.json"))
    server.enqueue("/api/default/streams", replay("listStreams.logs"))
    server.enqueue("/api/default/streams/app/schema", replay("getStreamSchema.ok"))
    hit = {"_timestamp": 2_000_000, "message": "hello"}
    server.defaults[SEARCH] = rows(hit)
    async with sdk.AsyncOpenObserveClient(cfg(server)) as c:
        log_client = c.log_client(resource={"service.name": "svc"})
        log_client.send(sdk.LogInput(message="hello"))
        await log_client.flush(5)
        assert log_client.stats().sent == 1
        exporter = c.log_exporter()
        assert (await exporter.export([], gzip=False, timeout_s=1)).accepted == 0
        await exporter.shutdown()
        otlp = c.log_exporter(protocol="otlp")
        assert (await otlp.export([sdk.LogRecord(body="x")], gzip=False, timeout_s=1)).accepted == 1
        await otlp.shutdown()
        with pytest.raises(sdk.ConfigError):
            c.log_exporter(protocol="xml")  # type: ignore[arg-type]
        q = c.log_query()
        page = await q.search("hello", limit=1)
        assert page.next_cursor == "1"
        assert (await q.wait_for_log("hello", timeout_s=5)).body == "hello"
        assert "sdk_integration" in await q.list_streams() or await q.list_streams()
        assert "_timestamp" in await q.list_labels()
        got: list[sdk.LogRecord] = []
        text_tail = await q.tail("hello", got.append, interval_s=0.01, start=1)
        sql_tail = await q.tail("SELECT * FROM app", got.append, interval_s=0.01)
        for _ in range(200):
            if len(got) >= 2:
                break
            await asyncio.sleep(0.01)
        await text_tail.stop()
        await sql_tail.stop()
        assert len(got) >= 2
        with pytest.raises(sdk.ValidationError):
            await q.search("x", limit=0)


async def test_async_wait_for_log_missing_stream_and_sql_tail_errors(server: MockServer) -> None:
    server.enqueue(SEARCH, replay("search.400_stream_not_found"))
    server.defaults[SEARCH] = rows({"_timestamp": 3, "message": "late"})
    async with sdk.AsyncOpenObserveClient(cfg(server)) as c:
        q = c.log_query("app")
        record = await q.wait_for_log("late", timeout_s=5, interval_s=0.01)
        assert record.body == "late"
        errors: list[Exception] = []
        server.enqueue(SEARCH, Reply(400, {"message": "bad"}))
        tail = await q.tail("SELECT 1", lambda r: None, interval_s=0.01, on_error=errors.append)
        for _ in range(200):
            if errors:
                break
            await asyncio.sleep(0.01)
        await tail.stop()
        assert errors
