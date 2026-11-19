"""client.logs with recorded OpenObserve responses (tests/fixtures)."""

from __future__ import annotations

import asyncio
import base64
import gzip
import json
import threading
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qs, urlsplit

import pytest

import grow_openobserve as sdk
from _support import MockServer, Reply, make_config, replay

USER, PASSWORD = "root@example.com", "pw-secret-1"
SEARCH = "/api/default/_search"


def oo(server: MockServer, **kw: Any) -> sdk.OpenObserveClient:
    return sdk.OpenObserveClient(make_config(server.url, user=USER, password=PASSWORD, **kw))


def aoo(server: MockServer, **kw: Any) -> sdk.AsyncOpenObserveClient:
    return sdk.AsyncOpenObserveClient(make_config(server.url, user=USER, password=PASSWORD, **kw))


def query(seen: Any) -> dict[str, list[str]]:
    return parse_qs(urlsplit(seen.path).query)


def body(seen: Any) -> Any:
    raw = seen.body
    if seen.headers.get("Content-Encoding") == "gzip":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def test_ingest_json(server: MockServer) -> None:
    path = "/api/default/sdk_logs_6moiazbq/_json"
    server.enqueue(path, replay("ingestJson.ok"), replay("ingestJson.200_partial_failure"))
    c = oo(server)
    result = c.logs.ingest_json("SDK-Logs-6moiazbq", [{"message": "a"}, {"message": "b"}])
    assert result == sdk.IngestResult("sdk_logs_6moiazbq", 3, 0)
    seen = server.seen[-1]
    assert seen.headers["Authorization"].startswith("Basic ")
    assert body(seen) == [{"message": "a"}, {"message": "b"}]
    partial = c.logs.ingest_json("sdk_logs_6moiazbq", {"message": "one"}, gzip=True)
    assert (partial.successful, partial.failed) == (1, 1)
    assert partial.stream == "sdk_logs_6moiazbq_partial"
    assert partial.error is not None and partial.error.startswith("Too old data")
    assert server.seen[-1].headers["Content-Encoding"] == "gzip"
    assert body(server.seen[-1]) == [{"message": "one"}]


def test_ingest_errors(server: MockServer) -> None:
    path = "/api/default/s/_json"
    server.enqueue(path, replay("ingestJson.400_invalid_json"), replay("ingestJson.401"))
    c = oo(server)
    with pytest.raises(sdk.ValidationError, match="SerdeJsonError"):
        c.logs.ingest_json("s", [{}])
    with pytest.raises(sdk.AuthenticationError) as info:
        c.logs.ingest_json("s", [{}])
    assert PASSWORD not in str(info.value)
    with pytest.raises(sdk.ValidationError):
        c.logs.ingest_json("  ", [{}])
    server.enqueue(path, Reply(200, {"code": 200, "status": []}))
    assert c.logs.ingest_json("s", []) == sdk.IngestResult("s", 0, 0)


def test_ingest_multi(server: MockServer) -> None:
    path = "/api/default/s/_multi"
    server.defaults[path] = replay("ingestMulti.ok")
    c = oo(server)
    result = c.logs.ingest_multi("s", [{"message": "a"}, {"message": "b"}])
    assert result.successful == 2
    assert server.seen[-1].body == b'{"message":"a"}\n{"message":"b"}\n'
    assert server.seen[-1].headers["Content-Type"] == "application/x-ndjson"
    c.logs.ingest_multi("s", '{"message":"c"}\n')
    assert server.seen[-1].body == b'{"message":"c"}\n'
    c.logs.ingest_multi("s", b'{"message":"d"}\n')
    assert server.seen[-1].body == b'{"message":"d"}\n'
    server.enqueue(path, replay("ingestMulti.400_invalid_line"))
    with pytest.raises(sdk.ValidationError):
        c.logs.ingest_multi("s", "nope")


def test_ingest_bulk(server: MockServer) -> None:
    path = "/api/default/_bulk"
    server.enqueue(
        path, replay("ingestBulk.ok"), replay("ingestBulk.200_item_error"), replay("ingestBulk.ok")
    )
    c = oo(server)
    ok = c.logs.ingest_bulk(
        [
            sdk.BulkAction("Bulk-A", {"message": "one"}),
            sdk.BulkAction("b", {"x": 1}, action="create"),
        ]
    )
    assert not ok.errors and [i.status for i in ok.items] == [200, 200]
    assert ok.items[0] == sdk.BulkItem("index", "sdk_bulk_6moiazbq", 200)
    lines = server.seen[-1].body.decode().splitlines()
    assert json.loads(lines[0]) == {"index": {"_index": "bulk_a"}}
    assert json.loads(lines[2]) == {"create": {"_index": "b"}}
    failed = c.logs.ingest_bulk('{"index":{"_index":"s"}}\n{"message":"old"}', gzip=True)
    assert failed.errors
    assert failed.items[0].error == sdk.BulkItemError(
        "timestamp_parsing_failed", failed.items[0].error.reason if failed.items[0].error else ""
    )
    assert failed.items[0].status == 422
    assert gzip.decompress(server.seen[-1].body).endswith(b"\n")
    raw = c.logs.ingest_bulk('{"create":{"_index":"s"}}\n{"m":1}\n')
    assert not raw.errors
    for bad in (
        [sdk.BulkAction("s", {}, action="delete")],
        [],
        "",
        '{"delete":{"_index":"s"}}\n',
        "not json\n{}",
        '["index"]\n{}',
    ):
        with pytest.raises(sdk.ValidationError):
            c.logs.ingest_bulk(bad)  # type: ignore[arg-type]


def test_ingest_otlp(server: MockServer) -> None:
    path = "/api/default/v1/logs"
    server.enqueue(
        path,
        replay("otlpLogs.json"),
        Reply(200, {"partialSuccess": {"rejectedLogRecords": "1"}}),
        replay("otlpLogs.400_invalid_protobuf"),
    )
    c = oo(server, stream="App")
    ts = datetime(2026, 11, 18, 12, 0, tzinfo=timezone.utc)
    records = [
        sdk.LogRecord(
            body="hello",
            severity=sdk.Severity.WARN,
            timestamp=ts,
            observed_timestamp=ts,
            attributes={
                "n": 1,
                "f": 0.5,
                "ok": True,
                "tags": ["a", 2],
                "map": {"k": "v"},
                "x": None,
            },  # type: ignore[dict-item]
            resource={"service.name": "svc"},
            trace_id="4bf92f3577b34da6a3ce929d0e0e4736",
            span_id="00f067aa0ba902b7",
            trace_flags=1,
        ),
        sdk.LogRecord(body="second", resource={"service.name": "other"}),
    ]
    result = c.logs.ingest_otlp(records)
    assert result == sdk.OtlpIngestResult(2, 0)
    seen = server.seen[-1]
    assert seen.headers["stream-name"] == "app"
    doc = body(seen)
    assert len(doc["resourceLogs"]) == 2
    log = doc["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0]
    assert log["timeUnixNano"] == str(int(ts.timestamp()) * 10**9)
    assert log["severityNumber"] == 13 and log["flags"] == 1
    values = {a["key"]: a["value"] for a in log["attributes"]}
    assert values["n"] == {"intValue": "1"}
    assert values["f"] == {"doubleValue": 0.5}
    assert values["ok"] == {"boolValue": True}
    assert values["tags"]["arrayValue"]["values"][1] == {"intValue": "2"}
    assert values["map"]["kvlistValue"]["values"][0]["key"] == "k"
    assert values["x"] == {"stringValue": "None"}
    partial = c.logs.ingest_otlp(records[:1], stream="s", gzip=True)
    assert partial == sdk.OtlpIngestResult(0, 1)
    with pytest.raises(sdk.ValidationError, match="Invalid proto"):
        c.logs.ingest_otlp(records[:1])
    assert c.logs.ingest_otlp([]) == sdk.OtlpIngestResult(0, 0)


def test_search_and_total_hits(server: MockServer) -> None:
    server.enqueue(
        SEARCH, replay("search.ok"), replay("search.paging"), replay("search.track_total_hits")
    )
    c = oo(server)
    result = c.logs.search('SELECT * FROM "s"', start=1, end=2)
    assert len(result.hits) == 5 and result.total == 5
    assert result.request_id == "01a1f050f76b74d09079000b8030b68f"
    assert result.took == 5 and result.scan_records == 5 and not result.is_partial
    assert result.total_hits is None
    sent = body(server.seen[-1])["query"]
    assert (sent["start_time"], sent["end_time"], sent["from"], sent["size"]) == (1, 2, 0, 100)
    assert query(server.seen[-1])["type"] == ["logs"]
    paged = c.logs.search(
        "SELECT 1",
        start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        from_=1,
        size=2,
        track_total_hits=True,
    )
    assert paged.total_hits == 5
    count_query = body(server.seen[-1])["query"]
    assert count_query["track_total_hits"] is True and count_query["from"] == 0
    with pytest.raises(sdk.ValidationError):
        c.logs.search("  ")
    with pytest.raises(sdk.ValidationError):
        c.logs.search("SELECT 1", stream_type="bogus")  # type: ignore[arg-type]
    with pytest.raises(sdk.ValidationError):
        c.logs.search("SELECT 1", start=10, end=5)


@pytest.mark.parametrize(
    ("name", "error"),
    [
        ("search.400_bad_sql", sdk.ValidationError),
        ("search.400_stream_not_found", sdk.NotFoundError),
        ("search.400_unknown_field", sdk.ValidationError),
        ("search.400_invalid_time_range", sdk.ValidationError),
        ("search.404_unknown_org", sdk.NotFoundError),
        ("search.401", sdk.AuthenticationError),
        ("search.422_missing_start_time", sdk.ValidationError),
    ],
)
def test_search_errors(server: MockServer, name: str, error: type[Exception]) -> None:
    server.enqueue(SEARCH, replay(name))
    with pytest.raises(error):
        oo(server).logs.search("SELECT 1")


def test_search_around(server: MockServer) -> None:
    path = "/api/default/s/_around"
    server.enqueue(
        path,
        replay("searchAround.ok"),
        replay("searchAround.ok"),
        replay("searchAround.400_stream_not_found"),
    )
    c = oo(server)
    result = c.logs.search_around("s", 1795033200242779)
    assert len(result.hits) == 5
    assert query(server.seen[-1]) == {"key": ["1795033200242779"], "size": ["10"], "type": ["logs"]}
    c.logs.search_around("s", datetime(2026, 1, 1, tzinfo=timezone.utc), size=4, sql="SELECT *")
    q = query(server.seen[-1])
    assert base64.urlsafe_b64decode(q["sql"][0] + "==") == b"SELECT *"
    assert q["size"] == ["4"]
    with pytest.raises(sdk.NotFoundError):
        c.logs.search_around("s", 1)


def test_values_of(server: MockServer) -> None:
    path = "/api/default/s/_values"
    server.enqueue(
        path,
        replay("valuesOf.ok"),
        Reply(
            200, {"hits": [{"field": "f", "values": [{"zo_sql_key": {"a": 1}, "zo_sql_num": 2}]}]}
        ),
        replay("valuesOf.400_missing_time"),
    )
    c = oo(server)
    values = c.logs.values_of("s", ["level", "run_id"], start=1, end=2, size=3)
    assert values[0].field == "level"
    assert values[0].values[0] == sdk.FieldValue("debug", 2)
    assert query(server.seen[-1])["fields"] == ["level,run_id"]
    odd = c.logs.values_of("s", "f")  # type: ignore[arg-type]
    assert odd[0].values[0].value == '{"a":1}'
    with pytest.raises(sdk.ValidationError):
        c.logs.values_of("s", ["x"])
    with pytest.raises(sdk.ValidationError):
        c.logs.values_of("s", [])


def test_histogram(server: MockServer) -> None:
    server.enqueue(
        SEARCH,
        replay("histogram.ok"),
        Reply(
            200,
            {
                "hits": [
                    {"zo_sql_key": "garbage", "zo_sql_num": 1},
                    {"zo_sql_key": "2026-01-01T00:00:00Z", "zo_sql_num": 2},
                ]
            },
        ),
    )
    c = oo(server)
    hist = c.logs.histogram("s", interval="1 minute", filter="level = 'error'")
    assert hist.interval_seconds == 60
    assert hist.buckets == [
        sdk.HistogramBucket(
            datetime(2026, 11, 18, 20, 20, tzinfo=timezone.utc), "2026-11-18T20:20:00", 5
        )
    ]
    sql = body(server.seen[-1])["query"]["sql"]
    assert sql == (
        "SELECT histogram(_timestamp, '1 minute') AS zo_sql_key, count(*) AS zo_sql_num "
        "FROM \"s\" WHERE level = 'error' GROUP BY zo_sql_key ORDER BY zo_sql_key"
    )
    assert body(server.seen[-1])["query"]["size"] == -1
    other = c.logs.histogram("s")
    assert other.interval_seconds is None
    assert other.buckets[0].start == datetime(1970, 1, 1, tzinfo=timezone.utc)
    assert other.buckets[1].start == datetime(2026, 1, 1, tzinfo=timezone.utc)
    with pytest.raises(sdk.ValidationError):
        c.logs.histogram("s", interval="1; DROP")


def search_reply(*rows: dict[str, Any]) -> Reply:
    return Reply(200, {"hits": list(rows), "total": len(rows)})


def test_tail_delivers_new_rows_once(server: MockServer) -> None:
    a = {"_timestamp": 2_000_000, "message": "a"}
    b = {"_timestamp": 2_000_001, "message": "b"}
    server.enqueue(SEARCH, search_reply(a), search_reply(a, b), Reply(500, {"message": "boom"}))
    server.defaults[SEARCH] = search_reply(a, b)
    rows: list[dict[str, Any]] = []
    errors: list[Exception] = []
    c = oo(server, retry=sdk.RetryPolicy(max_retries=0))
    handle = c.logs.tail(
        "s",
        rows.append,
        start=1_000_000,
        interval_s=0.02,
        page_size=5,
        on_error=errors.append,
        filter="level = 'x'",
    )
    deadline = time.monotonic() + 5
    while (len(rows) < 2 or not errors) and time.monotonic() < deadline:
        time.sleep(0.02)
    handle.stop()
    handle.stop()
    assert rows == [a, b]
    assert isinstance(errors[0], sdk.UnavailableError)
    sql = body(server.seen[0])["query"]["sql"]
    assert sql == (
        "SELECT * FROM \"s\" WHERE _timestamp >= 1000000 AND (level = 'x') ORDER BY _timestamp ASC"
    )
    sql2 = body(server.seen[1])["query"]["sql"]
    assert "_timestamp >= 1000000" in sql2  # never before the start


def test_tail_pages_and_poll_once(server: MockServer) -> None:
    rows = [{"_timestamp": 5_000_000 + i, "message": str(i)} for i in range(3)]
    server.enqueue(SEARCH, search_reply(*rows[:2]), search_reply(*rows))
    got: list[dict[str, Any]] = []
    c = oo(server)
    handle = c.logs.tail("s", got.append, start=5_000_000, interval_s=60, page_size=2)
    assert handle.poll_once() == 3
    handle.stop()
    assert [r["message"] for r in got] == ["0", "1", "2"]


async def test_async_logs(server: MockServer) -> None:
    server.enqueue("/api/default/s/_json", replay("ingestJson.ok"))
    server.enqueue("/api/default/s/_multi", replay("ingestMulti.ok"))
    server.enqueue("/api/default/_bulk", replay("ingestBulk.ok"))
    server.enqueue("/api/default/v1/logs", replay("otlpLogs.json"))
    server.enqueue(SEARCH, replay("search.ok"), replay("histogram.ok"))
    server.enqueue("/api/default/s/_around", replay("searchAround.ok"))
    server.enqueue("/api/default/s/_values", replay("valuesOf.ok"))
    async with aoo(server) as c:
        assert (await c.logs.ingest_json("s", [{}])).successful == 3
        assert (await c.logs.ingest_multi("s", "{}\n")).successful == 2
        assert not (await c.logs.ingest_bulk([sdk.BulkAction("s", {})])).errors
        assert (await c.logs.ingest_otlp([sdk.LogRecord(body="x")])).accepted == 1
        assert len((await c.logs.search("SELECT 1")).hits) == 5
        assert (await c.logs.histogram("s")).interval_seconds == 60
        assert len((await c.logs.search_around("s", 1)).hits) == 5
        assert (await c.logs.values_of("s", ["level"]))[0].field == "level"


async def test_async_tail(server: MockServer) -> None:
    a = {"_timestamp": 2_000_000, "message": "a"}
    server.enqueue(SEARCH, search_reply(a), Reply(400, {"message": "bad"}))
    server.defaults[SEARCH] = search_reply(a)
    rows: list[dict[str, Any]] = []
    errors: list[Exception] = []
    async with aoo(server) as c:
        handle = c.logs.tail("s", rows.append, start=1, interval_s=0.02, on_error=errors.append)
        for _ in range(200):
            if rows and errors:
                break
            await asyncio.sleep(0.02)
        await handle.stop()
        await handle.stop()
        assert rows == [a]
        assert isinstance(errors[0], sdk.ValidationError)
        quiet = c.logs.tail("s", rows.append, interval_s=0.01)
        await asyncio.sleep(0.05)
        await quiet.stop()
        assert await quiet.poll_once() in (0, 1)


def test_tail_stop_from_callback(server: MockServer) -> None:
    a = {"_timestamp": 2_000_000, "message": "a"}
    b = {"_timestamp": 2_000_001, "message": "b"}
    server.defaults[SEARCH] = search_reply(a, b)
    c = oo(server)
    got: list[dict[str, Any]] = []
    done = threading.Event()
    holder: list[sdk.RowTail] = []

    def on_row(row: dict[str, Any]) -> None:
        got.append(row)
        holder[0].stop()
        done.set()

    holder.append(c.logs.tail("s", on_row, start=1, interval_s=0.01))
    assert done.wait(5)
    time.sleep(0.05)
    assert got == [a]
