"""client.logs against the real service: every ingest call round-trips through search."""

from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta, timezone

import pytest

import grow_openobserve as sdk


def _now_us() -> int:
    return int(time.time() * 1_000_000)


def _rows(client: sdk.OpenObserveClient, stream: str, where: str = "") -> list[dict]:
    sql = f'SELECT * FROM "{stream}"{where} ORDER BY _timestamp ASC'
    return client.logs.search(sql, size=1000).hits


def test_ingest_json_and_search(client: sdk.OpenObserveClient, stream: str, rid: str) -> None:
    base = _now_us()
    result = client.logs.ingest_json(
        stream,
        [
            {"message": f"alpha {rid}", "level": "info", "n": 1, "_timestamp": base},
            {
                "message": "beta",
                "level": "error",
                "n": 2,
                "user": {"id": 7},
                "_timestamp": base + 1,
            },
            {"message": "gamma ünïcødé", "level": "warn", "n": 3, "_timestamp": base + 2},
        ],
    )
    assert (result.stream, result.successful, result.failed) == (stream, 3, 0)
    rows = _rows(client, stream)
    assert [r["message"] for r in rows] == [f"alpha {rid}", "beta", "gamma ünïcødé"]
    assert rows[1]["user_id"] == 7

    page = client.logs.search(
        f'SELECT * FROM "{stream}" ORDER BY _timestamp DESC',
        from_=1,
        size=1,
        track_total_hits=True,
    )
    assert [h["message"] for h in page.hits] == ["beta"]
    assert page.total == 1
    assert page.total_hits == 3
    assert page.request_id

    single = client.logs.ingest_json(
        stream, {"message": "single", "_timestamp": base + 3}, gzip=True
    )
    assert single.successful == 1
    assert len(_rows(client, stream)) == 4


def test_ingest_json_partial_failure(client: sdk.OpenObserveClient, stream: str) -> None:
    old = _now_us() - 24 * 3600 * 1_000_000
    result = client.logs.ingest_json(
        stream, [{"message": "fresh"}, {"message": "old", "_timestamp": old}]
    )
    assert result.successful == 1
    assert result.failed == 1
    assert result.error and "old" in result.error.lower()


def test_ingest_multi(client: sdk.OpenObserveClient, stream: str) -> None:
    result = client.logs.ingest_multi(stream, [{"message": "m0"}, {"message": "m1"}])
    assert result.successful == 2
    raw = client.logs.ingest_multi(stream, '{"message": "m2"}\n{"message": "m3"}\n', gzip=True)
    assert raw.successful == 2
    assert sorted(r["message"] for r in _rows(client, stream)) == ["m0", "m1", "m2", "m3"]
    with pytest.raises(sdk.ValidationError):
        client.logs.ingest_multi(stream, '{"message": "ok"}\nnot json\n')


def test_ingest_bulk(client: sdk.OpenObserveClient, stream: str) -> None:
    result = client.logs.ingest_bulk(
        [
            sdk.BulkAction(stream, {"message": "bulk one"}),
            sdk.BulkAction(stream, {"message": "bulk two"}, action="create"),
        ]
    )
    assert not result.errors
    assert [i.status for i in result.items] == [200, 200]
    assert {i.stream for i in result.items} == {stream}
    raw = client.logs.ingest_bulk(
        f'{{"index":{{"_index":"{stream}"}}}}\n{{"message":"bulk three"}}'
    )
    assert not raw.errors
    assert sorted(r["message"] for r in _rows(client, stream)) == [
        "bulk one",
        "bulk three",
        "bulk two",
    ]
    old = _now_us() - 24 * 3600 * 1_000_000
    failed = client.logs.ingest_bulk(
        [sdk.BulkAction(stream, {"message": "old", "_timestamp": old})]
    )
    assert failed.errors
    assert failed.items[0].error is not None
    with pytest.raises(sdk.ValidationError):
        client.logs.ingest_bulk([sdk.BulkAction(stream, {}, action="delete")])


def test_ingest_otlp(client: sdk.OpenObserveClient, stream: str, rid: str) -> None:
    records = [
        sdk.LogRecord(
            body=f"otlp {rid}",
            severity=sdk.Severity.WARN,
            attributes={"run": rid, "n": 1},
            resource={"service.name": "py-otlp"},
            trace_id="4bf92f3577b34da6a3ce929d0e0e4736",
            span_id="00f067aa0ba902b7",
        )
    ]
    result = client.logs.ingest_otlp(records, stream=stream, gzip=True)
    assert (result.accepted, result.rejected) == (1, 0)
    rows = _rows(client, stream)
    assert rows[0]["body"] == f"otlp {rid}"
    assert rows[0]["severity"] == "WARN"
    assert rows[0]["service_name"] == "py-otlp"
    assert rows[0]["trace_id"] == "4bf92f3577b34da6a3ce929d0e0e4736"
    record = sdk.row_to_log_record(rows[0])
    assert record.severity == sdk.Severity.WARN
    assert record.resource["service.name"] == "py-otlp"
    assert client.logs.ingest_otlp([], stream=stream) == sdk.OtlpIngestResult(0, 0)


def test_search_around_values_histogram(client: sdk.OpenObserveClient, stream: str) -> None:
    base = _now_us() - 5_000_000
    client.logs.ingest_json(
        stream,
        [
            {"message": f"row {i}", "level": "error" if i % 2 else "info", "_timestamp": base + i}
            for i in range(6)
        ],
    )
    around = client.logs.search_around(stream, base + 2, size=10)
    assert len(around.hits) == 6
    assert any(h["_timestamp"] == base + 2 for h in around.hits)
    # v1.1.2 accepts the sql filter but does not apply it (see docs/compatibility.md).
    filtered = client.logs.search_around(
        stream, base + 2, size=10, sql=f"SELECT * FROM \"{stream}\" WHERE level = 'error'"
    )
    assert any(h["level"] == "error" for h in filtered.hits)

    values = client.logs.values_of(stream, ["level"])
    assert values[0].field == "level"
    assert {(v.value, v.count) for v in values[0].values} == {("error", 3), ("info", 3)}

    histogram = client.logs.histogram(stream, interval="1 minute", filter="level = 'error'")
    assert sum(b.count for b in histogram.buckets) == 3
    assert histogram.interval_seconds == 60
    assert all(b.start.tzinfo is not None for b in histogram.buckets)
    default = client.logs.histogram(stream, start=datetime.now(timezone.utc) - timedelta(minutes=5))
    assert sum(b.count for b in default.buckets) == 6


def test_search_errors(client: sdk.OpenObserveClient, rid: str) -> None:
    with pytest.raises(sdk.NotFoundError) as info:
        client.logs.search(f"SELECT * FROM no_such_stream_{rid}")
    assert info.value.http_status == 400
    assert info.value.request_id
    with pytest.raises(sdk.ValidationError):
        client.logs.search("SELEC nonsense FROM")


def test_tail(client: sdk.OpenObserveClient, stream: str, rid: str) -> None:
    client.logs.ingest_json(stream, [{"message": "before tail"}])
    seen: list[str] = []
    got = threading.Event()

    def on_row(row: dict) -> None:
        seen.append(row["message"])
        if len(seen) >= 2:
            got.set()

    handle = client.logs.tail(stream, on_row, interval_s=0.2, start=_now_us())
    try:
        time.sleep(0.3)
        client.logs.ingest_json(
            stream, [{"message": f"tail a {rid}"}, {"message": f"tail b {rid}"}]
        )
        assert got.wait(15), seen
    finally:
        handle.stop()
        handle.stop()
    assert sorted(seen) == [f"tail a {rid}", f"tail b {rid}"]
