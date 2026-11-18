"""client.streams with recorded OpenObserve responses."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qs, urlsplit

import pytest

import grow_openobserve as sdk
from _support import MockServer, Reply, make_config, replay

SCHEMA = "/api/default/streams/s/schema"
SETTINGS = "/api/default/streams/s/settings"


def oo(server: MockServer) -> sdk.OpenObserveClient:
    return sdk.OpenObserveClient(make_config(server.url, user="u@example.com", password="pw"))


def aoo(server: MockServer) -> sdk.AsyncOpenObserveClient:
    return sdk.AsyncOpenObserveClient(make_config(server.url, user="u@example.com", password="pw"))


def last(server: MockServer, path: str) -> Any:
    return next(s for s in reversed(server.seen) if s.path.split("?")[0] == path)


def test_list_streams(server: MockServer) -> None:
    server.enqueue(
        "/api/default/streams", replay("listStreams.paged_keyword"), replay("listStreams.traces")
    )
    c = oo(server)
    streams = c.streams.list_streams("logs", keyword="sdk", offset=0, limit=2)
    assert streams and all(isinstance(s, sdk.StreamInfo) for s in streams)
    q = parse_qs(urlsplit(server.seen[-1].path).query)
    assert q == {
        "type": ["logs"],
        "keyword": ["sdk"],
        "offset": ["0"],
        "limit": ["2"],
        "sort": ["name"],
        "asc": ["true"],
    }
    traces = c.streams.list_streams("traces")
    assert traces[0].name == "sdk_traces_6moiazbq"
    assert traces[0].stream_type == "traces"
    assert traces[0].stats.created_at is not None
    with pytest.raises(sdk.ValidationError):
        c.streams.list_streams("nope")  # type: ignore[arg-type]


def test_get_stream_schema(server: MockServer) -> None:
    server.enqueue(SCHEMA, replay("getStreamSchema.after_settings"), replay("getStreamSchema.404"))
    c = oo(server)
    info = c.streams.get_stream_schema("S")
    assert info.name == "sdk_logs_6moiazbq"
    assert sdk.SchemaField("_timestamp", "Int64") in info.schema
    assert info.settings.retention_days == 7
    assert info.settings.partition_keys == [sdk.PartitionKey("run_id", "value", True)]
    assert info.total_fields == 8
    assert info.stats.created_at == datetime(2026, 11, 18, 20, 20, 0, 241779, tzinfo=timezone.utc)
    with pytest.raises(sdk.NotFoundError):
        c.streams.get_stream_schema("s")


def test_stream_info_odd_values() -> None:
    from grow_openobserve.streams import stream_info

    info = stream_info(
        {
            "settings": {"partition_keys": {"L0": {"field": "f", "types": {"hash": 8}}}},
            "stats": {"storage_size": "x"},
        }
    )
    assert info.settings.partition_keys[0].types == '{"hash": 8}'
    assert info.stats.created_at is None and info.stats.storage_size == 0.0


def test_update_settings_replace_computes_deltas(server: MockServer) -> None:
    current = {
        "name": "s",
        "settings": {
            "partition_keys": {
                "L0": {"field": "keep", "types": "value"},
                "L1": {"field": "old", "types": "value"},
                "L2": {"field": "gone", "types": "value", "disabled": True},
            },
            "full_text_search_keys": ["message", "body"],
            "index_fields": [],
            "bloom_filter_fields": ["x"],
        },
    }
    server.enqueue(SCHEMA, Reply(200, current))
    server.enqueue(SETTINGS, replay("updateStreamSettings.ok"))
    c = oo(server)
    c.streams.update_stream_settings(
        "s",
        retention_days=7,
        partition_keys=["keep", sdk.PartitionKey("new", "hash")],
        full_text_search_keys=["message"],
        index_fields="trace_id",  # type: ignore[arg-type]
        bloom_filter_fields=sdk.ListDelta(add=["y"], remove=["x"]),
    )
    sent = json.loads(last(server, SETTINGS).body)
    assert sent == {
        "data_retention": 7,
        "partition_keys": {
            "add": [{"field": "new", "types": "hash"}],
            "remove": [{"field": "old", "types": "value"}],
        },
        "full_text_search_keys": {"add": [], "remove": ["body"]},
        "index_fields": {"add": ["trace_id"], "remove": []},
        "bloom_filter_fields": {"add": ["y"], "remove": ["x"]},
    }


def test_update_settings_delta_only_skips_schema_read(server: MockServer) -> None:
    server.enqueue(
        SETTINGS, replay("updateStreamSettings.remove"), replay("updateStreamSettings.404")
    )
    c = oo(server)
    c.streams.update_stream_settings(
        "s",
        partition_keys=sdk.ListDelta(remove=[{"field": "run_id", "types": "value"}]),
        full_text_search_keys=sdk.ListDelta(remove=["message"]),
    )
    assert [s.path.split("?")[0] for s in server.seen] == [SETTINGS]
    assert json.loads(server.seen[-1].body)["partition_keys"] == {
        "add": [],
        "remove": [{"field": "run_id", "types": "value"}],
    }
    with pytest.raises(sdk.NotFoundError):
        c.streams.update_stream_settings("s", retention_days=1)
    for bad in (-1, True, 1.5):
        with pytest.raises(sdk.ValidationError):
            c.streams.update_stream_settings("s", retention_days=bad)  # type: ignore[arg-type]
    with pytest.raises(sdk.ValidationError):
        c.streams.update_stream_settings("s", partition_keys=sdk.ListDelta(add=[3]))


def test_update_settings_422(server: MockServer) -> None:
    server.enqueue(SETTINGS, replay("updateStreamSettings.422_plain_array"))
    with pytest.raises(sdk.ValidationError, match="Failed to deserialize"):
        oo(server).streams.update_stream_settings("s", retention_days=3)


def test_create_and_delete(server: MockServer) -> None:
    server.enqueue(
        "/api/default/streams/s",
        replay("createStream.ok"),
        replay("deleteStream.ok"),
        replay("deleteStream.404"),
    )
    c = oo(server)
    c.streams.create_stream(
        "s",
        fields=[sdk.SchemaField("message", "Utf8")],
        retention_days=3,
        partition_keys=["run_id"],
        full_text_search_keys=["message"],
        index_fields=["i"],
        bloom_filter_fields=["b"],
        stream_type="traces",
    )
    seen = server.seen[-1]
    assert seen.method == "POST" and "type=traces" in seen.path
    assert json.loads(seen.body) == {
        "fields": [{"name": "message", "type": "Utf8"}],
        "settings": {
            "partition_keys": [{"field": "run_id", "types": "value"}],
            "index_fields": ["i"],
            "full_text_search_keys": ["message"],
            "bloom_filter_fields": ["b"],
            "data_retention": 3,
        },
    }
    c.streams.delete_stream("s")
    assert server.seen[-1].method == "DELETE"
    with pytest.raises(sdk.NotFoundError):
        c.streams.delete_stream("s")


async def test_async_streams(server: MockServer) -> None:
    server.enqueue("/api/default/streams", replay("listStreams.traces"))
    server.enqueue(SCHEMA, replay("getStreamSchema.ok"))
    server.enqueue(SETTINGS, replay("updateStreamSettings.ok"))
    server.enqueue("/api/default/streams/s", replay("createStream.ok"), replay("deleteStream.ok"))
    async with aoo(server) as c:
        assert (await c.streams.list_streams("traces"))[0].stream_type == "traces"
        assert (await c.streams.get_stream_schema("s")).name
        await c.streams.update_stream_settings("s", retention_days=2)
        await c.streams.create_stream("s")
        await c.streams.delete_stream("s")
    assert [s.method for s in server.seen] == ["GET", "GET", "PUT", "POST", "DELETE"]
