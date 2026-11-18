"""client.streams against the real service."""

from __future__ import annotations

import pytest

import grow_openobserve as sdk


def test_stream_lifecycle(client: sdk.OpenObserveClient, stream: str) -> None:
    client.streams.create_stream(
        stream,
        fields=[sdk.SchemaField("message", "Utf8"), sdk.SchemaField("run_id", "Utf8")],
        retention_days=3,
    )
    info = client.streams.get_stream_schema(stream)
    assert info.name == stream
    assert info.stream_type == "logs"
    assert {"message", "run_id"} <= {f.name for f in info.schema}
    assert info.settings.retention_days == 3

    listed = client.streams.list_streams("logs", keyword=stream)
    assert [s.name for s in listed] == [stream]
    assert stream in [s.name for s in client.streams.list_streams()]

    client.streams.update_stream_settings(
        stream,
        retention_days=7,
        partition_keys=["run_id"],
        full_text_search_keys=["message"],
    )
    after = client.streams.get_stream_schema(stream).settings
    assert after.retention_days == 7
    assert [(k.field, k.disabled) for k in after.partition_keys] == [("run_id", False)]
    assert after.full_text_search_keys == ["message"]

    client.streams.update_stream_settings(
        stream,
        partition_keys=sdk.ListDelta(remove=["run_id"]),
        full_text_search_keys=[],
    )
    removed = client.streams.get_stream_schema(stream).settings
    assert all(k.disabled for k in removed.partition_keys if k.field == "run_id")
    assert removed.full_text_search_keys == []

    client.streams.delete_stream(stream)
    with pytest.raises(sdk.NotFoundError):
        client.streams.get_stream_schema(stream)
    with pytest.raises(sdk.NotFoundError):
        client.streams.delete_stream(stream)
    with pytest.raises(sdk.NotFoundError):
        client.streams.update_stream_settings(stream, retention_days=1)


def test_list_trace_streams(client: sdk.OpenObserveClient) -> None:
    streams = client.streams.list_streams("traces")
    assert all(s.stream_type == "traces" for s in streams)
