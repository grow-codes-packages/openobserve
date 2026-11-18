"""Stream management: list, schema, settings, create, delete (``client.streams``)."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, TypeVar

from ._core import CancelToken, ValidationError
from ._http import (
    Call,
    Ctx,
    as_dict,
    as_int,
    as_list,
    as_str,
    json_body,
    micros_to_dt,
    org_path,
    request,
    run_async,
    run_sync,
    stream_name,
    stream_type,
)
from .logs import StreamType

if TYPE_CHECKING:
    from .config import OpenObserveConfig

__all__ = [
    "AsyncOpenObserveStreams",
    "ListDelta",
    "OpenObserveStreams",
    "PartitionKey",
    "SchemaField",
    "StreamInfo",
    "StreamSettings",
    "StreamStats",
]

_T = TypeVar("_T")
_W = TypeVar("_W")


@dataclass(frozen=True)
class SchemaField:
    """One schema field; ``type`` is the Arrow type name (``Utf8``, ``Int64``, …)."""

    name: str
    type: str


@dataclass(frozen=True)
class PartitionKey:
    """A partition key; removed keys stay listed with ``disabled=True``."""

    field: str
    #: ``value`` (default) or ``hash``.
    types: str = "value"
    disabled: bool = False


@dataclass(frozen=True)
class StreamSettings:
    """Selected stream settings; ``raw`` holds the full server object."""

    partition_keys: list[PartitionKey] = field(default_factory=list)
    full_text_search_keys: list[str] = field(default_factory=list)
    index_fields: list[str] = field(default_factory=list)
    bloom_filter_fields: list[str] = field(default_factory=list)
    #: Retention in days; 0 = the server default.
    retention_days: int = 0
    raw: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StreamStats:
    created_at: datetime | None = None
    doc_num: int = 0
    storage_size: float = 0.0
    compressed_size: float = 0.0


@dataclass(frozen=True)
class StreamInfo:
    """A stream with its stats, schema and settings."""

    name: str
    stream_type: str
    storage_type: str = ""
    stats: StreamStats = field(default_factory=StreamStats)
    schema: list[SchemaField] = field(default_factory=list)
    settings: StreamSettings = field(default_factory=StreamSettings)
    total_fields: int = 0


@dataclass(frozen=True)
class ListDelta:
    """An explicit add/remove change of a list setting (sent as given)."""

    add: Sequence[Any] = ()
    remove: Sequence[Any] = ()


PartitionKeyInput = str | PartitionKey
ListUpdate = Sequence[Any] | ListDelta


def _num(value: Any) -> float:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    return 0.0


def _strings(value: Any) -> list[str]:
    return [v for v in as_list(value) if isinstance(v, str)]


def stream_info(data: Mapping[str, Any]) -> StreamInfo:
    """Parse one stream object (``/streams`` list entry or ``/schema`` response)."""
    stats = as_dict(data.get("stats"))
    settings = as_dict(data.get("settings"))
    created = as_int(stats.get("created_at"))
    keys = []
    for raw in as_dict(settings.get("partition_keys")).values():
        key = as_dict(raw)
        types = key.get("types", "value")
        keys.append(
            PartitionKey(
                field=as_str(key.get("field")) or "",
                types=types if isinstance(types, str) else json.dumps(types),
                disabled=key.get("disabled") is True,
            )
        )
    return StreamInfo(
        name=as_str(data.get("name")) or "",
        stream_type=as_str(data.get("stream_type")) or "",
        storage_type=as_str(data.get("storage_type")) or "",
        stats=StreamStats(
            created_at=micros_to_dt(created) if created > 0 else None,
            doc_num=as_int(stats.get("doc_num")),
            storage_size=_num(stats.get("storage_size")),
            compressed_size=_num(stats.get("compressed_size")),
        ),
        schema=[
            SchemaField(
                name=as_str(as_dict(f).get("name")) or "", type=as_str(as_dict(f).get("type")) or ""
            )
            for f in as_list(data.get("schema"))
        ],
        settings=StreamSettings(
            partition_keys=keys,
            full_text_search_keys=_strings(settings.get("full_text_search_keys")),
            index_fields=_strings(settings.get("index_fields")),
            bloom_filter_fields=_strings(settings.get("bloom_filter_fields")),
            retention_days=as_int(settings.get("data_retention")),
            raw=settings,
        ),
        total_fields=as_int(data.get("total_fields")),
    )


def _key_wire(key: PartitionKeyInput | Mapping[str, Any]) -> dict[str, str]:
    if isinstance(key, str):
        return {"field": key, "types": "value"}
    if isinstance(key, PartitionKey):
        return {"field": key.field, "types": key.types}
    if isinstance(key, Mapping) and isinstance(key.get("field"), str):
        return {"field": key["field"], "types": str(key.get("types", "value"))}
    raise ValidationError("streams: a partition key is a field name or a PartitionKey")


def _delta(
    update: ListUpdate,
    current: Sequence[_W],
    key_of: Callable[[Any], str],
    to_wire: Callable[[Any], _W],
) -> dict[str, list[_W]]:
    if isinstance(update, ListDelta):
        return {
            "add": [to_wire(v) for v in update.add],
            "remove": [to_wire(v) for v in update.remove],
        }
    if isinstance(update, str):
        update = [update]
    wanted = [to_wire(v) for v in update]
    wanted_keys = {key_of(w) for w in wanted}
    current_keys = {key_of(c) for c in current}
    return {
        "add": [w for w in wanted if key_of(w) not in current_keys],
        "remove": [c for c in current if key_of(c) not in wanted_keys],
    }


def list_streams_call(
    config: OpenObserveConfig,
    kind: str,
    keyword: str | None,
    offset: int | None,
    limit: int | None,
) -> Call[list[StreamInfo]]:
    stream_type(kind, "streams.list_streams")
    resp = yield request(
        config,
        "GET",
        org_path(config, "streams"),
        "streams.list_streams",
        params={
            "type": kind,
            "keyword": keyword,
            "offset": offset,
            "limit": limit,
            "sort": "name",
            "asc": True,
        },
    )
    return [stream_info(as_dict(s)) for s in as_list(as_dict(json_body(resp)).get("list"))]


def get_stream_schema_call(config: OpenObserveConfig, stream: str, kind: str) -> Call[StreamInfo]:
    name = stream_name(stream, "", "streams.get_stream_schema")
    stream_type(kind, "streams.get_stream_schema")
    resp = yield request(
        config,
        "GET",
        org_path(config, "streams", name, "schema"),
        f"streams.get_stream_schema {name}",
        params={"type": kind},
    )
    return stream_info(as_dict(json_body(resp)))


def update_stream_settings_call(
    config: OpenObserveConfig,
    stream: str,
    *,
    retention_days: int | None,
    partition_keys: ListUpdate | None,
    full_text_search_keys: ListUpdate | None,
    index_fields: ListUpdate | None,
    bloom_filter_fields: ListUpdate | None,
    kind: str,
) -> Call[None]:
    name = stream_name(stream, "", "streams.update_stream_settings")
    stream_type(kind, "streams.update_stream_settings")
    body: dict[str, Any] = {}
    if retention_days is not None:
        if (
            isinstance(retention_days, bool)
            or not isinstance(retention_days, int)
            or (retention_days < 0)
        ):
            raise ValidationError("streams.update_stream_settings: retention_days must be >= 0")
        body["data_retention"] = retention_days
    lists = (partition_keys, full_text_search_keys, index_fields, bloom_filter_fields)
    current = StreamSettings()
    if any(u is not None and not isinstance(u, ListDelta) for u in lists):
        info = yield from get_stream_schema_call(config, name, kind)
        current = info.settings
    if partition_keys is not None:
        enabled = [
            {"field": k.field, "types": k.types} for k in current.partition_keys if not k.disabled
        ]
        body["partition_keys"] = _delta(
            partition_keys, enabled, lambda k: str(k["field"]), _key_wire
        )
    for update, existing, wire in (
        (full_text_search_keys, current.full_text_search_keys, "full_text_search_keys"),
        (index_fields, current.index_fields, "index_fields"),
        (bloom_filter_fields, current.bloom_filter_fields, "bloom_filter_fields"),
    ):
        if update is not None:
            body[wire] = _delta(update, existing, str, str)
    yield request(
        config,
        "PUT",
        org_path(config, "streams", name, "settings"),
        f"streams.update_stream_settings {name}",
        params={"type": kind},
        json_body=body,
    )
    return None


def create_stream_call(
    config: OpenObserveConfig,
    stream: str,
    *,
    fields: Sequence[SchemaField],
    retention_days: int,
    partition_keys: Sequence[PartitionKeyInput],
    full_text_search_keys: Sequence[str],
    index_fields: Sequence[str],
    bloom_filter_fields: Sequence[str],
    kind: str,
) -> Call[None]:
    name = stream_name(stream, "", "streams.create_stream")
    stream_type(kind, "streams.create_stream")
    yield request(
        config,
        "POST",
        org_path(config, "streams", name),
        f"streams.create_stream {name}",
        params={"type": kind},
        json_body={
            "fields": [{"name": f.name, "type": f.type} for f in fields],
            "settings": {
                "partition_keys": [_key_wire(k) for k in partition_keys],
                "index_fields": list(index_fields),
                "full_text_search_keys": list(full_text_search_keys),
                "bloom_filter_fields": list(bloom_filter_fields),
                "data_retention": int(retention_days),
            },
        },
    )
    return None


def delete_stream_call(config: OpenObserveConfig, stream: str, kind: str) -> Call[None]:
    name = stream_name(stream, "", "streams.delete_stream")
    stream_type(kind, "streams.delete_stream")
    yield request(
        config,
        "DELETE",
        org_path(config, "streams", name),
        f"streams.delete_stream {name}",
        params={"type": kind},
    )
    return None


class OpenObserveStreams:
    """Stream management (``client.streams``). Every method accepts ``cancel=``."""

    def __init__(self, ctx: Ctx) -> None:
        self._ctx = ctx
        self._config: OpenObserveConfig = ctx.config

    def _run(self, call: Call[_T], cancel: CancelToken | None) -> _T:
        return run_sync(self._ctx, call, cancel)

    def list_streams(
        self,
        stream_type: StreamType = "logs",
        *,
        keyword: str | None = None,
        offset: int | None = None,
        limit: int | None = None,
        cancel: CancelToken | None = None,
    ) -> list[StreamInfo]:
        """Streams of one type, sorted by name: ``GET /api/{org}/streams?type=…``.

        ``keyword`` keeps only names containing that text; ``offset``/``limit`` page the list.
        """
        return self._run(
            list_streams_call(self._config, stream_type, keyword, offset, limit), cancel
        )

    def get_stream_schema(
        self,
        stream: str,
        *,
        stream_type: StreamType = "logs",
        cancel: CancelToken | None = None,
    ) -> StreamInfo:
        """Schema, stats and settings of one stream; :class:`NotFoundError` when missing."""
        return self._run(get_stream_schema_call(self._config, stream, stream_type), cancel)

    def update_stream_settings(
        self,
        stream: str,
        *,
        retention_days: int | None = None,
        partition_keys: ListUpdate | None = None,
        full_text_search_keys: ListUpdate | None = None,
        index_fields: ListUpdate | None = None,
        bloom_filter_fields: ListUpdate | None = None,
        stream_type: StreamType = "logs",
        cancel: CancelToken | None = None,
    ) -> None:
        """Change stream settings: ``PUT /api/{org}/streams/{stream}/settings``.

        A list replaces the current list (the SDK reads the schema and sends add/remove
        deltas, one extra request); a :class:`ListDelta` is sent as given. Omitted settings stay
        unchanged. Partition keys are field names (type ``value``) or :class:`PartitionKey`.
        :class:`NotFoundError` when the stream is missing.
        """
        self._run(
            update_stream_settings_call(
                self._config,
                stream,
                retention_days=retention_days,
                partition_keys=partition_keys,
                full_text_search_keys=full_text_search_keys,
                index_fields=index_fields,
                bloom_filter_fields=bloom_filter_fields,
                kind=stream_type,
            ),
            cancel,
        )

    def create_stream(
        self,
        stream: str,
        *,
        fields: Sequence[SchemaField] = (),
        retention_days: int = 0,
        partition_keys: Sequence[PartitionKeyInput] = (),
        full_text_search_keys: Sequence[str] = (),
        index_fields: Sequence[str] = (),
        bloom_filter_fields: Sequence[str] = (),
        stream_type: StreamType = "logs",
        cancel: CancelToken | None = None,
    ) -> None:
        """Create a stream with an optional schema and settings: ``POST /api/{org}/streams/{s}``."""
        self._run(
            create_stream_call(
                self._config,
                stream,
                fields=fields,
                retention_days=retention_days,
                partition_keys=partition_keys,
                full_text_search_keys=full_text_search_keys,
                index_fields=index_fields,
                bloom_filter_fields=bloom_filter_fields,
                kind=stream_type,
            ),
            cancel,
        )

    def delete_stream(
        self,
        stream: str,
        *,
        stream_type: StreamType = "logs",
        cancel: CancelToken | None = None,
    ) -> None:
        """Delete a stream and its data: ``DELETE /api/{org}/streams/{stream}`` (alerts and
        dashboards are kept). :class:`NotFoundError` when missing."""
        self._run(delete_stream_call(self._config, stream, stream_type), cancel)


class AsyncOpenObserveStreams:
    """Async :class:`OpenObserveStreams`; same methods, awaited."""

    def __init__(self, ctx: Ctx) -> None:
        self._ctx = ctx
        self._config: OpenObserveConfig = ctx.config

    async def _run(self, call: Call[_T]) -> _T:
        return await run_async(self._ctx, call)

    async def list_streams(
        self,
        stream_type: StreamType = "logs",
        *,
        keyword: str | None = None,
        offset: int | None = None,
        limit: int | None = None,
    ) -> list[StreamInfo]:
        """See :meth:`OpenObserveStreams.list_streams`."""
        return await self._run(list_streams_call(self._config, stream_type, keyword, offset, limit))

    async def get_stream_schema(
        self, stream: str, *, stream_type: StreamType = "logs"
    ) -> StreamInfo:
        """See :meth:`OpenObserveStreams.get_stream_schema`."""
        return await self._run(get_stream_schema_call(self._config, stream, stream_type))

    async def update_stream_settings(
        self,
        stream: str,
        *,
        retention_days: int | None = None,
        partition_keys: ListUpdate | None = None,
        full_text_search_keys: ListUpdate | None = None,
        index_fields: ListUpdate | None = None,
        bloom_filter_fields: ListUpdate | None = None,
        stream_type: StreamType = "logs",
    ) -> None:
        """See :meth:`OpenObserveStreams.update_stream_settings`."""
        await self._run(
            update_stream_settings_call(
                self._config,
                stream,
                retention_days=retention_days,
                partition_keys=partition_keys,
                full_text_search_keys=full_text_search_keys,
                index_fields=index_fields,
                bloom_filter_fields=bloom_filter_fields,
                kind=stream_type,
            )
        )

    async def create_stream(
        self,
        stream: str,
        *,
        fields: Sequence[SchemaField] = (),
        retention_days: int = 0,
        partition_keys: Sequence[PartitionKeyInput] = (),
        full_text_search_keys: Sequence[str] = (),
        index_fields: Sequence[str] = (),
        bloom_filter_fields: Sequence[str] = (),
        stream_type: StreamType = "logs",
    ) -> None:
        """See :meth:`OpenObserveStreams.create_stream`."""
        await self._run(
            create_stream_call(
                self._config,
                stream,
                fields=fields,
                retention_days=retention_days,
                partition_keys=partition_keys,
                full_text_search_keys=full_text_search_keys,
                index_fields=index_fields,
                bloom_filter_fields=bloom_filter_fields,
                kind=stream_type,
            )
        )

    async def delete_stream(self, stream: str, *, stream_type: StreamType = "logs") -> None:
        """See :meth:`OpenObserveStreams.delete_stream`."""
        await self._run(delete_stream_call(self._config, stream, stream_type))
