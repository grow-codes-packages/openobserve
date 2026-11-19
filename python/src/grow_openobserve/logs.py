"""Logs ingestion (``_json``, ``_multi``, ``_bulk``, OTLP) and search (``_search``,
``_around``, ``_values``, histogram, polling tail): ``client.logs``.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import re
import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Literal

from ._core import (
    CancelToken,
    ValidationError,
    to_seconds,
)
from ._http import (
    Call,
    Ctx,
    Timestamp,
    as_dict,
    as_int,
    as_list,
    as_str,
    json_body,
    org_path,
    request,
    run_async,
    run_sync,
    sql_ident,
    stream_name,
    stream_type,
    to_micros,
    utcnow,
    vendor_error_text,
    window,
)
from ._logging import LogRecord
from ._version import __version__
from .records import to_otlp_logs_request

if TYPE_CHECKING:
    from .config import OpenObserveConfig

__all__ = [
    "DEFAULT_SEARCH_WINDOW",
    "AsyncOpenObserveLogs",
    "AsyncRowTail",
    "BulkAction",
    "BulkItem",
    "BulkItemError",
    "BulkResult",
    "FieldValue",
    "FieldValues",
    "Histogram",
    "HistogramBucket",
    "IngestResult",
    "OpenObserveLogs",
    "OtlpIngestResult",
    "RowTail",
    "SearchResult",
    "StreamType",
]

#: Default search window when ``start`` is not given: the last 15 minutes.
DEFAULT_SEARCH_WINDOW = timedelta(minutes=15)

StreamType = Literal["logs", "traces", "metrics"]
JsonRow = dict[str, Any]

_INTERVAL = re.compile(r"^\s*\d+\s*[a-zA-Z]+\s*$")


# --------------------------------------------------------------------------- models


@dataclass(frozen=True)
class IngestResult:
    """Result of ``ingest_json`` / ``ingest_multi``.

    OpenObserve answers HTTP 200 even when some records were rejected: check ``failed``.
    """

    #: Stream name as normalized by the server.
    stream: str
    successful: int
    failed: int
    #: Last per-record error reported by the server (e.g. "Too old data …"), redacted.
    error: str | None = None


@dataclass(frozen=True)
class BulkAction:
    """One ``_bulk`` action. Only ``index`` and ``create`` are accepted (the server ignores
    ``delete``/``update``, so the SDK rejects them with :class:`ValidationError`)."""

    #: Target stream (``_index``).
    stream: str
    document: Mapping[str, Any]
    action: str = "index"


@dataclass(frozen=True)
class BulkItemError:
    type: str
    reason: str


@dataclass(frozen=True)
class BulkItem:
    """Outcome of one bulk action (``status`` 200 = created)."""

    action: str
    stream: str
    status: int
    error: BulkItemError | None = None


@dataclass(frozen=True)
class BulkResult:
    """Result of ``ingest_bulk``. Always check ``errors``: item failures keep HTTP 200."""

    took: int
    errors: bool
    items: list[BulkItem] = field(default_factory=list)


@dataclass(frozen=True)
class OtlpIngestResult:
    """Result of ``ingest_otlp``."""

    accepted: int
    rejected: int


@dataclass(frozen=True)
class SearchResult:
    """Result of ``search`` and ``search_around``."""

    #: Flat rows; each has ``_timestamp`` (µs).
    hits: list[JsonRow]
    #: Rows in this page (the server's ``total``).
    total: int
    from_: int = 0
    size: int = 0
    #: Server time in milliseconds.
    took: int = 0
    scan_records: int = 0
    is_partial: bool = False
    #: Exact number of matching rows, only with ``track_total_hits``.
    total_hits: int | None = None
    #: Bucket width in seconds (histogram queries).
    histogram_interval: int | None = None
    #: The server's search trace id.
    request_id: str | None = None


@dataclass(frozen=True)
class FieldValue:
    value: str | int | float | bool
    count: int


@dataclass(frozen=True)
class FieldValues:
    """Distinct values of one field, most frequent first."""

    field: str
    values: list[FieldValue]


@dataclass(frozen=True)
class HistogramBucket:
    #: Bucket start (aware UTC).
    start: datetime
    #: The server's bucket key (naive UTC ISO string).
    key: str
    count: int


@dataclass(frozen=True)
class Histogram:
    buckets: list[HistogramBucket]
    #: Bucket width in seconds.
    interval_seconds: int | None = None


# --------------------------------------------------------------------------- calls


def _ingest_call(
    config: OpenObserveConfig,
    endpoint: str,
    stream: str,
    body: bytes,
    content_type: str,
    gzip: bool,
    timeout: float | None = None,
) -> Call[IngestResult]:
    resp = yield request(
        config,
        "POST",
        org_path(config, stream, endpoint),
        f"logs.ingest {stream}",
        content=body,
        content_type=content_type,
        gzip=gzip,
        timeout=timeout,
    )
    status = [as_dict(s) for s in as_list(as_dict(json_body(resp)).get("status"))]
    first = next((s for s in status if s.get("name") == stream), status[0] if status else {})
    error = as_str(first.get("error"))
    return IngestResult(
        stream=as_str(first.get("name")) or stream,
        successful=as_int(first.get("successful")),
        failed=as_int(first.get("failed")),
        error=vendor_error_text(error, config.known_secrets()) if error else None,
    )


def _dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False, default=str)


def ingest_json_call(
    config: OpenObserveConfig,
    stream: str,
    records: Sequence[Mapping[str, Any]] | Mapping[str, Any],
    gzip: bool,
    timeout: float | None = None,
) -> Call[IngestResult]:
    name = stream_name(stream, "", "logs.ingest_json")
    items = [records] if isinstance(records, Mapping) else list(records)
    body = _dumps(items).encode()
    return _ingest_call(config, "_json", name, body, "application/json", gzip, timeout)


def ingest_multi_call(
    config: OpenObserveConfig,
    stream: str,
    ndjson: str | bytes | Sequence[Mapping[str, Any]],
    gzip: bool,
) -> Call[IngestResult]:
    name = stream_name(stream, "", "logs.ingest_multi")
    if isinstance(ndjson, bytes):
        body = ndjson
    elif isinstance(ndjson, str):
        body = ndjson.encode()
    else:
        body = "".join(_dumps(r) + "\n" for r in ndjson).encode()
    return _ingest_call(config, "_multi", name, body, "application/x-ndjson", gzip)


def _bulk_body(actions: Sequence[BulkAction]) -> str:
    if not actions:
        raise ValidationError("logs.ingest_bulk: no actions")
    lines: list[str] = []
    for item in actions:
        if item.action not in ("index", "create"):
            raise ValidationError(
                f'logs.ingest_bulk: unsupported action "{item.action}" (only index and create)'
            )
        name = stream_name(item.stream, "", "logs.ingest_bulk")
        lines.append(_dumps({item.action: {"_index": name}}))
        lines.append(_dumps(dict(item.document)))
    return "\n".join(lines) + "\n"


def _check_raw_bulk(body: str) -> str:
    lines = [line for line in body.split("\n") if line.strip()]
    if not lines:
        raise ValidationError("logs.ingest_bulk: no actions")
    for i in range(0, len(lines), 2):
        try:
            action = json.loads(lines[i])
        except ValueError:
            raise ValidationError(f"logs.ingest_bulk: line {i + 1} is not JSON") from None
        name = next(iter(action), "") if isinstance(action, dict) else ""
        if name not in ("index", "create"):
            raise ValidationError(
                f'logs.ingest_bulk: unsupported action "{name}" on line {i + 1} '
                "(only index and create)"
            )
    return body if body.endswith("\n") else body + "\n"


def ingest_bulk_call(
    config: OpenObserveConfig, actions: Sequence[BulkAction] | str, gzip: bool
) -> Call[BulkResult]:
    body = _check_raw_bulk(actions) if isinstance(actions, str) else _bulk_body(actions)
    resp = yield request(
        config,
        "POST",
        org_path(config, "_bulk"),
        "logs.ingest_bulk",
        content=body.encode(),
        content_type="application/x-ndjson",
        gzip=gzip,
    )
    data = as_dict(json_body(resp))
    secrets = config.known_secrets()
    items: list[BulkItem] = []
    for raw in as_list(data.get("items")):
        entry: tuple[str, Any] = next(iter(as_dict(raw).items()), ("index", {}))
        item = as_dict(entry[1])
        error = None
        if item.get("error") is not None:
            err = as_dict(item.get("error"))
            error = BulkItemError(
                type=as_str(err.get("type")) or "error",
                reason=vendor_error_text(as_str(err.get("reason")) or "", secrets),
            )
        items.append(
            BulkItem(
                action=entry[0],
                stream=as_str(item.get("_index")) or "",
                status=as_int(item.get("status"), 200),
                error=error,
            )
        )
    return BulkResult(
        took=as_int(data.get("took")),
        errors=data.get("errors") is True or any(i.status >= 300 for i in items),
        items=items,
    )


def ingest_otlp_call(
    config: OpenObserveConfig,
    records: Sequence[LogRecord],
    stream: str | None,
    gzip: bool,
    timeout: float | None = None,
) -> Call[OtlpIngestResult]:
    name = stream_name(stream, config.stream, "logs.ingest_otlp")
    if not records:
        return OtlpIngestResult(0, 0)
    resp = yield request(
        config,
        "POST",
        org_path(config, "v1", "logs"),
        f"logs.ingest_otlp {name}",
        json_body=to_otlp_logs_request(records, __version__),
        headers={"stream-name": name},
        gzip=gzip,
        timeout=timeout,
    )
    data = as_dict(json_body(resp))
    partial = as_dict(data.get("partialSuccess") or data.get("partial_success"))
    rejected = as_int(partial.get("rejectedLogRecords", partial.get("rejected_log_records")))
    rejected = max(0, min(len(records), rejected))
    return OtlpIngestResult(accepted=len(records) - rejected, rejected=rejected)


def _search_result(data: Mapping[str, Any]) -> SearchResult:
    interval = data.get("histogram_interval")
    return SearchResult(
        hits=[as_dict(h) for h in as_list(data.get("hits"))],
        total=as_int(data.get("total")),
        from_=as_int(data.get("from")),
        size=as_int(data.get("size")),
        took=as_int(data.get("took")),
        scan_records=as_int(data.get("scan_records")),
        is_partial=data.get("is_partial") is True,
        histogram_interval=interval if isinstance(interval, int) else None,
        request_id=as_str(data.get("trace_id")),
    )


def raw_search_call(
    config: OpenObserveConfig, query: Mapping[str, Any], kind: str, operation: str
) -> Call[SearchResult]:
    resp = yield request(
        config,
        "POST",
        org_path(config, "_search"),
        operation,
        params={"type": kind},
        json_body={"query": dict(query)},
        idempotent=True,
    )
    return _search_result(as_dict(json_body(resp)))


def search_call(
    config: OpenObserveConfig,
    sql: str,
    *,
    start: Timestamp | None,
    end: Timestamp | None,
    from_: int,
    size: int,
    track_total_hits: bool,
    kind: str,
    operation: str = "logs.search",
) -> Call[SearchResult]:
    if not sql.strip():
        raise ValidationError(f"{operation}: sql must not be empty")
    stream_type(kind, operation)
    start_us, end_us = window(start, end, DEFAULT_SEARCH_WINDOW, operation)
    query: dict[str, Any] = {
        "sql": sql,
        "start_time": start_us,
        "end_time": end_us,
        "from": max(0, int(from_)),
        "size": int(size),
    }
    result = yield from raw_search_call(config, query, kind, operation)
    if not track_total_hits:
        return result
    # The count row is subject to "from": always ask for it from offset 0.
    counted = yield from raw_search_call(
        config, {**query, "from": 0, "track_total_hits": True}, kind, operation
    )
    first = counted.hits[0] if counted.hits else {}
    total_hits = as_int(first.get("zo_sql_num"), counted.total)
    return SearchResult(**{**result.__dict__, "total_hits": total_hits})


def search_around_call(
    config: OpenObserveConfig,
    stream: str,
    timestamp: Timestamp,
    size: int,
    sql: str | None,
    kind: str,
) -> Call[SearchResult]:
    name = stream_name(stream, "", "logs.search_around")
    stream_type(kind, "logs.search_around")
    params: dict[str, Any] = {
        "key": to_micros(timestamp, "logs.search_around"),
        "size": int(size),
        "type": kind,
    }
    if sql is not None:
        params["sql"] = base64.urlsafe_b64encode(sql.encode()).decode().rstrip("=")
    resp = yield request(
        config,
        "GET",
        org_path(config, name, "_around"),
        f"logs.search_around {name}",
        params=params,
    )
    return _search_result(as_dict(json_body(resp)))


def _field_value(key: Any) -> str | int | float | bool:
    if isinstance(key, (str, int, float, bool)):
        return key
    return _dumps(key)


def values_of_call(
    config: OpenObserveConfig,
    stream: str,
    fields: Sequence[str],
    start: Timestamp | None,
    end: Timestamp | None,
    size: int,
    kind: str,
) -> Call[list[FieldValues]]:
    name = stream_name(stream, "", "logs.values_of")
    if isinstance(fields, str):
        fields = [fields]
    if not fields:
        raise ValidationError("logs.values_of: fields must not be empty")
    stream_type(kind, "logs.values_of")
    start_us, end_us = window(start, end, DEFAULT_SEARCH_WINDOW, "logs.values_of")
    resp = yield request(
        config,
        "GET",
        org_path(config, name, "_values"),
        f"logs.values_of {name}",
        params={
            "fields": ",".join(fields),
            "start_time": start_us,
            "end_time": end_us,
            "size": int(size),
            "type": kind,
        },
    )
    out: list[FieldValues] = []
    for raw in as_list(as_dict(json_body(resp)).get("hits")):
        hit = as_dict(raw)
        values = [
            FieldValue(
                value=_field_value(as_dict(v).get("zo_sql_key")),
                count=as_int(as_dict(v).get("zo_sql_num")),
            )
            for v in as_list(hit.get("values"))
        ]
        out.append(FieldValues(field=as_str(hit.get("field")) or "", values=values))
    return out


def _parse_naive_utc(key: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(key.replace("Z", "+00:00"))
    except ValueError:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def histogram_call(
    config: OpenObserveConfig,
    stream: str,
    start: Timestamp | None,
    end: Timestamp | None,
    interval: str | None,
    filter_: str | None,
    kind: str,
) -> Call[Histogram]:
    name = stream_name(stream, "", "logs.histogram")
    if interval is not None and not _INTERVAL.match(interval):
        raise ValidationError('logs.histogram: interval must look like "1 minute"')
    bucket = "" if interval is None else f", '{interval.strip()}'"
    where = "" if filter_ is None else f" WHERE {filter_}"
    sql = (
        f"SELECT histogram(_timestamp{bucket}) AS zo_sql_key, count(*) AS zo_sql_num "
        f"FROM {sql_ident(name)}{where} GROUP BY zo_sql_key ORDER BY zo_sql_key"
    )
    result = yield from search_call(
        config,
        sql,
        start=start,
        end=end,
        from_=0,
        size=-1,
        track_total_hits=False,
        kind=kind,
        operation="logs.histogram",
    )
    buckets = []
    for hit in result.hits:
        key = as_str(hit.get("zo_sql_key")) or ""
        buckets.append(
            HistogramBucket(
                start=_parse_naive_utc(key), key=key, count=as_int(hit.get("zo_sql_num"))
            )
        )
    return Histogram(buckets=buckets, interval_seconds=result.histogram_interval)


# --------------------------------------------------------------------------- tail


class _TailCursor:
    """Cursor + duplicate filter of a polling tail (shared by the sync and async tails)."""

    def __init__(
        self,
        stream: str,
        filter_: str | None,
        start: Timestamp | None,
        overlap_s: float,
        page_size: int,
        kind: str,
    ) -> None:
        self.stream = stream_name(stream, "", "logs.tail")
        stream_type(kind, "logs.tail")
        self.filter = filter_
        self.cursor = to_micros(start, "logs.tail") if start is not None else to_micros(utcnow())
        #: The overlap window never reaches back before the requested start.
        self.floor = self.cursor
        self.overlap = max(0, int(overlap_s * 1_000_000))
        self.page_size = max(1, int(page_size))
        self.kind = kind
        self.seen: dict[str, int] = {}

    def poll_call(self, config: OpenObserveConfig) -> Call[list[JsonRow]]:
        """One poll: every new row since the cursor (pages until nothing new arrives)."""
        delivered: list[JsonRow] = []
        while True:
            since = max(self.floor, self.cursor - self.overlap)
            where = "" if self.filter is None else f" AND ({self.filter})"
            sql = (
                f"SELECT * FROM {sql_ident(self.stream)} WHERE _timestamp >= {since}{where} "
                "ORDER BY _timestamp ASC"
            )
            page = yield from search_call(
                config,
                sql,
                start=since,
                end=to_micros(utcnow()) + 60_000_000,
                from_=0,
                size=self.page_size,
                track_total_hits=False,
                kind=self.kind,
                operation="logs.tail",
            )
            fresh = 0
            for row in page.hits:
                key = _dumps(row)
                if key in self.seen:
                    continue
                ts = as_int(row.get("_timestamp"))
                self.seen[key] = ts
                fresh += 1
                self.cursor = max(self.cursor, ts)
                delivered.append(row)
            floor = self.cursor - self.overlap
            self.seen = {k: v for k, v in self.seen.items() if v >= floor}
            if len(page.hits) < self.page_size or fresh == 0:
                return delivered


class RowTail:
    """A running ``client.logs.tail`` (daemon thread). :meth:`stop` is idempotent."""

    def __init__(
        self,
        ctx: Ctx,
        cursor: _TailCursor,
        on_row: Callable[[JsonRow], None],
        interval_s: float,
        on_error: Callable[[Exception], None] | None,
    ) -> None:
        self._ctx = ctx
        self._cursor = cursor
        self._on_row = on_row
        self._on_error = on_error
        self._interval = interval_s
        self._stop = threading.Event()
        self._cancel = CancelToken()
        self._thread = threading.Thread(target=self._run, name="grow-openobserve-tail", daemon=True)
        self._thread.start()

    def poll_once(self) -> int:
        """Run one poll now; returns the number of delivered rows."""
        rows = run_sync(self._ctx, self._cursor.poll_call(self._ctx.config), self._cancel)
        for row in rows:
            if self._stop.is_set():
                break
            self._on_row(row)
        return len(rows)

    def _run(self) -> None:
        while not self._stop.wait(self._interval):
            try:
                self.poll_once()
            except Exception as exc:  # noqa: BLE001 - reported, tailing continues
                if self._on_error is not None and not self._stop.is_set():
                    self._on_error(exc)

    def stop(self) -> None:
        """Stop polling and wait for the poller thread to exit."""
        self._stop.set()
        self._cancel.cancel()
        if self._thread is not threading.current_thread():
            self._thread.join()


class AsyncRowTail:
    """A running ``AsyncOpenObserveLogs.tail`` (asyncio task). :meth:`stop` is idempotent."""

    def __init__(
        self,
        ctx: Ctx,
        cursor: _TailCursor,
        on_row: Callable[[JsonRow], None],
        interval_s: float,
        on_error: Callable[[Exception], None] | None,
    ) -> None:
        self._ctx = ctx
        self._cursor = cursor
        self._on_row = on_row
        self._on_error = on_error
        self._interval = interval_s
        self._task = asyncio.get_running_loop().create_task(self._run())

    async def poll_once(self) -> int:
        rows = await run_async(self._ctx, self._cursor.poll_call(self._ctx.config))
        for row in rows:
            self._on_row(row)
        return len(rows)

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            try:
                await self.poll_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - reported, tailing continues
                if self._on_error is not None:
                    self._on_error(exc)

    async def stop(self) -> None:
        if not self._task.done():
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task


# --------------------------------------------------------------------------- APIs


class OpenObserveLogs:
    """Logs ingestion and SQL search (``client.logs``).

    Times are aware ``datetime`` objects (naive = UTC) or ``int`` epoch **microseconds**
    (exact row timestamps); the SDK converts them to the server's microseconds itself.
    Every method accepts ``cancel=`` (a :class:`CancelToken`).
    """

    def __init__(self, ctx: Ctx) -> None:
        self._ctx = ctx
        self._config: OpenObserveConfig = ctx.config

    def _run(self, call: Call[Any], cancel: CancelToken | None) -> Any:
        return run_sync(self._ctx, call, cancel)

    def ingest_json(
        self,
        stream: str,
        records: Sequence[Mapping[str, Any]] | Mapping[str, Any],
        *,
        gzip: bool = False,
        cancel: CancelToken | None = None,
    ) -> IngestResult:
        """Write records with ``POST /api/{org}/{stream}/_json``.

        Give ``_timestamp`` in epoch µs (ms, ns and RFC 3339 are detected by the server);
        without it the server uses its own time. Nested objects are flattened with ``_`` by
        the server. Partial failures keep HTTP 200: check :attr:`IngestResult.failed`.
        """
        result: IngestResult = self._run(
            ingest_json_call(self._config, stream, records, gzip), cancel
        )
        return result

    def ingest_multi(
        self,
        stream: str,
        ndjson: str | bytes | Sequence[Mapping[str, Any]],
        *,
        gzip: bool = False,
        cancel: CancelToken | None = None,
    ) -> IngestResult:
        """Write newline-delimited JSON with ``POST /api/{org}/{stream}/_multi``.

        ``ndjson`` is the raw body or a list of objects. One bad line rejects the whole
        request (:class:`ValidationError`).
        """
        result: IngestResult = self._run(
            ingest_multi_call(self._config, stream, ndjson, gzip), cancel
        )
        return result

    def ingest_bulk(
        self,
        actions: Sequence[BulkAction] | str,
        *,
        gzip: bool = False,
        cancel: CancelToken | None = None,
    ) -> BulkResult:
        """Elasticsearch-compatible bulk write: ``POST /api/{org}/_bulk``.

        Accepts :class:`BulkAction` objects or a raw NDJSON body. ``delete`` and ``update``
        actions raise :class:`ValidationError` (the server silently ignores them).
        """
        result: BulkResult = self._run(ingest_bulk_call(self._config, actions, gzip), cancel)
        return result

    def ingest_otlp(
        self,
        records: Sequence[LogRecord],
        *,
        stream: str | None = None,
        gzip: bool = False,
        cancel: CancelToken | None = None,
    ) -> OtlpIngestResult:
        """Write SDK log records as OTLP/JSON (``POST /api/{org}/v1/logs``, header
        ``stream-name``; default: the configured stream). Trace flags are not stored."""
        result: OtlpIngestResult = self._run(
            ingest_otlp_call(self._config, records, stream, gzip), cancel
        )
        return result

    def search(
        self,
        sql: str,
        *,
        start: Timestamp | None = None,
        end: Timestamp | None = None,
        from_: int = 0,
        size: int = 100,
        track_total_hits: bool = False,
        stream_type: StreamType = "logs",
        cancel: CancelToken | None = None,
    ) -> SearchResult:
        """Run a SQL query: ``POST /api/{org}/_search?type=logs``.

        Quote stream names with double quotes (``SELECT * FROM "default"``); without
        ``ORDER BY`` rows come newest first; a SQL ``LIMIT`` overrides ``size``. ``start``
        defaults to ``end`` - 15 minutes, ``end`` to now. ``track_total_hits`` fetches the exact
        number of matching rows with one extra request into :attr:`SearchResult.total_hits`.
        """
        result: SearchResult = self._run(
            search_call(
                self._config,
                sql,
                start=start,
                end=end,
                from_=from_,
                size=size,
                track_total_hits=track_total_hits,
                kind=stream_type,
            ),
            cancel,
        )
        return result

    def search_around(
        self,
        stream: str,
        timestamp: Timestamp,
        *,
        size: int = 10,
        sql: str | None = None,
        stream_type: StreamType = "logs",
        cancel: CancelToken | None = None,
    ) -> SearchResult:
        """Rows around one timestamp: ``GET /api/{org}/{stream}/_around``.

        The anchor is a ``datetime`` or a row's exact ``_timestamp`` (µs); the anchor row is
        included. ``sql`` is an optional ``SELECT …`` filter sent base64url-encoded; OpenObserve
        v1.1.2 accepts it but does not apply it (verified), so filter the rows yourself.
        """
        result: SearchResult = self._run(
            search_around_call(self._config, stream, timestamp, size, sql, stream_type), cancel
        )
        return result

    def values_of(
        self,
        stream: str,
        fields: Sequence[str],
        *,
        start: Timestamp | None = None,
        end: Timestamp | None = None,
        size: int = 10,
        stream_type: StreamType = "logs",
        cancel: CancelToken | None = None,
    ) -> list[FieldValues]:
        """Distinct values with counts: ``GET /api/{org}/{stream}/_values?fields=a,b``
        (``size`` values per field, most frequent first; default window: last 15 minutes)."""
        result: list[FieldValues] = self._run(
            values_of_call(self._config, stream, fields, start, end, size, stream_type), cancel
        )
        return result

    def histogram(
        self,
        stream: str,
        *,
        start: Timestamp | None = None,
        end: Timestamp | None = None,
        interval: str | None = None,
        filter: str | None = None,  # noqa: A002 - SPEC option name
        stream_type: StreamType = "logs",
        cancel: CancelToken | None = None,
    ) -> Histogram:
        """Record counts per time bucket (``histogram(_timestamp, …)`` via ``_search``).

        ``interval`` looks like ``"1 minute"`` (default: chosen by the server); ``filter`` is a
        SQL ``WHERE`` condition such as ``level = 'error'``.
        """
        result: Histogram = self._run(
            histogram_call(self._config, stream, start, end, interval, filter, stream_type),
            cancel,
        )
        return result

    def tail(
        self,
        stream: str,
        on_row: Callable[[JsonRow], None],
        *,
        filter: str | None = None,  # noqa: A002 - SPEC option name
        start: Timestamp | None = None,
        interval_s: float | timedelta = 1.0,
        overlap_s: float | timedelta = 1.0,
        page_size: int = 1000,
        on_error: Callable[[Exception], None] | None = None,
        stream_type: StreamType = "logs",
    ) -> RowTail:
        """Follow a stream by polling ``_search`` with a ``_timestamp`` cursor.

        OpenObserve has no live tail. Every poll re-reads ``overlap_s`` before the cursor and
        drops rows it already delivered (full-row equality), so rows that are exact duplicates
        within that window are delivered once. ``start`` defaults to now; errors go to
        ``on_error`` and polling continues. Call :meth:`RowTail.stop` to end it.
        """
        cursor = _TailCursor(stream, filter, start, to_seconds(overlap_s), page_size, stream_type)
        return RowTail(self._ctx, cursor, on_row, to_seconds(interval_s), on_error)


class AsyncOpenObserveLogs:
    """Async :class:`OpenObserveLogs` (``AsyncOpenObserveClient.logs``); same methods, awaited."""

    def __init__(self, ctx: Ctx) -> None:
        self._ctx = ctx
        self._config: OpenObserveConfig = ctx.config

    async def _run(self, call: Call[Any]) -> Any:
        return await run_async(self._ctx, call)

    async def ingest_json(
        self,
        stream: str,
        records: Sequence[Mapping[str, Any]] | Mapping[str, Any],
        *,
        gzip: bool = False,
    ) -> IngestResult:
        """See :meth:`OpenObserveLogs.ingest_json`."""
        result: IngestResult = await self._run(
            ingest_json_call(self._config, stream, records, gzip)
        )
        return result

    async def ingest_multi(
        self,
        stream: str,
        ndjson: str | bytes | Sequence[Mapping[str, Any]],
        *,
        gzip: bool = False,
    ) -> IngestResult:
        """See :meth:`OpenObserveLogs.ingest_multi`."""
        result: IngestResult = await self._run(
            ingest_multi_call(self._config, stream, ndjson, gzip)
        )
        return result

    async def ingest_bulk(
        self, actions: Sequence[BulkAction] | str, *, gzip: bool = False
    ) -> BulkResult:
        """See :meth:`OpenObserveLogs.ingest_bulk`."""
        result: BulkResult = await self._run(ingest_bulk_call(self._config, actions, gzip))
        return result

    async def ingest_otlp(
        self, records: Sequence[LogRecord], *, stream: str | None = None, gzip: bool = False
    ) -> OtlpIngestResult:
        """See :meth:`OpenObserveLogs.ingest_otlp`."""
        result: OtlpIngestResult = await self._run(
            ingest_otlp_call(self._config, records, stream, gzip)
        )
        return result

    async def search(
        self,
        sql: str,
        *,
        start: Timestamp | None = None,
        end: Timestamp | None = None,
        from_: int = 0,
        size: int = 100,
        track_total_hits: bool = False,
        stream_type: StreamType = "logs",
    ) -> SearchResult:
        """See :meth:`OpenObserveLogs.search`."""
        result: SearchResult = await self._run(
            search_call(
                self._config,
                sql,
                start=start,
                end=end,
                from_=from_,
                size=size,
                track_total_hits=track_total_hits,
                kind=stream_type,
            )
        )
        return result

    async def search_around(
        self,
        stream: str,
        timestamp: Timestamp,
        *,
        size: int = 10,
        sql: str | None = None,
        stream_type: StreamType = "logs",
    ) -> SearchResult:
        """See :meth:`OpenObserveLogs.search_around`."""
        result: SearchResult = await self._run(
            search_around_call(self._config, stream, timestamp, size, sql, stream_type)
        )
        return result

    async def values_of(
        self,
        stream: str,
        fields: Sequence[str],
        *,
        start: Timestamp | None = None,
        end: Timestamp | None = None,
        size: int = 10,
        stream_type: StreamType = "logs",
    ) -> list[FieldValues]:
        """See :meth:`OpenObserveLogs.values_of`."""
        result: list[FieldValues] = await self._run(
            values_of_call(self._config, stream, fields, start, end, size, stream_type)
        )
        return result

    async def histogram(
        self,
        stream: str,
        *,
        start: Timestamp | None = None,
        end: Timestamp | None = None,
        interval: str | None = None,
        filter: str | None = None,  # noqa: A002 - SPEC option name
        stream_type: StreamType = "logs",
    ) -> Histogram:
        """See :meth:`OpenObserveLogs.histogram`."""
        result: Histogram = await self._run(
            histogram_call(self._config, stream, start, end, interval, filter, stream_type)
        )
        return result

    def tail(
        self,
        stream: str,
        on_row: Callable[[JsonRow], None],
        *,
        filter: str | None = None,  # noqa: A002 - SPEC option name
        start: Timestamp | None = None,
        interval_s: float | timedelta = 1.0,
        overlap_s: float | timedelta = 1.0,
        page_size: int = 1000,
        on_error: Callable[[Exception], None] | None = None,
        stream_type: StreamType = "logs",
    ) -> AsyncRowTail:
        """See :meth:`OpenObserveLogs.tail`; must be called inside a running event loop."""
        cursor = _TailCursor(stream, filter, start, to_seconds(overlap_s), page_size, stream_type)
        return AsyncRowTail(self._ctx, cursor, on_row, to_seconds(interval_s), on_error)
