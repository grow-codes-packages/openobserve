"""Tracing for OpenObserve: ``init_tracing`` (OTLP exporters to ``/api/{org}/v1/traces``) and
the :class:`OpenObserveTraceQuery` trace query client (``client.traces``).
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import timedelta
from typing import TYPE_CHECKING, Any, TypeVar
from urllib.parse import quote, urlsplit, urlunsplit

from opentelemetry.sdk.trace import SpanProcessor
from opentelemetry.sdk.trace.export import SpanExporter
from opentelemetry.sdk.trace.id_generator import IdGenerator

from ._core import CancelToken, ConfigError, NotFoundError, UnsupportedError, ValidationError
from ._http import (
    Call,
    Ctx,
    Timestamp,
    as_dict,
    as_int,
    as_list,
    as_str,
    flatten_key,
    json_body,
    org_path,
    request,
    run_async,
    run_sync,
    sql_ident,
    sql_string,
    stream_name,
    window,
)
from ._tracing import (
    AsyncBaseTraceQueryClient,
    BaseTraceQueryClient,
    DependencyLink,
    PropagatorFactory,
    Span,
    SpanEvent,
    SpanLink,
    SpanStatus,
    Trace,
    TraceSearchQuery,
    TraceSummary,
    TracingConfig,
    TracingHandle,
    init_tracing_with,
    normalize_trace_id,
)
from .config import OpenObserveConfig

if TYPE_CHECKING:
    from ._tracing import SpanKindName, StatusCodeName

__all__ = [
    "DEFAULT_GET_TRACE_WINDOW",
    "DEFAULT_SERVICES_WINDOW",
    "DEFAULT_TRACE_SEARCH_WINDOW",
    "OPENOBSERVE_GRPC_PORT",
    "AsyncOpenObserveTraceQuery",
    "OpenObserveTraceQuery",
    "TraceTimeRange",
    "create_span_exporter",
    "grpc_endpoint",
    "hit_to_summary",
    "init_tracing",
    "row_to_span",
    "trace_filter",
    "traces_endpoint",
    "tracing_defaults",
]

#: Default window of ``search_traces``: the last hour.
DEFAULT_TRACE_SEARCH_WINDOW = timedelta(hours=1)
#: Default window of ``get_trace``: the last 7 days.
DEFAULT_GET_TRACE_WINDOW = timedelta(days=7)
#: Default window of ``list_services`` / ``list_operations``: the last 24 hours.
DEFAULT_SERVICES_WINDOW = timedelta(hours=24)
#: Default OTLP/gRPC port of OpenObserve.
OPENOBSERVE_GRPC_PORT = 5081

_T = TypeVar("_T")
_PAGE_SIZE = 1000
_MAX_PAGES = 1000

_KINDS: dict[str, SpanKindName] = {
    "0": "unspecified",
    "1": "internal",
    "2": "server",
    "3": "client",
    "4": "producer",
    "5": "consumer",
}

#: Span row columns mapped to span fields or internal to OpenObserve.
_SPAN_COLUMNS = frozenset(
    {
        "_timestamp",
        "_o2_ingest_ts",
        "trace_id",
        "span_id",
        "reference_parent_span_id",
        "reference_parent_trace_id",
        "reference_ref_type",
        "operation_name",
        "span_kind",
        "service_name",
        "start_time",
        "end_time",
        "duration",
        "span_status",
        "status_code",
        "status_message",
        "flags",
        "events",
        "links",
    }
)


@dataclass(frozen=True)
class TraceTimeRange:
    """Result of ``trace_time_range``: where and when a trace was stored."""

    found: bool
    #: Stream that holds the trace.
    stream: str | None = None
    start_micros: int | None = None
    end_micros: int | None = None


# --------------------------------------------------------------------------- row mapping


def _ns_to_micros(value: Any) -> int:
    return as_int(value) // 1000


def _attr(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)):
        return value
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def _attributes(source: Mapping[str, Any], skip: Any) -> dict[str, Any]:
    return {k: _attr(v) for k, v in source.items() if v is not None and not skip(k)}


def _json_list(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            return []
    return [as_dict(v) for v in as_list(value)]


def _skip_span_column(key: str) -> bool:
    return key in _SPAN_COLUMNS or key.startswith(("infer_service_", "o2_db_"))


def row_to_span(row: Mapping[str, Any]) -> Span:
    """Map an OpenObserve span row to the normalized :class:`Span`.

    ``start_time``/``end_time`` are nanoseconds, ``span_kind`` is a string number, ``events``
    and ``links`` are JSON strings. Internal columns (``_timestamp``, ``infer_service_*``,
    ``o2_db_*``, …) are dropped; the rest become attributes under the server's flattened names.
    """
    start = _ns_to_micros(row.get("start_time"))
    end = _ns_to_micros(row.get("end_time"))
    status_text = (as_str(row.get("span_status")) or "").upper()
    status_code = as_int(row.get("status_code"))
    code: StatusCodeName = (
        "error"
        if status_text == "ERROR" or status_code == 2
        else "ok"
        if status_text == "OK" or status_code == 1
        else "unset"
    )
    events = [
        SpanEvent(
            name=as_str(e.get("name")) or "",
            time_micros=_ns_to_micros(e.get("_timestamp")),
            attributes=_attributes(e, lambda k: k in ("name", "_timestamp")),
        )
        for e in _json_list(row.get("events"))
    ]
    links = []
    for link in _json_list(row.get("links")):
        context = as_dict(link.get("context"))
        trace_id = as_str(context.get("traceId")) or as_str(context.get("trace_id"))
        span_id = as_str(context.get("spanId")) or as_str(context.get("span_id"))
        if not trace_id or not span_id:
            continue
        links.append(
            SpanLink(
                trace_id=trace_id,
                span_id=span_id,
                attributes=_attributes(
                    link,
                    lambda k: (
                        k in ("context", "droppedAttributesCount", "dropped_attributes_count")
                    ),
                ),
            )
        )
    return Span(
        trace_id=as_str(row.get("trace_id")) or "",
        span_id=as_str(row.get("span_id")) or "",
        parent_span_id=as_str(row.get("reference_parent_span_id")),
        name=as_str(row.get("operation_name")) or "",
        kind=_KINDS.get(str(row.get("span_kind", "0")), "unspecified"),
        service_name=as_str(row.get("service_name")) or "",
        start_micros=start,
        end_micros=end,
        duration_micros=end - start if end >= start else as_int(row.get("duration")),
        status=SpanStatus(code, as_str(row.get("status_message"))),
        attributes=_attributes(row, _skip_span_column),
        events=events,
        links=links,
    )


def hit_to_summary(hit: Mapping[str, Any]) -> TraceSummary:
    """Map one ``traces/latest`` hit to a :class:`TraceSummary`."""
    first = as_dict(hit.get("first_event"))
    spans = as_list(hit.get("spans"))
    return TraceSummary(
        trace_id=normalize_trace_id(as_str(hit.get("trace_id")) or ""),
        start_micros=_ns_to_micros(hit.get("start_time")),
        duration_micros=as_int(hit.get("duration")),
        span_count=as_int(spans[0]) if spans else 0,
        root_service_name=as_str(first.get("service_name")),
        root_name=as_str(first.get("operation_name")),
    )


def trace_filter(query: TraceSearchQuery) -> str:
    """The ``traces/latest`` filter (a SQL ``WHERE`` fragment) for a search query.

    Tag keys are flattened like the server stores them (``db.system`` → ``db_system``).
    """
    parts: list[str] = []
    if query.service is not None:
        parts.append(f"service_name = {sql_string(query.service)}")
    if query.operation is not None:
        parts.append(f"operation_name = {sql_string(query.operation)}")
    for key, value in query.tags.items():
        column = flatten_key(key)
        if not column:
            raise ValidationError("tracing.search_traces: empty tag key")
        parts.append(f"{column} = {sql_string(str(value))}")
    bounds: tuple[tuple[int | None, str], ...] = (
        (query.min_duration_micros, ">="),
        (query.max_duration_micros, "<="),
    )
    for micros, op in bounds:
        if micros is None:
            continue
        if micros < 0:
            raise ValidationError("tracing.search_traces: durations must be >= 0")
        parts.append(f"duration {op} {int(micros)}")
    return " AND ".join(parts)


# --------------------------------------------------------------------------- calls


def get_trace_call(
    config: OpenObserveConfig,
    stream: str,
    trace_id: str,
    start: Timestamp | None,
    end: Timestamp | None,
) -> Call[Trace | None]:
    tid = normalize_trace_id(trace_id)
    start_us, end_us = window(start, end, DEFAULT_GET_TRACE_WINDOW, "tracing.get_trace")
    spans: dict[str, Span] = {}
    after: tuple[str, str] | None = None
    for _ in range(_MAX_PAGES):
        params: dict[str, Any] = {"start_time": start_us, "end_time": end_us, "size": _PAGE_SIZE}
        if after is not None:
            params["after_start_time"], params["after_span_id"] = after
        try:
            resp = yield request(
                config,
                "GET",
                org_path(config, stream, "traces", tid, "details"),
                f"tracing.get_trace {tid}",
                params=params,
            )
        except NotFoundError:
            return None
        data = as_dict(json_body(resp))
        for hit in as_list(data.get("hits")):
            span = row_to_span(as_dict(hit))
            spans[span.span_id] = span
        nxt = as_dict(data.get("next_after"))
        next_start = nxt.get("start_time")
        next_span = as_str(nxt.get("span_id"))
        if data.get("has_more") is not True or next_start is None or not next_span:
            break
        after = (str(next_start), next_span)
    if not spans:
        return None
    return Trace(tid, list(spans.values()))


def search_traces_call(
    config: OpenObserveConfig, stream: str, query: TraceSearchQuery
) -> Call[list[TraceSummary]]:
    limit = int(query.limit)
    if limit <= 0:
        raise ValidationError("tracing.search_traces: limit must be positive")
    start_us, end_us = window(
        query.start, query.end, DEFAULT_TRACE_SEARCH_WINDOW, "tracing.search_traces"
    )
    filter_ = trace_filter(query)
    try:
        resp = yield request(
            config,
            "GET",
            org_path(config, stream, "traces", "latest"),
            "tracing.search_traces",
            params={
                "start_time": start_us,
                "end_time": end_us,
                "from": 0,
                "size": limit,
                "filter": filter_ or None,
            },
        )
    except NotFoundError:
        return []
    hits = as_list(as_dict(json_body(resp)).get("hits"))
    return [hit_to_summary(as_dict(h)) for h in hits][:limit]


def distinct_call(
    config: OpenObserveConfig,
    stream: str,
    column: str,
    where: str,
    operation: str,
    start: Timestamp | None,
    end: Timestamp | None,
) -> Call[list[str]]:
    start_us, end_us = window(start, end, DEFAULT_SERVICES_WINDOW, operation)
    sql = f"SELECT DISTINCT {column} FROM {sql_ident(stream)}{where} ORDER BY {column}"
    try:
        resp = yield request(
            config,
            "POST",
            org_path(config, "_search"),
            operation,
            params={"type": "traces"},
            json_body={
                "query": {
                    "sql": sql,
                    "start_time": start_us,
                    "end_time": end_us,
                    "from": 0,
                    "size": 10_000,
                }
            },
            idempotent=True,
        )
    except NotFoundError:
        return []
    values = {
        v
        for v in (
            as_str(as_dict(h).get(column)) for h in as_list(as_dict(json_body(resp)).get("hits"))
        )
        if v is not None
    }
    return sorted(values)


def trace_time_range_call(config: OpenObserveConfig, trace_id: str) -> Call[TraceTimeRange]:
    tid = normalize_trace_id(trace_id)
    resp = yield request(
        config,
        "GET",
        org_path(config, "traces", "time_range"),
        f"tracing.trace_time_range {tid}",
        params={"trace_id": tid},
    )
    for raw in as_list(as_dict(json_body(resp)).get("results")):
        result = as_dict(raw)
        if as_str(result.get("status")) == "found":
            span = as_dict(result.get("range"))
            return TraceTimeRange(
                found=True,
                stream=as_str(result.get("stream")),
                start_micros=as_int(span.get("start_time")),
                end_micros=as_int(span.get("end_time")),
            )
    return TraceTimeRange(found=False)


def _dependencies_unsupported() -> UnsupportedError:
    return UnsupportedError(
        "tracing.get_dependencies: the service graph is an enterprise feature of OpenObserve"
    )


# --------------------------------------------------------------------------- query clients


class OpenObserveTraceQuery(BaseTraceQueryClient):
    """:class:`TraceQueryClient` on one traces stream (default: the config's ``traces_stream``).

    ``search_traces`` uses ``traces/latest``; with a filter each summary covers only the
    matching spans (start, duration and span count), which keeps the call cheap.
    ``get_dependencies`` raises :class:`UnsupportedError`: the service graph is an enterprise
    feature (404 in the OSS server). Use ``client.traces`` or ``client.trace_query(stream)``.
    """

    def __init__(self, ctx: Ctx, stream: str | None = None) -> None:
        self._ctx = ctx
        self._config: OpenObserveConfig = ctx.config
        #: The traces stream.
        self.stream = stream_name(stream, self._config.traces_stream, "tracing.query")

    def _run(self, call: Call[_T], cancel: CancelToken | None) -> _T:
        return run_sync(self._ctx, call, cancel)

    def get_trace(
        self,
        trace_id: str,
        *,
        start: Timestamp | None = None,
        end: Timestamp | None = None,
        cancel: CancelToken | None = None,
    ) -> Trace | None:
        """All spans of a trace (``traces/{id}/details``, paginated), or ``None`` when the trace
        (or the stream) does not exist in the window (default: the last 7 days)."""
        return self._run(get_trace_call(self._config, self.stream, trace_id, start, end), cancel)

    def search_traces(
        self, query: TraceSearchQuery | None = None, *, cancel: CancelToken | None = None
    ) -> list[TraceSummary]:
        """Recent traces matching ``query`` (``traces/latest``); default window: the last hour."""
        return self._run(
            search_traces_call(self._config, self.stream, query or TraceSearchQuery()), cancel
        )

    def list_services(
        self,
        *,
        start: Timestamp | None = None,
        end: Timestamp | None = None,
        cancel: CancelToken | None = None,
    ) -> list[str]:
        """Distinct ``service_name`` values in the window (default: the last 24 hours), sorted."""
        return self._run(
            distinct_call(
                self._config,
                self.stream,
                "service_name",
                "",
                "tracing.list_services",
                start,
                end,
            ),
            cancel,
        )

    def list_operations(
        self,
        service: str,
        *,
        start: Timestamp | None = None,
        end: Timestamp | None = None,
        cancel: CancelToken | None = None,
    ) -> list[str]:
        """Distinct operation names of one service in the window (default: 24 hours), sorted."""
        return self._run(
            distinct_call(
                self._config,
                self.stream,
                "operation_name",
                f" WHERE service_name = {sql_string(service)}",
                "tracing.list_operations",
                start,
                end,
            ),
            cancel,
        )

    def get_dependencies(self, *, start: Any = None, end: Any = None) -> list[DependencyLink]:
        """Always :class:`UnsupportedError` (enterprise-only service graph)."""
        raise _dependencies_unsupported()

    def trace_time_range(
        self, trace_id: str, *, cancel: CancelToken | None = None
    ) -> TraceTimeRange:
        """Where and when a trace was stored (``traces/time_range``, the server's trace index)."""
        return self._run(trace_time_range_call(self._config, trace_id), cancel)


class AsyncOpenObserveTraceQuery(AsyncBaseTraceQueryClient):
    """Async :class:`OpenObserveTraceQuery`; same methods, awaited."""

    def __init__(self, ctx: Ctx, stream: str | None = None) -> None:
        self._ctx = ctx
        self._config: OpenObserveConfig = ctx.config
        self.stream = stream_name(stream, self._config.traces_stream, "tracing.query")

    async def _run(self, call: Call[_T]) -> _T:
        return await run_async(self._ctx, call)

    async def get_trace(
        self, trace_id: str, *, start: Timestamp | None = None, end: Timestamp | None = None
    ) -> Trace | None:
        """See :meth:`OpenObserveTraceQuery.get_trace`."""
        return await self._run(get_trace_call(self._config, self.stream, trace_id, start, end))

    async def search_traces(self, query: TraceSearchQuery | None = None) -> list[TraceSummary]:
        """See :meth:`OpenObserveTraceQuery.search_traces`."""
        return await self._run(
            search_traces_call(self._config, self.stream, query or TraceSearchQuery())
        )

    async def list_services(
        self, *, start: Timestamp | None = None, end: Timestamp | None = None
    ) -> list[str]:
        """See :meth:`OpenObserveTraceQuery.list_services`."""
        return await self._run(
            distinct_call(
                self._config, self.stream, "service_name", "", "tracing.list_services", start, end
            )
        )

    async def list_operations(
        self, service: str, *, start: Timestamp | None = None, end: Timestamp | None = None
    ) -> list[str]:
        """See :meth:`OpenObserveTraceQuery.list_operations`."""
        return await self._run(
            distinct_call(
                self._config,
                self.stream,
                "operation_name",
                f" WHERE service_name = {sql_string(service)}",
                "tracing.list_operations",
                start,
                end,
            )
        )

    async def get_dependencies(self, *, start: Any = None, end: Any = None) -> list[DependencyLink]:
        """Always :class:`UnsupportedError` (enterprise-only service graph)."""
        raise _dependencies_unsupported()

    async def trace_time_range(self, trace_id: str) -> TraceTimeRange:
        """See :meth:`OpenObserveTraceQuery.trace_time_range`."""
        return await self._run(trace_time_range_call(self._config, trace_id))


# --------------------------------------------------------------------------- init_tracing


def traces_endpoint(config: OpenObserveConfig) -> str:
    """The OTLP/HTTP traces endpoint: ``<url>/api/{org}/v1/traces``."""
    return config.url.rstrip("/") + f"/api/{quote(config.org, safe='')}/v1/traces"


def grpc_endpoint(config: OpenObserveConfig) -> str:
    """The default OTLP/gRPC endpoint: the URL's host with port 5081 (when it uses 5080 or
    none)."""
    parts = urlsplit(config.url)
    host = parts.hostname or "127.0.0.1"
    if ":" in host:
        host = f"[{host}]"
    port = parts.port
    if port in (None, 5080):
        port = OPENOBSERVE_GRPC_PORT
    return urlunsplit((parts.scheme, f"{host}:{port}", "", "", ""))


def tracing_defaults(config: OpenObserveConfig, stream: str | None = None) -> dict[str, Any]:
    """Service defaults merged under the caller's :class:`TracingConfig`: OTLP/HTTP protobuf to
    :func:`traces_endpoint` with ``authorization`` (Basic) and ``stream-name`` headers."""
    headers = {"stream-name": stream_name(stream, config.traces_stream, "tracing")}
    auth = config.authorization()
    if auth is not None:
        headers["authorization"] = auth
    return {"protocol": "http/protobuf", "endpoint": traces_endpoint(config), "headers": headers}


def create_span_exporter(config: TracingConfig, openobserve: OpenObserveConfig) -> SpanExporter:
    """The span exporter for a resolved config.

    ``http/protobuf`` and ``native`` → OTLP/HTTP protobuf (the official exporter); ``grpc`` →
    OTLP/gRPC (extra ``grpc``) with ``authorization``, ``organization`` and ``stream-name``
    metadata. TLS settings come from ``config.tls`` or else the OpenObserve config.
    """
    tls = config.tls or openobserve.tls
    timeout = config.batch.export_timeout_s
    gzip = config.compression == "gzip"
    if config.protocol == "grpc":
        try:
            import grpc  # type: ignore[import-untyped]  # noqa: PLC0415 - optional extra
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (  # noqa: PLC0415
                OTLPSpanExporter as GrpcSpanExporter,
            )
        except ImportError as exc:  # pragma: no cover - depends on the installed extras
            raise ConfigError("tracing protocol grpc needs the 'grpc' extra") from exc
        endpoint = config.endpoint or grpc_endpoint(openobserve)
        metadata = {k.lower(): v for k, v in config.headers.items()}
        metadata.setdefault("organization", openobserve.org)
        return GrpcSpanExporter(
            endpoint=endpoint,
            insecure=urlsplit(endpoint).scheme != "https",
            headers=metadata,
            timeout=timeout,
            compression=grpc.Compression.Gzip if gzip else grpc.Compression.NoCompression,
        )
    from opentelemetry.exporter.otlp.proto.http import Compression  # noqa: PLC0415
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import (  # noqa: PLC0415
        OTLPSpanExporter,
    )

    session = None
    if tls.insecure_skip_verify:
        import requests  # noqa: PLC0415 - dependency of the OTLP/HTTP exporter

        session = requests.Session()
        session.verify = False
    return OTLPSpanExporter(
        endpoint=config.endpoint or traces_endpoint(openobserve),
        certificate_file=tls.ca_cert_path,
        client_key_file=tls.client_key_path,
        client_certificate_file=tls.client_cert_path,
        headers=dict(config.headers),
        timeout=timeout,
        compression=Compression.Gzip if gzip else Compression.NoCompression,
        session=session,
    )


def init_tracing(
    config: TracingConfig,
    *,
    openobserve: OpenObserveConfig | None = None,
    stream: str | None = None,
    propagator_factories: Mapping[str, PropagatorFactory] | None = None,
    span_processors: Sequence[SpanProcessor] = (),
    id_generator: IdGenerator | None = None,
) -> TracingHandle:
    """Set up OpenTelemetry tracing that exports to OpenObserve (SPEC §5).

    Endpoint, auth and traces stream come from ``openobserve`` (default
    ``OpenObserveConfig.from_env()``) and ``stream`` (default: its ``traces_stream``); the
    caller's ``config.endpoint`` and ``config.headers`` override them. With ``protocol="grpc"``
    and no endpoint, the URL's host on port 5081 is used. Raises :class:`ConfigError` for
    invalid settings.
    """
    oo = openobserve if openobserve is not None else OpenObserveConfig.from_env()
    defaults = tracing_defaults(oo, stream)
    endpoint = config.endpoint
    if endpoint is None:
        endpoint = grpc_endpoint(oo) if config.protocol == "grpc" else defaults["endpoint"]
    resolved = replace(
        config, endpoint=endpoint, headers={**defaults["headers"], **dict(config.headers)}
    )
    return init_tracing_with(
        resolved,
        lambda cfg: create_span_exporter(cfg, oo),
        propagator_factories=propagator_factories,
        span_processors=span_processors,
        id_generator=id_generator,
    )
