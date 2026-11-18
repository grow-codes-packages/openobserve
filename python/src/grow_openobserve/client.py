"""OpenObserve clients (sync and async).

Core calls: ``health()`` → ``GET /healthz`` (no auth), ``ping()`` and ``server_version()`` →
``GET /api/{org}/config`` (Basic auth). Service modules hang off the client: ``logs``,
``streams``, ``traces``, ``admin``, plus ``log_query()``, ``log_client()``, ``trace_query()``
and ``init_tracing()``.
"""

from __future__ import annotations

import time
from collections.abc import Mapping
from types import TracebackType
from typing import Any, Literal

import httpx

from ._core import (
    AsyncHttpTransport,
    CancelledError,
    CancelToken,
    ConfigError,
    HealthStatus,
    HttpTransport,
    Request,
    ResourceRegistry,
    Response,
    ServiceError,
    TimeoutError,
    UnavailableError,
    UnsupportedError,
    ValidationError,
)
from ._http import Call, Ctx, as_dict, json_body, org_path, request, run_async, run_sync
from ._logging import (
    AsyncBatchingLogClient,
    BatchingLogClient,
    BatchOptions,
    RedactionProcessor,
)
from ._logging._client import _DEFAULT_REDACTION
from ._logging._model import AttrValue
from ._tracing import TracingConfig, TracingHandle
from .admin import AsyncOpenObserveAdmin, OpenObserveAdmin
from .config import OpenObserveConfig
from .log_query import (
    AsyncOpenObserveLogExporter,
    AsyncOpenObserveLogQuery,
    AsyncOpenObserveOtlpLogExporter,
    OpenObserveLogExporter,
    OpenObserveLogQuery,
    OpenObserveOtlpLogExporter,
)
from .logs import AsyncOpenObserveLogs, OpenObserveLogs
from .streams import AsyncOpenObserveStreams, OpenObserveStreams
from .traces import AsyncOpenObserveTraceQuery, OpenObserveTraceQuery, init_tracing

__all__ = ["AsyncOpenObserveClient", "LogProtocol", "OpenObserveClient"]

#: Log export protocol of ``log_client``/``log_exporter``.
LogProtocol = Literal["json", "otlp"]

_HEALTH_PATH = "/healthz"


_HEALTH = Request("GET", _HEALTH_PATH, operation="health", raise_for_status=False, max_retries=0)


def _health_from_response(resp: Response) -> HealthStatus:
    status = "ok" if resp.ok else "unhealthy"
    details: dict[str, str] = {}
    try:
        body: Any = resp.json()
    except ValidationError:
        body = None
    if isinstance(body, Mapping):
        for key, value in body.items():
            if key == "status" and isinstance(value, str) and value:
                status = value
            elif isinstance(value, (str, int, float, bool)):
                details[str(key)] = str(value)
    if not resp.ok:
        details.setdefault("httpStatus", str(resp.status))
    return HealthStatus(healthy=resp.ok, status=status, details=details)


def _health_from_error(err: ServiceError) -> HealthStatus:
    if isinstance(err, (CancelledError, ConfigError)):
        raise err
    word = "unreachable" if isinstance(err, (UnavailableError, TimeoutError)) else "unhealthy"
    return HealthStatus(healthy=False, status=word, details={"error": err.message})


def _config_call(config: OpenObserveConfig, operation: str) -> Call[dict[str, Any]]:
    resp = yield request(config, "GET", org_path(config, "config"), operation)
    return as_dict(json_body(resp))


def _version_call(config: OpenObserveConfig) -> Call[str]:
    body = yield from _config_call(config, "server_version")
    version = body.get("version")
    if not isinstance(version, str) or not version:
        raise UnsupportedError("server_version: the service does not report a version")
    return version


class OpenObserveClient:
    """Synchronous OpenObserve client. Thread-safe; use as a context manager or call
    :meth:`close`.

    Every I/O method accepts ``cancel=`` (a :class:`CancelToken`) to abort from another thread.

    Modules: :attr:`logs` (ingest + SQL search), :attr:`streams`, :attr:`traces` (trace query
    on the configured traces stream) and :attr:`admin`.
    """

    def __init__(
        self,
        config: OpenObserveConfig | None = None,
        *,
        http_transport: httpx.BaseTransport | None = None,
    ) -> None:
        if config is None:
            config = OpenObserveConfig.from_env()
        self.config = config.validate()
        self._resources = ResourceRegistry()
        self._ctx = Ctx(None, self.config)
        self._transport = HttpTransport(
            self.config, known_secrets=self._ctx.known_secrets, http_transport=http_transport
        )
        self._ctx.transport = self._transport
        self._log_clients: list[BatchingLogClient] = []
        #: Logs ingestion and SQL search.
        self.logs = OpenObserveLogs(self._ctx)
        #: Stream management.
        self.streams = OpenObserveStreams(self._ctx)
        #: Trace query client on the configured traces stream.
        self.traces = OpenObserveTraceQuery(self._ctx)
        #: Organizations, service accounts, passcode.
        self.admin = OpenObserveAdmin(self._ctx)

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, str] | None = None,
        **overrides: Any,
    ) -> OpenObserveClient:
        """Build a client from the ``GROW_OPENOBSERVE_*`` and ``OPENOBSERVE_*`` variables."""
        return cls(OpenObserveConfig.from_env(env, **overrides))

    @property
    def resources(self) -> ResourceRegistry:
        """Background resources stopped by :meth:`close`."""
        return self._resources

    def request(self, req: Request, *, cancel: CancelToken | None = None) -> Response:
        """Send a raw request through the client's transport (retries, hooks, redaction).

        No auth header is added: build ``req.headers`` with :meth:`OpenObserveConfig.authorization`.
        """
        return self._transport.request(req, cancel=cancel)

    def health(self, *, cancel: CancelToken | None = None) -> HealthStatus:
        """Unauthenticated health check (``GET /healthz``); never raises for an unhealthy or
        unreachable service (only :class:`CancelledError` and :class:`ConfigError`)."""
        try:
            resp = self._transport.request(_HEALTH, cancel=cancel)
        except ServiceError as err:
            return _health_from_error(err)
        return _health_from_response(resp)

    def ping(self, *, cancel: CancelToken | None = None) -> float:
        """Authenticated round trip (``GET /api/{org}/config``); returns the latency in ms.

        :class:`AuthenticationError` on bad or missing credentials.
        """
        started = time.monotonic()
        run_sync(self._ctx, _config_call(self.config, "ping"), cancel)
        return (time.monotonic() - started) * 1000.0

    def server_version(self, *, cancel: CancelToken | None = None) -> str:
        """The server version (``version`` of ``GET /api/{org}/config``, e.g. ``v1.1.2``)."""
        return run_sync(self._ctx, _version_call(self.config), cancel)

    def log_query(self, stream: str | None = None) -> OpenObserveLogQuery:
        """The :class:`LogQueryClient` for ``stream`` (default: the config's ``stream``)."""
        return OpenObserveLogQuery(self.logs, self.streams, stream or self.config.stream)

    def trace_query(self, stream: str | None = None) -> OpenObserveTraceQuery:
        """The :class:`TraceQueryClient` for ``stream`` (default: the config's
        ``traces_stream``)."""
        return OpenObserveTraceQuery(self._ctx, stream)

    def log_exporter(self, stream: str | None = None, *, protocol: LogProtocol = "json") -> Any:
        """A :class:`LogExporter` writing to ``stream`` (default: the config's ``stream``):
        ``protocol="json"`` (``_json`` ingestion, default) or ``"otlp"`` (OTLP/JSON to
        ``/v1/logs``)."""
        name = stream or self.config.stream
        if protocol not in ("json", "otlp"):
            raise ConfigError("log protocol must be json or otlp")
        if protocol == "otlp":
            return OpenObserveOtlpLogExporter(self.logs, name)
        return OpenObserveLogExporter(self.logs, name)

    def log_client(
        self,
        stream: str | None = None,
        *,
        options: BatchOptions | None = None,
        resource: Mapping[str, AttrValue] | None = None,
        redaction: RedactionProcessor | None = _DEFAULT_REDACTION,
        correlate: bool = True,
        protocol: LogProtocol = "json",
    ) -> BatchingLogClient:
        """A batching :class:`LogClient` that ships records to ``stream``.

        Records get ``resource`` (e.g. ``service.name``), default redaction and trace
        correlation from the active OpenTelemetry span. :meth:`close` of this client also
        closes the log clients it created.
        """
        log_client = BatchingLogClient(
            self.log_exporter(stream, protocol=protocol),
            options=options,
            resource=resource,
            redaction=redaction,
            correlate=correlate,
        )
        self._log_clients.append(log_client)
        return log_client

    def init_tracing(self, config: TracingConfig, *, stream: str | None = None) -> TracingHandle:
        """:func:`~grow_openobserve.init_tracing` with this client's connection settings."""
        return init_tracing(config, openobserve=self.config, stream=stream)

    def close(self) -> None:
        """Flush log clients, stop background resources and close connections (idempotent)."""
        clients, self._log_clients = self._log_clients, []
        for log_client in clients:
            log_client.close()
        self._resources.stop_all()
        self._transport.close()

    def __enter__(self) -> OpenObserveClient:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"OpenObserveClient(config={self.config!r})"


class AsyncOpenObserveClient:
    """Asynchronous OpenObserve client; use ``async with`` or call :meth:`aclose`.

    Cancel a call by cancelling its task: the SDK raises :class:`CancelledError`, which is also
    an :class:`asyncio.CancelledError`. Same modules as :class:`OpenObserveClient`.
    """

    def __init__(
        self,
        config: OpenObserveConfig | None = None,
        *,
        http_transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if config is None:
            config = OpenObserveConfig.from_env()
        self.config = config.validate()
        self._resources = ResourceRegistry()
        self._ctx = Ctx(None, self.config)
        self._transport = AsyncHttpTransport(
            self.config, known_secrets=self._ctx.known_secrets, http_transport=http_transport
        )
        self._ctx.transport = self._transport
        self._log_clients: list[AsyncBatchingLogClient] = []
        self.logs = AsyncOpenObserveLogs(self._ctx)
        self.streams = AsyncOpenObserveStreams(self._ctx)
        self.traces = AsyncOpenObserveTraceQuery(self._ctx)
        self.admin = AsyncOpenObserveAdmin(self._ctx)

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, str] | None = None,
        **overrides: Any,
    ) -> AsyncOpenObserveClient:
        """See :meth:`OpenObserveClient.from_env`."""
        return cls(OpenObserveConfig.from_env(env, **overrides))

    @property
    def resources(self) -> ResourceRegistry:
        return self._resources

    async def request(self, req: Request) -> Response:
        """See :meth:`OpenObserveClient.request`."""
        return await self._transport.request(req)

    async def health(self) -> HealthStatus:
        """See :meth:`OpenObserveClient.health`."""
        try:
            resp = await self._transport.request(_HEALTH)
        except ServiceError as err:
            return _health_from_error(err)
        return _health_from_response(resp)

    async def ping(self) -> float:
        """See :meth:`OpenObserveClient.ping`."""
        started = time.monotonic()
        await run_async(self._ctx, _config_call(self.config, "ping"))
        return (time.monotonic() - started) * 1000.0

    async def server_version(self) -> str:
        """See :meth:`OpenObserveClient.server_version`."""
        return await run_async(self._ctx, _version_call(self.config))

    def log_query(self, stream: str | None = None) -> AsyncOpenObserveLogQuery:
        """See :meth:`OpenObserveClient.log_query`."""
        return AsyncOpenObserveLogQuery(self.logs, self.streams, stream or self.config.stream)

    def trace_query(self, stream: str | None = None) -> AsyncOpenObserveTraceQuery:
        """See :meth:`OpenObserveClient.trace_query`."""
        return AsyncOpenObserveTraceQuery(self._ctx, stream)

    def log_exporter(self, stream: str | None = None, *, protocol: LogProtocol = "json") -> Any:
        """See :meth:`OpenObserveClient.log_exporter` (async exporters)."""
        name = stream or self.config.stream
        if protocol not in ("json", "otlp"):
            raise ConfigError("log protocol must be json or otlp")
        if protocol == "otlp":
            return AsyncOpenObserveOtlpLogExporter(self.logs, name)
        return AsyncOpenObserveLogExporter(self.logs, name)

    def log_client(
        self,
        stream: str | None = None,
        *,
        options: BatchOptions | None = None,
        resource: Mapping[str, AttrValue] | None = None,
        redaction: RedactionProcessor | None = _DEFAULT_REDACTION,
        correlate: bool = True,
        protocol: LogProtocol = "json",
    ) -> AsyncBatchingLogClient:
        """See :meth:`OpenObserveClient.log_client`; must be called inside a running loop."""
        log_client = AsyncBatchingLogClient(
            self.log_exporter(stream, protocol=protocol),
            options=options,
            resource=resource,
            redaction=redaction,
            correlate=correlate,
        )
        self._log_clients.append(log_client)
        return log_client

    def init_tracing(self, config: TracingConfig, *, stream: str | None = None) -> TracingHandle:
        """See :meth:`OpenObserveClient.init_tracing`."""
        return init_tracing(config, openobserve=self.config, stream=stream)

    async def aclose(self) -> None:
        """Flush log clients, stop background resources and close connections (idempotent)."""
        clients, self._log_clients = self._log_clients, []
        for log_client in clients:
            await log_client.close()
        self._resources.stop_all()
        await self._transport.aclose()

    close = aclose

    async def __aenter__(self) -> AsyncOpenObserveClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    def __repr__(self) -> str:
        return f"AsyncOpenObserveClient(config={self.config!r})"
