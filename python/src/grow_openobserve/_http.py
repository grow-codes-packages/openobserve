"""Internal helpers shared by the OpenObserve service modules (not public API).

Every multi-step operation is written once as a *call*: a generator that yields
:class:`Request` objects and receives :class:`Response` objects. :func:`run_sync` and
:func:`run_async` drive the same call over the sync or async transport, so the sync and async
clients share all request building, parsing and error mapping.
"""

from __future__ import annotations

import gzip as _gzip
import json
import threading
from collections.abc import Generator, Mapping
from datetime import datetime, timedelta, timezone
from typing import Any, TypeVar
from urllib.parse import quote

from ._core import (
    AsyncHttpTransport,
    CancelToken,
    ConflictError,
    HttpTransport,
    NotFoundError,
    Request,
    Response,
    ServiceError,
    ValidationError,
    error_from_status,
    parse_retry_after,
    redact_text,
    truncate,
)
from .config import OpenObserveConfig, normalize_stream_name

T = TypeVar("T")

#: A sans-IO operation: yields requests, receives responses, returns the result.
Call = Generator[Request, Response, T]

#: A point in time: an aware ``datetime`` (naive = UTC) or epoch **microseconds**.
Timestamp = datetime | int

STREAM_TYPES = ("logs", "traces", "metrics")
_STREAM_NOT_FOUND_CODE = 20002


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def to_micros(value: Timestamp, what: str = "time") -> int:
    """Epoch microseconds of a ``datetime`` (naive = UTC) or an ``int`` (already µs)."""
    if isinstance(value, bool):
        raise ValidationError(f"{what}: invalid timestamp")
    if isinstance(value, int):
        return value
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        delta = value - datetime(1970, 1, 1, tzinfo=timezone.utc)
        return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds
    raise ValidationError(f"{what}: invalid timestamp")


def micros_to_dt(micros: int) -> datetime:
    """Aware UTC datetime for epoch microseconds."""
    return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=int(micros))


def window(
    start: Timestamp | None, end: Timestamp | None, default: timedelta, what: str
) -> tuple[int, int]:
    """``(start_µs, end_µs)`` with defaults ``end = now`` and ``start = end - default``."""
    end_us = to_micros(end, what) if end is not None else to_micros(utcnow())
    start_us = to_micros(start, what) if start is not None else end_us - to_micros_delta(default)
    if start_us > end_us:
        raise ValidationError(f"{what}: start must not be after end")
    return start_us, end_us


def to_micros_delta(value: timedelta) -> int:
    return (value.days * 86_400 + value.seconds) * 1_000_000 + value.microseconds


def stream_name(name: str | None, fallback: str, operation: str) -> str:
    """Normalized stream name; :class:`ValidationError` when blank."""
    normalized = normalize_stream_name(name if name is not None else fallback)
    if not normalized:
        raise ValidationError(f"{operation}: stream name must not be blank")
    return normalized


def stream_type(value: str, operation: str) -> str:
    if value not in STREAM_TYPES:
        raise ValidationError(f"{operation}: stream type must be logs, traces or metrics")
    return value


def sql_string(value: str) -> str:
    """A single-quoted SQL string literal (quotes doubled)."""
    return "'" + str(value).replace("'", "''") + "'"


def sql_ident(name: str) -> str:
    """A double-quoted SQL identifier (``"default"`` is a reserved word)."""
    return '"' + str(name).replace('"', '""') + '"'


def flatten_key(key: str) -> str:
    """The server's field-name flattening: lower case, every non ``[a-z0-9_]`` → ``_``."""
    return normalize_stream_name(key)


def org_path(config: OpenObserveConfig, *segments: str) -> str:
    """``/api/{org}/…`` with every segment URL-encoded."""
    parts = ["api", config.org, *segments]
    return "/" + "/".join(quote(p, safe="") for p in parts)


def request(
    config: OpenObserveConfig,
    method: str,
    path: str,
    operation: str,
    *,
    params: Mapping[str, Any] | None = None,
    json_body: Any = None,
    content: bytes | None = None,
    content_type: str | None = None,
    headers: Mapping[str, str] | None = None,
    gzip: bool = False,
    auth: bool = True,
    idempotent: bool | None = None,
    timeout: float | None = None,
) -> Request:
    """A request with Basic auth, optional gzip body and ``raise_for_status=False``."""
    hdrs: dict[str, str] = {}
    if auth:
        header = config.authorization()
        if header is not None:
            hdrs["Authorization"] = header
    if json_body is not None:
        content = json.dumps(json_body, separators=(",", ":"), ensure_ascii=False).encode()
        content_type = content_type or "application/json"
    if content_type:
        hdrs["Content-Type"] = content_type
    if gzip and content is not None:
        content = _gzip.compress(content)
        hdrs["Content-Encoding"] = "gzip"
    if headers:
        hdrs.update(headers)
    return Request(
        method,
        path,
        operation=operation,
        params=params,
        content=content,
        headers=hdrs,
        idempotent=idempotent,
        timeout=timeout,
        raise_for_status=False,
    )


def _protobuf_status_message(content: bytes) -> str | None:
    """``message`` (field 2) of a protobuf ``google.rpc.Status``; ``None`` when unparsable."""
    pos = 0
    try:
        while pos < len(content):
            tag, pos = _varint(content, pos)
            field, wire = tag >> 3, tag & 7
            if wire == 0:
                _, pos = _varint(content, pos)
            elif wire == 2:
                size, pos = _varint(content, pos)
                chunk = content[pos : pos + size]
                pos += size
                if field == 2:
                    return chunk.decode("utf-8", errors="replace") or None
            else:
                return None
    except IndexError:
        return None
    return None


def _varint(data: bytes, pos: int) -> tuple[int, int]:
    result = shift = 0
    while True:
        byte = data[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return result, pos
        shift += 7


def decode_body(content: bytes) -> Any:
    try:
        return json.loads(content) if content else None
    except ValueError:
        return None


def map_error(resp: Response, operation: str, secrets: list[str]) -> ServiceError:
    """SPEC 2.1 error for an OpenObserve error response.

    Vendor code ``20002`` / "stream not found" (HTTP 400) → :class:`NotFoundError`, "already
    exists" (HTTP 400) → :class:`ConflictError`. The body's search ``trace_id`` becomes
    ``request_id``. Protobuf ``google.rpc.Status`` bodies (OTLP) are decoded for the message.
    """
    retry_after = None
    if resp.status in (429, 503):
        retry_after = parse_retry_after(resp.headers.get("retry-after"))
    content = resp.content
    content_type = resp.headers.get("content-type", "")
    if "protobuf" in content_type:
        text = _protobuf_status_message(content)
        content = json.dumps({"message": text}).encode() if text else b""
    elif content and not content_type and not content.lstrip().startswith((b"{", b"[")):
        # OpenObserve sends some plain-text errors without a content type.
        content_type = "text/plain"
    err = error_from_status(
        resp.status,
        operation,
        content=content,
        headers=_with_content_type(resp, content_type),
        known_secrets=secrets,
        retry_after=retry_after,
    )
    body = decode_body(content)
    vendor_code = body.get("code") if isinstance(body, dict) else None
    trace_id = body.get("trace_id") if isinstance(body, dict) else None
    request_id = err.request_id or (trace_id if isinstance(trace_id, str) and trace_id else None)
    cls: type[ServiceError] = type(err)
    lower = err.message.lower()
    if resp.status == 400 and (
        vendor_code == _STREAM_NOT_FOUND_CODE or "stream not found" in lower
    ):
        cls = NotFoundError
    elif resp.status == 400 and "already exists" in lower:
        cls = ConflictError
    if cls is type(err) and request_id == err.request_id:
        return err
    return cls(
        err.message,
        http_status=err.http_status,
        request_id=request_id,
        retryable=err.retryable,
        retry_after=err.retry_after,
    )


def _with_content_type(resp: Response, content_type: str) -> Any:
    headers = resp.headers.copy()
    if content_type:
        headers["content-type"] = content_type
    return headers


def vendor_error_text(text: str, secrets: list[str]) -> str:
    """Redacted, truncated vendor text for messages."""
    return truncate(redact_text(text, secrets), 300)


def json_body(resp: Response) -> Any:
    """The decoded JSON body (``ValidationError`` when the body is not JSON)."""
    if not resp.content:
        return None
    return resp.json()


def as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def as_int(value: Any, default: int = 0) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value.strip())
        except ValueError:
            try:
                return int(float(value))
            except ValueError:
                return default
    return default


def as_str(value: Any) -> str | None:
    return value if isinstance(value, str) and value != "" else None


class Ctx:
    """What a service module needs: the transport, the config and the redaction secrets."""

    def __init__(self, transport: Any, config: OpenObserveConfig) -> None:
        self.transport = transport
        self.config = config
        self._extra: list[str] = []
        self._lock = threading.Lock()

    def known_secrets(self) -> list[str]:
        """Config credentials plus secrets returned by the server (tokens, passcodes)."""
        with self._lock:
            extra = list(self._extra)
        return self.config.known_secrets() + extra

    def add_secret(self, value: str | None) -> None:
        """Redact ``value`` from every later error message and log line of this client."""
        if value and len(value) >= 4:
            with self._lock:
                if value not in self._extra:
                    self._extra.append(value)


def _drive_error(call: Call[T], resp: Response, operation: str, ctx: Ctx) -> tuple[Any, Any, bool]:
    err = map_error(resp, operation, ctx.known_secrets())
    try:
        nxt = call.throw(err)
    except StopIteration as stop:
        return None, stop.value, True
    return nxt, None, False


def run_sync(ctx: Ctx, call: Call[T], cancel: CancelToken | None = None) -> T:
    """Drive ``call`` over the synchronous transport (``ctx.transport``).

    Error responses are mapped with :func:`map_error` and thrown into the call (which may
    catch them, e.g. to turn a 404 into ``None``).
    """
    transport: HttpTransport = ctx.transport
    try:
        req = next(call)
    except StopIteration as stop:
        return stop.value  # type: ignore[no-any-return]
    while True:
        resp = transport.request(req, cancel=cancel)
        if resp.status >= 400:
            nxt, value, done = _drive_error(call, resp, req.op, ctx)
            if done:
                return value  # type: ignore[no-any-return]
            req = nxt
            continue
        try:
            req = call.send(resp)
        except StopIteration as stop:
            return stop.value  # type: ignore[no-any-return]


async def run_async(ctx: Ctx, call: Call[T]) -> T:
    """Drive ``call`` over the asynchronous transport (see :func:`run_sync`)."""
    transport: AsyncHttpTransport = ctx.transport
    try:
        req = next(call)
    except StopIteration as stop:
        return stop.value  # type: ignore[no-any-return]
    while True:
        resp = await transport.request(req)
        if resp.status >= 400:
            nxt, value, done = _drive_error(call, resp, req.op, ctx)
            if done:
                return value  # type: ignore[no-any-return]
            req = nxt
            continue
        try:
            req = call.send(resp)
        except StopIteration as stop:
            return stop.value  # type: ignore[no-any-return]
