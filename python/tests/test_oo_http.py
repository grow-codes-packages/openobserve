"""Internal helpers: error mapping, time conversion, the sans-IO drivers."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import httpx
import pytest

import grow_openobserve as sdk
from grow_openobserve import _http
from grow_openobserve._core import Response


def resp(status: int, body: bytes, ctype: str | None = "application/json", **hdrs: str) -> Response:
    headers = httpx.Headers({**({"content-type": ctype} if ctype else {}), **hdrs})
    return Response(status, headers, body)


def test_map_error_stream_not_found_uses_trace_id() -> None:
    err = _http.map_error(
        resp(400, b'{"code":20002,"message":"Search stream not found: x","trace_id":"abc123"}'),
        "logs.search",
        [],
    )
    assert isinstance(err, sdk.NotFoundError)
    assert err.http_status == 400
    assert err.request_id == "abc123"
    assert err.message == "logs.search: Search stream not found: x (HTTP 400)"


def test_map_error_variants() -> None:
    conflict = _http.map_error(resp(400, b'{"code":400,"message":"User already exists"}'), "op", [])
    assert isinstance(conflict, sdk.ConflictError)
    plain = _http.map_error(resp(401, b"Unauthorized Access", None), "ping", [])
    assert isinstance(plain, sdk.AuthenticationError)
    assert "Unauthorized Access" in plain.message
    text = _http.map_error(
        resp(422, b"Failed to deserialize", "text/plain; charset=utf-8"), "op", []
    )
    assert isinstance(text, sdk.ValidationError)
    proto = _http.map_error(
        resp(400, b"\x08\x03\x12\x0dInvalid proto", "application/x-protobuf"), "otlp", []
    )
    assert isinstance(proto, sdk.ValidationError)
    assert "Invalid proto" in proto.message
    broken = _http.map_error(resp(400, b"\x08\x03\x12\x7f", "application/x-protobuf"), "otlp", [])
    assert "bad request" in broken.message
    odd_wire = _http.map_error(resp(400, b"\x0d\x00", "application/x-protobuf"), "otlp", [])
    assert "bad request" in odd_wire.message
    no_message = _http.map_error(resp(400, b"\x08\x03", "application/x-protobuf"), "otlp", [])
    assert "bad request" in no_message.message
    limited = _http.map_error(resp(429, b"", None, **{"retry-after": "3"}), "op", [])
    assert isinstance(limited, sdk.RateLimitError) and limited.retry_after == 3.0
    secret = _http.map_error(
        resp(400, b'{"message":"bad token s3cr3t-value"}'), "op", ["s3cr3t-value"]
    )
    assert "s3cr3t-value" not in secret.message
    same = _http.map_error(resp(500, b'{"message":"boom"}'), "op", [])
    assert isinstance(same, sdk.UnavailableError)


def test_time_helpers() -> None:
    aware = datetime(2026, 1, 1, tzinfo=timezone.utc)
    micros = _http.to_micros(aware)
    assert _http.to_micros(aware.replace(tzinfo=None)) == micros
    assert _http.to_micros(micros) == micros
    assert _http.micros_to_dt(micros) == aware
    with pytest.raises(sdk.ValidationError):
        _http.to_micros(True)
    with pytest.raises(sdk.ValidationError):
        _http.to_micros("2026")  # type: ignore[arg-type]
    start, end = _http.window(None, micros, timedelta(minutes=15), "op")
    assert end - start == 15 * 60 * 1_000_000
    with pytest.raises(sdk.ValidationError):
        _http.window(micros + 1, micros, timedelta(minutes=1), "op")


def test_value_helpers() -> None:
    assert _http.as_int(True) == 1
    assert _http.as_int(2.9) == 2
    assert _http.as_int(" 7 ") == 7
    assert _http.as_int("1.5") == 1
    assert _http.as_int("x", 9) == 9
    assert _http.as_int(None, 4) == 4
    assert _http.as_str("") is None
    assert _http.sql_string("it's") == "'it''s'"
    assert _http.sql_ident('a"b') == '"a""b"'
    with pytest.raises(sdk.ValidationError):
        _http.stream_name(" ", "", "op")
    with pytest.raises(sdk.ValidationError):
        _http.stream_type("bogus", "op")
    assert _http.json_body(Response(200, httpx.Headers(), b"")) is None


def test_ctx_secrets() -> None:
    ctx = _http.Ctx(None, sdk.OpenObserveConfig())
    ctx.add_secret("abc")  # too short: ignored
    ctx.add_secret("long-secret")
    ctx.add_secret("long-secret")
    ctx.add_secret(None)
    assert ctx.known_secrets() == ["long-secret"]


def test_calls_without_requests_return_directly() -> None:
    def call() -> _http.Call[int]:
        return 5
        yield  # pragma: no cover

    ctx = _http.Ctx(None, sdk.OpenObserveConfig())
    assert _http.run_sync(ctx, call()) == 5

    import asyncio

    assert asyncio.run(_http.run_async(ctx, call())) == 5
