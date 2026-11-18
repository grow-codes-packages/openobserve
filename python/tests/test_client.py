"""OpenObserve client (sync + async): health, ping, version, close and polling helpers."""

from __future__ import annotations

import asyncio
import base64
import time
from typing import Any

import pytest

import grow_openobserve as sdk
from _support import MockServer, Reply, closed_port_url, make_config

PASSWORD = "client-pass-789"
USER = "root@example.com"
CONFIG = "/api/default/config"


def client(url: str, **kw: Any) -> sdk.OpenObserveClient:
    return sdk.OpenObserveClient(make_config(url, **kw))


def aclient(url: str, **kw: Any) -> sdk.AsyncOpenObserveClient:
    return sdk.AsyncOpenObserveClient(make_config(url, **kw))


# --------------------------------------------------------------------------- sync


def test_health_ping_version(server: MockServer) -> None:
    server.enqueue("/healthz", Reply(200, {"status": "ok", "build": 7, "nested": {"x": 1}}))
    server.enqueue(CONFIG, Reply(200, {"version": "1.2.3"}), Reply(200, {"version": "1.2.3"}))
    with client(server.url, user=USER, password=PASSWORD) as c:
        health = c.health()
        assert health == sdk.HealthStatus(True, "ok", {"build": "7"})
        latency = c.ping()
        assert isinstance(latency, float) and latency >= 0
        assert c.server_version() == "1.2.3"
        assert PASSWORD not in repr(c)
    ping = next(s for s in server.seen if s.path == CONFIG)
    expected = "Basic " + base64.b64encode(f"{USER}:{PASSWORD}".encode()).decode()
    assert ping.headers["Authorization"] == expected
    health_req = next(s for s in server.seen if s.path == "/healthz")
    assert "Authorization" not in health_req.headers


def test_health_variants(server: MockServer) -> None:
    c = client(server.url)
    server.enqueue("/healthz", Reply(503, {"status": "degraded"}))
    assert c.health() == sdk.HealthStatus(False, "degraded", {"httpStatus": "503"})
    server.enqueue("/healthz", Reply(500, "boom"))
    assert c.health() == sdk.HealthStatus(False, "unhealthy", {"httpStatus": "500"})
    server.enqueue("/healthz", Reply(200, "OK"))
    assert c.health() == sdk.HealthStatus(True, "ok", {})
    server.enqueue("/healthz", Reply(200, [1, 2]))
    assert c.health().healthy
    assert len([s for s in server.seen if s.path == "/healthz"]) == 4  # health never retries


def test_core_health_unreachable() -> None:
    """core.health.unreachable"""
    c = client(closed_port_url())
    status = c.health()
    assert status.healthy is False
    assert status.status == "unreachable"
    assert "error" in status.details


def test_health_timeout_is_unreachable(server: MockServer) -> None:
    server.enqueue("/healthz", Reply(200, delay=1.0))
    c = client(server.url, request_timeout=0.1)
    assert c.health().status == "unreachable"


def test_health_raises_only_cancelled_and_config(server: MockServer) -> None:
    c = client(server.url)
    c.close()
    with pytest.raises(sdk.CancelledError):
        c.health()
    token = sdk.CancelToken()
    token.cancel()
    with pytest.raises(sdk.CancelledError):
        client(server.url).health(cancel=token)
    bad = sdk.OpenObserveClient.__new__(sdk.OpenObserveClient)
    bad.config = sdk.OpenObserveConfig(url="ftp://svc")
    bad._transport = sdk.HttpTransport(bad.config)
    with pytest.raises(sdk.ConfigError):
        bad.health()


def test_ping_bad_credentials(server: MockServer) -> None:
    server.enqueue(CONFIG, Reply(401, b"Unauthorized Access", headers={"Content-Type": ""}))
    with pytest.raises(sdk.AuthenticationError) as info:
        client(server.url, user=USER, password=PASSWORD).ping()
    assert info.value.http_status == 401
    assert PASSWORD not in str(info.value)


def test_server_version_errors(server: MockServer) -> None:
    c = client(server.url)
    server.enqueue(CONFIG, Reply(404, "Organization not found"))
    with pytest.raises(sdk.NotFoundError) as info:
        c.server_version()
    assert info.value.http_status == 404
    assert "Organization not found" in str(info.value)
    server.enqueue(CONFIG, Reply(200, {"nope": 1}))
    with pytest.raises(sdk.UnsupportedError):
        c.server_version()
    server.enqueue(CONFIG, Reply(403))
    with pytest.raises(sdk.PermissionDeniedError):
        c.server_version()


def test_core_close_idempotent(server: MockServer) -> None:
    """core.close.idempotent"""
    c = client(server.url)
    stopped: list[int] = []

    class Res:
        def stop(self) -> None:
            stopped.append(1)

    c.resources.register(Res())
    c.close()
    c.close()
    assert stopped == [1]
    c.resources.register(Res())  # registering after close stops immediately
    assert stopped == [1, 1]


def test_from_env_and_raw_request(server: MockServer, monkeypatch: pytest.MonkeyPatch) -> None:
    env = {"OPENOBSERVE_URL": server.url, "OPENOBSERVE_USER": USER, "OPENOBSERVE_TOKEN": "tok-1"}
    with sdk.OpenObserveClient.from_env(env) as c:
        assert c.config.url == server.url
        resp = c.request(sdk.Request("GET", "/anything"))
        assert resp.status == 200
    monkeypatch.setenv(sdk.ENV_PREFIX + "URL", server.url)
    with sdk.OpenObserveClient() as c2:
        assert c2.config.url == server.url
    with pytest.raises(sdk.ConfigError):
        sdk.OpenObserveClient.from_env({sdk.ENV_PREFIX + "URL": "nope://x"})


# --------------------------------------------------------------------------- async


async def test_async_client(server: MockServer, monkeypatch: pytest.MonkeyPatch) -> None:
    server.enqueue("/healthz", Reply(200, {"status": "ok"}))
    server.enqueue(CONFIG, Reply(200, {"version": "9.9.9"}), Reply(200, {"version": "9.9.9"}))
    async with aclient(server.url, user=USER, password=PASSWORD) as c:
        assert (await c.health()).healthy
        assert await c.ping() >= 0
        assert await c.server_version() == "9.9.9"
        assert (await c.request(sdk.Request("GET", "/raw"))).status == 200
        assert PASSWORD not in repr(c)
        assert c.resources is not None
    await c.close()
    with pytest.raises(sdk.CancelledError):
        await c.health()
    env = {sdk.ENV_PREFIX + "URL": server.url}
    c2 = sdk.AsyncOpenObserveClient.from_env(env)
    server.enqueue(CONFIG, Reply(501))
    with pytest.raises(sdk.UnsupportedError):
        await c2.server_version()
    server.enqueue(CONFIG, Reply(404))
    with pytest.raises(sdk.NotFoundError):
        await c2.server_version()
    await c2.aclose()
    monkeypatch.setenv(sdk.ENV_PREFIX + "URL", server.url)
    c3 = sdk.AsyncOpenObserveClient()
    assert c3.config.url == server.url
    await c3.aclose()


async def test_async_health_unreachable() -> None:
    """core.health.unreachable (async)"""
    c = aclient(closed_port_url())
    status = await c.health()
    assert (status.healthy, status.status) == (False, "unreachable")
    await c.aclose()


async def test_async_health_cancel(server: MockServer) -> None:
    server.enqueue("/healthz", Reply(200, delay=2.0))
    c = aclient(server.url)
    task = asyncio.ensure_future(c.health())
    await asyncio.sleep(0.1)
    task.cancel()
    with pytest.raises(sdk.CancelledError):
        await task
    await c.aclose()


# --------------------------------------------------------------------------- polling


def test_poll_until() -> None:
    values = iter([None, [], [1]])
    assert sdk.poll_until(lambda: next(values), interval_s=0.001) == [1]
    attempts = iter([0, 1, 2, 3])
    assert sdk.poll_until(lambda: next(attempts), predicate=lambda v: v >= 2, interval_s=0) == 2
    errors = iter([sdk.UnavailableError("flaky"), "ok"])

    def flaky() -> str:
        item = next(errors)
        if isinstance(item, Exception):
            raise item
        return item

    assert sdk.poll_until(flaky, interval_s=0) == "ok"
    with pytest.raises(sdk.NotFoundError):
        sdk.poll_until(lambda: (_ for _ in ()).throw(sdk.NotFoundError("gone")))
    started = time.monotonic()
    with pytest.raises(sdk.TimeoutError, match=r"wait\.thing") as info:
        sdk.poll_until(
            lambda: (_ for _ in ()).throw(sdk.UnavailableError("down")),
            timeout_s=0.05,
            interval_s=0.01,
            operation="wait.thing",
        )
    assert isinstance(info.value.cause, sdk.UnavailableError)
    assert time.monotonic() - started < 1.0
    with pytest.raises(sdk.TimeoutError):
        sdk.poll_until(lambda: None, timeout_s=0.02, interval_s=0.005)


async def test_apoll_until() -> None:
    values = iter([None, "", "done"])

    async def fetch() -> str | None:
        return next(values)

    assert await sdk.apoll_until(fetch, interval_s=0.001) == "done"

    async def failing() -> str:
        raise sdk.UnavailableError("down")

    with pytest.raises(sdk.TimeoutError):
        await sdk.apoll_until(failing, timeout_s=0.03, interval_s=0.01)

    async def fatal() -> str:
        raise sdk.AuthenticationError("no")

    with pytest.raises(sdk.AuthenticationError):
        await sdk.apoll_until(fatal)

    async def numbers() -> int:
        return 5

    assert await sdk.apoll_until(numbers, predicate=lambda n: n == 5) == 5


def test_resource_registry() -> None:
    reg = sdk.ResourceRegistry()
    calls: list[str] = []

    class Good:
        def stop(self) -> None:
            calls.append("good")

    class Bad:
        def stop(self) -> None:
            raise RuntimeError("ignored")

    good, bad = Good(), Bad()
    reg.register(bad)
    reg.register(good)
    reg.unregister(good)
    reg.unregister(good)
    reg.register(good)
    assert isinstance(good, sdk.Stoppable)
    assert not reg.closed
    reg.stop_all()
    reg.stop_all()
    assert reg.closed and calls == ["good"]
    reg.register(bad)  # stopped immediately, error ignored


def test_get_logger_is_silent() -> None:
    logger = sdk.get_logger()
    assert logger.name == sdk.LOGGER_NAME == "grow_openobserve"
    assert any(type(h).__name__ == "NullHandler" for h in logger.handlers)
