"""Core round trips against the real service plus the core conformance scenarios."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from factories import CORE_CAPABILITIES, async_core_client, core_client

import grow_openobserve as sdk
from grow_openobserve.testing import ConformanceRunner, assert_scenario, pytest_params


def test_health(client: sdk.OpenObserveClient) -> None:
    status = client.health()
    assert status.healthy, status
    assert status.status == "ok"


def test_ping(client: sdk.OpenObserveClient) -> None:
    assert client.ping() >= 0


def test_server_version(client: sdk.OpenObserveClient, env: dict[str, str]) -> None:
    version = client.server_version()
    assert version.startswith("v")
    expected = env.get("OPENOBSERVE_SERVER_VERSION")
    if expected:
        assert version == expected


def test_close_is_idempotent(env: dict[str, str]) -> None:
    c = core_client(env)
    assert c.ping() >= 0
    c.close()
    c.close()
    with pytest.raises(sdk.CancelledError):
        c.ping()


def test_async_client(env: dict[str, str]) -> None:
    async def run() -> None:
        async with async_core_client(env) as c:
            assert (await c.health()).healthy
            assert await c.ping() >= 0
            assert (await c.server_version()).startswith("v")

    asyncio.run(run())


def test_bad_password_is_rejected(env: dict[str, str]) -> None:
    with core_client({**env, "GROW_OPENOBSERVE_PASSWORD": "definitely-wrong-pass"}) as c:
        with pytest.raises(sdk.AuthenticationError) as info:
            c.ping()
        assert info.value.http_status == 401
        assert "definitely-wrong-pass" not in str(info.value)
        assert "definitely-wrong-pass" not in repr(c.config)


def test_service_account_token_auth(env: dict[str, str]) -> None:
    email, token = env.get("OPENOBSERVE_SA_EMAIL"), env.get("OPENOBSERVE_SA_TOKEN")
    if not email or not token:
        pytest.skip("no service account in /bootstrap/env")
    with sdk.OpenObserveClient.from_env(env, user=email, token=token) as c:
        assert c.config.password is None
        assert c.ping() >= 0


def test_unreachable_health() -> None:
    with sdk.OpenObserveClient(
        sdk.OpenObserveConfig(url="http://127.0.0.1:1", total_timeout=2.0)
    ) as c:
        status = c.health()
    assert not status.healthy
    assert status.status == "unreachable"


@pytest.mark.parametrize("scenario", pytest_params(categories=["core"]))
def test_core_conformance(env: dict[str, str], scenario: dict[str, Any]) -> None:
    runner = ConformanceRunner(
        {"core": lambda ctx: core_client(env)}, capabilities=CORE_CAPABILITIES
    )
    assert_scenario(runner, scenario)
