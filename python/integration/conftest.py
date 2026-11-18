"""Integration fixtures: environment from /bootstrap/env and the service URL (SPEC section 8)."""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import pytest
from factories import URL_VAR, core_client, run_id

import grow_openobserve as sdk


def load_env() -> dict[str, str]:
    """Process environment overlaid with the KEY=value lines of the bootstrap file."""
    env = dict(os.environ)
    path = Path(env.get("GROW_BOOTSTRAP_ENV", "/bootstrap/env"))
    if path.is_file():
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                env.setdefault(key.strip(), value.strip())
    return env


@pytest.fixture(scope="session")
def env() -> dict[str, str]:
    values = load_env()
    if not values.get(URL_VAR):
        pytest.skip(f"{URL_VAR} is not set (run through docker/compose.yml)")
    return values


@pytest.fixture
def client(env: dict[str, str]) -> Iterator[sdk.OpenObserveClient]:
    with core_client(env) as c:
        yield c


@pytest.fixture
def rid() -> str:
    """A unique id for this test (stream names, accounts, needles)."""
    return run_id()


@pytest.fixture
def stream(client: sdk.OpenObserveClient, rid: str) -> Iterator[str]:
    """A fresh log stream name; the stream is deleted after the test when it exists."""
    name = f"sdk_py_{rid}"
    yield name
    try:
        client.streams.delete_stream(name)
    except sdk.NotFoundError:
        pass
