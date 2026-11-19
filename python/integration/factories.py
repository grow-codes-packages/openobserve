"""Factories the integration suite and the conformance runner use (owned by this repository).

Credentials come from /bootstrap/env: ``OPENOBSERVE_USER``/``OPENOBSERVE_PASSWORD`` (root),
``OPENOBSERVE_STREAM`` (seeded log stream), ``OPENOBSERVE_TRACES_STREAM`` and a service
account (``OPENOBSERVE_SA_EMAIL``/``OPENOBSERVE_SA_TOKEN``). Capabilities left out are
explained in docs/compatibility.md (``dependencies``: enterprise-only service graph).
"""

from __future__ import annotations

import secrets
from collections.abc import Iterable, Mapping
from datetime import timedelta
from typing import Any

import grow_openobserve as sdk
from grow_openobserve.testing import LoggingTarget, ScenarioContext, TracingTarget
from grow_openobserve.testing.logging_conformance import batch_options_for

URL_VAR = "GROW_OPENOBSERVE_URL"
SERVICE_NAME = "grow-sdk-python-conformance"

#: Capabilities of the deployment under test for the core scenarios ("version" = serverVersion).
CORE_CAPABILITIES: set[str] = {"version"}


def run_id() -> str:
    """A short random lowercase id for unique stream/account names."""
    return "".join(secrets.choice("abcdefghijklmnopqrstuvwxyz0123456789") for _ in range(8))


def core_client(env: Mapping[str, str]) -> sdk.OpenObserveClient:
    """A client configured from the test environment (root user from /bootstrap/env)."""
    return sdk.OpenObserveClient.from_env(env)


def async_core_client(env: Mapping[str, str]) -> sdk.AsyncOpenObserveClient:
    return sdk.AsyncOpenObserveClient.from_env(env)


#: Capabilities for the logging scenarios: query, tail, streams, labels.
LOGGING_CAPABILITIES: set[str] = {"query", "tail", "streams", "labels"}


class OwnedLogClient:
    """A log client that also closes the core client it was made from."""

    def __init__(self, client: sdk.OpenObserveClient, log_client: sdk.BatchingLogClient) -> None:
        self._client = client
        self._log_client = log_client

    def send(self, record: sdk.LogRecord | sdk.LogInput) -> None:
        self._log_client.send(record)

    def send_batch(self, records: Iterable[sdk.LogRecord | sdk.LogInput]) -> None:
        self._log_client.send_batch(records)

    def flush(self, timeout_s: float | timedelta | None = None) -> None:
        self._log_client.flush(timeout_s)

    def stats(self) -> sdk.BatchStats:
        return self._log_client.stats()

    def close(self, timeout_s: float | timedelta | None = None) -> None:
        self._log_client.close(timeout_s)
        self._client.close()


def logging_target(env: Mapping[str, str], scenario: Mapping[str, Any]) -> Any | None:
    """A ``LoggingTarget(client, query)`` on the seeded stream with the scenario's batch options."""
    client = core_client(env)
    log_client = client.log_client(
        options=batch_options_for(scenario),
        resource={"service.name": SERVICE_NAME},
    )
    return LoggingTarget(OwnedLogClient(client, log_client), client.log_query())


#: Capabilities for the tracing scenarios: query, services ("dependencies" is enterprise-only).
TRACING_CAPABILITIES: set[str] = {"query", "services"}


def tracing_target(env: Mapping[str, str], ctx: ScenarioContext) -> Any | None:
    """A ``TracingTarget(handle, query)`` exporting to the traces stream from /bootstrap/env."""
    config = sdk.OpenObserveConfig.from_env(env)
    handle = sdk.init_tracing(
        sdk.TracingConfig(
            service_name=ctx.service_name or SERVICE_NAME,
            set_global=False,
            batch=sdk.SpanBatchOptions(scheduled_delay_s=0.2),
        ),
        openobserve=config,
    )
    client = core_client(env)
    return TracingTarget(handle, client.trace_query())
