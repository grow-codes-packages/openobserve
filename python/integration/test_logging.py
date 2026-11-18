"""The logging category on OpenObserve: log client, Logger, adapters, log query and the
logging conformance suite."""

from __future__ import annotations

import logging
import threading
from typing import Any

import pytest
from factories import LOGGING_CAPABILITIES, logging_target

import grow_openobserve as sdk
from grow_openobserve.testing import ConformanceRunner, assert_scenario, pytest_params


def test_log_client_logger_and_handler(
    client: sdk.OpenObserveClient, stream: str, rid: str
) -> None:
    log_client = client.log_client(stream, resource={"service.name": "py-logs", "host.name": "h1"})
    logger = log_client.logger("orders", {"run": rid})
    logger.info(f"order created {rid}", {"order_id": 42, "password": "hunter2"})
    logger.child({"step": "pay"}).error(f"payment failed {rid}")
    py_logger = logging.getLogger(f"integration.{rid}")
    py_logger.propagate = False
    py_logger.addHandler(sdk.LogHandler(log_client))
    py_logger.warning("stdlib %s", rid)
    log_client.flush(30)
    assert log_client.stats().sent == 3

    query = client.log_query(stream)
    record = query.wait_for_log(f"order created {rid}", timeout_s=30, interval_s=0.25)
    assert record.severity == sdk.Severity.INFO
    assert record.attributes["order_id"] == 42
    assert record.attributes["password"] == "[REDACTED]"
    assert record.resource["service.name"] == "py-logs"
    assert record.resource["host.name"] == "h1"
    error = query.wait_for_log(
        rid, predicate=lambda r: r.severity == sdk.Severity.ERROR, timeout_s=30
    )
    assert error.body == f"payment failed {rid}"
    assert error.attributes["step"] == "pay"
    warn = query.wait_for_log(f"stdlib {rid}", timeout_s=30)
    assert warn.severity == sdk.Severity.WARN

    page = query.search(rid, limit=2, order="asc", track_total_hits=True)
    assert len(page.records) == 2
    assert page.total == 3
    assert page.next_cursor == "2"
    rest = query.search(rid, limit=2, order="asc", cursor=page.next_cursor)
    assert len(rest.records) == 1
    assert rest.next_cursor is None
    sql = query.search(f"SELECT * FROM \"{stream}\" WHERE severity = 'ERROR'")
    assert [r.body for r in sql.records] == [f"payment failed {rid}"]

    assert stream in query.list_streams()
    assert {"body", "severity", "run"} <= set(query.list_labels())
    log_client.close()
    log_client.close()


def test_otlp_log_client_flushed_by_close(
    client: sdk.OpenObserveClient, env: dict[str, str], stream: str, rid: str
) -> None:
    owner = sdk.OpenObserveClient.from_env(env)
    log_client = owner.log_client(stream, protocol="otlp", resource={"service.name": "py-otlp"})
    log_client.send(sdk.LogInput(message=f"otlp client {rid}", severity=sdk.Severity.DEBUG))
    owner.close()
    assert log_client.stats().sent == 1
    record = client.log_query(stream).wait_for_log(f"otlp client {rid}", timeout_s=30)
    assert record.severity == sdk.Severity.DEBUG
    assert record.resource["service.name"] == "py-otlp"


def test_log_query_tail(client: sdk.OpenObserveClient, stream: str, rid: str) -> None:
    client.logs.ingest_json(stream, [{"message": "seed"}])
    query = client.log_query(stream)
    got: list[sdk.LogRecord] = []
    done = threading.Event()

    def on_record(record: sdk.LogRecord) -> None:
        got.append(record)
        done.set()

    handle = query.tail(rid, on_record, interval_s=0.2)
    try:
        client.logs.ingest_json(stream, [{"message": f"tailed {rid}"}, {"message": "other"}])
        assert done.wait(15)
    finally:
        handle.stop()
    assert [r.body for r in got] == [f"tailed {rid}"]


def test_wait_for_log_times_out(client: sdk.OpenObserveClient, rid: str) -> None:
    with pytest.raises(sdk.TimeoutError):
        client.log_query(f"never_{rid}").wait_for_log("nothing", timeout_s=1, interval_s=0.2)


@pytest.mark.parametrize("scenario", pytest_params(categories=["logging"]))
def test_logging_conformance(env: dict[str, str], scenario: dict[str, Any]) -> None:
    runner = ConformanceRunner(
        {"logging": lambda ctx: logging_target(env, ctx.scenario)},
        capabilities=LOGGING_CAPABILITIES,
    )
    assert_scenario(runner, scenario)
