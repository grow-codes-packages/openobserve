"""AsyncOpenObserveClient against the real service: every module once, awaited."""

from __future__ import annotations

import asyncio
import time

import pytest
from factories import async_core_client, run_id

import grow_openobserve as sdk


def test_async_modules(env: dict[str, str]) -> None:
    rid = run_id()
    stream = f"sdk_pya_{rid}"
    email = f"sdk-pya-{rid}@stack.local"

    async def run() -> None:
        async with async_core_client(env) as c:
            base = int(time.time() * 1_000_000)
            assert (
                await c.logs.ingest_json(stream, [{"message": f"a {rid}", "_timestamp": base}])
            ).successful == 1
            assert (
                await c.logs.ingest_multi(stream, [{"message": "b", "_timestamp": base + 1}])
            ).successful == 1
            bulk = await c.logs.ingest_bulk(
                [sdk.BulkAction(stream, {"message": "c", "_timestamp": base + 2})]
            )
            assert not bulk.errors
            otlp = await c.logs.ingest_otlp([sdk.LogRecord(body=f"d {rid}")], stream=stream)
            assert otlp.accepted == 1
            result = await c.logs.search(f'SELECT * FROM "{stream}"', track_total_hits=True)
            assert result.total_hits == 4
            around = await c.logs.search_around(stream, base + 1)
            assert len(around.hits) >= 3
            values = await c.logs.values_of(stream, ["message"])
            assert {v.value for v in values[0].values} >= {f"a {rid}", "b", "c"}
            histogram = await c.logs.histogram(stream)
            assert sum(b.count for b in histogram.buckets) == 4

            rows: list[dict] = []
            tail = c.logs.tail(stream, rows.append, interval_s=0.2, start=base + 3)
            await c.logs.ingest_json(stream, [{"message": "tailed"}])
            for _ in range(50):
                if any(r.get("message") == "tailed" for r in rows):
                    break
                await asyncio.sleep(0.2)
            await tail.stop()
            assert any(r.get("message") == "tailed" for r in rows)

            query = c.log_query(stream)
            found = await query.wait_for_log(f"a {rid}", timeout_s=30, interval_s=0.2)
            assert found.body == f"a {rid}"
            assert stream in await query.list_streams()
            assert "message" in await query.list_labels()
            records: list[sdk.LogRecord] = []
            qtail = await query.tail("tailed2", records.append, interval_s=0.2)
            await c.logs.ingest_json(stream, [{"message": "tailed2"}])
            for _ in range(50):
                if records:
                    break
                await asyncio.sleep(0.2)
            await qtail.stop()
            assert [r.body for r in records] == ["tailed2"]

            log_client = c.log_client(stream, resource={"service.name": "py-async"})
            log_client.logger().info(f"async logger {rid}")
            await log_client.flush(30)
            assert (await query.wait_for_log(f"async logger {rid}", timeout_s=30)).resource == {
                "service.name": "py-async"
            }

            info = await c.streams.get_stream_schema(stream)
            assert info.name == stream
            assert stream in [s.name for s in await c.streams.list_streams("logs", keyword=stream)]
            await c.streams.update_stream_settings(stream, retention_days=5)
            assert (await c.streams.get_stream_schema(stream)).settings.retention_days == 5
            created = f"{stream}_new"
            await c.streams.create_stream(created, fields=[sdk.SchemaField("message", "Utf8")])
            await c.streams.delete_stream(created)
            await c.streams.delete_stream(stream)
            with pytest.raises(sdk.NotFoundError):
                await c.streams.delete_stream(stream)

            orgs = await c.admin.list_organizations()
            assert c.config.org in [o.identifier for o in orgs]
            token = await c.admin.create_service_account(email)
            assert email in [a.email for a in await c.admin.list_service_accounts()]
            rotated = await c.admin.rotate_service_account_token(email)
            assert rotated.token.reveal() != token.token.reveal()
            await c.admin.delete_service_account(email)
            assert (await c.admin.get_passcode()).passcode.reveal()

            service = f"py-async-{rid}"
            handle = c.init_tracing(sdk.TracingConfig(service_name=service, set_global=False))
            with handle.tracer("async").start_as_current_span("async-root") as span:
                trace_id = format(span.get_span_context().trace_id, "032x")
            handle.shutdown()
            trace = await c.traces.wait_for_trace(trace_id, timeout_s=30, interval_s=0.2)
            assert [s.name for s in trace.spans] == ["async-root"]
            assert (await c.traces.get_trace(trace_id)) is not None
            summaries = await c.traces.search_traces(sdk.TraceSearchQuery(service=service))
            assert [s.trace_id for s in summaries] == [trace_id]
            assert service in await c.traces.list_services()
            assert await c.traces.list_operations(service) == ["async-root"]
            assert (await c.traces.trace_time_range(trace_id)).found
            with pytest.raises(sdk.UnsupportedError):
                await c.traces.get_dependencies()
            other = c.trace_query(c.traces.stream)
            assert (await other.get_trace(trace_id)) is not None

    asyncio.run(run())
