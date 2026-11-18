"""Ingest logs, search them back with SQL, ship records with the batching log client.

Run against a real OpenObserve (``OPENOBSERVE_URL``, ``OPENOBSERVE_USER``,
``OPENOBSERVE_PASSWORD``; the stream is created on first write).
"""

from __future__ import annotations

import logging
import time

import grow_openobserve as sdk

STREAM = "sdk_example"


def main() -> None:
    with sdk.OpenObserveClient.from_env() as client:
        # 1. Raw ingestion: _json (also ingest_multi for NDJSON and ingest_bulk for _bulk).
        now_us = int(time.time() * 1_000_000)
        result = client.logs.ingest_json(
            STREAM,
            [
                {
                    "message": "user signed in",
                    "level": "info",
                    "user": {"id": 7},
                    "_timestamp": now_us,
                },
                {"message": "payment failed", "level": "error", "_timestamp": now_us + 1},
            ],
        )
        print(f"ingested {result.successful} records into {result.stream} ({result.failed} failed)")

        # 2. SQL search (times are datetimes or epoch microseconds; default: last 15 minutes).
        page = client.logs.search(
            f"SELECT * FROM \"{STREAM}\" WHERE level = 'error' ORDER BY _timestamp DESC",
            track_total_hits=True,
        )
        print(
            f"{page.total_hits} error rows, first: {page.hits[0]['message'] if page.hits else None}"
        )
        for field in client.logs.values_of(STREAM, ["level"]):
            print(field.field, [(v.value, v.count) for v in field.values])

        # 3. Batching log client with the Logger facade and the stdlib logging handler.
        log_client = client.log_client(STREAM, resource={"service.name": "example-app"})
        logger = log_client.logger("checkout", {"region": "eu"})
        logger.info("order created", {"order_id": 42, "password": "redacted before sending"})
        logging.getLogger("example").addHandler(sdk.LogHandler(log_client))
        logging.getLogger("example").warning("stdlib logging works too")
        log_client.flush(10)

        # 4. Category query client: full-text text query or raw SQL, normalized LogRecords.
        record = client.log_query(STREAM).wait_for_log("order created", timeout_s=30)
        print(f"found: {record.body} attrs={dict(record.attributes)}")

        # 5. Streams: schema and settings.
        info = client.streams.get_stream_schema(STREAM)
        print(f"{info.name}: {[f.name for f in info.schema]}")
        client.streams.update_stream_settings(
            STREAM, retention_days=7, full_text_search_keys=["message"]
        )


if __name__ == "__main__":
    main()
