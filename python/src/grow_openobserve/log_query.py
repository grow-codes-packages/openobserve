"""The logging category on OpenObserve: log exporters for ``BatchingLogClient`` and the
:class:`OpenObserveLogQuery` log query client (DECISIONS 2 and 3).
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import Callable, Sequence
from datetime import datetime, timedelta

from ._core import NotFoundError, ValidationError, to_seconds
from ._core.health import apoll_until, poll_until
from ._http import Timestamp, sql_ident, sql_string, stream_name
from ._logging import (
    AsyncBaseLogQueryClient,
    AsyncPollingTail,
    AsyncTailHandle,
    BaseLogQueryClient,
    ExportResult,
    LogRecord,
    LogSearchResult,
    PollingTail,
    SearchOrder,
    TailHandle,
)
from .logs import AsyncOpenObserveLogs, OpenObserveLogs, SearchResult
from .records import log_record_to_row, row_to_log_record
from .streams import AsyncOpenObserveStreams, OpenObserveStreams

__all__ = [
    "AsyncOpenObserveLogExporter",
    "AsyncOpenObserveLogQuery",
    "AsyncOpenObserveOtlpLogExporter",
    "OpenObserveLogExporter",
    "OpenObserveLogQuery",
    "OpenObserveOtlpLogExporter",
    "is_sql_query",
    "text_query_sql",
]

_SQL = re.compile(r"^\s*select\s", re.IGNORECASE)
RecordPredicate = Callable[[LogRecord], bool]


class _AsyncSqlTail(AsyncPollingTail):
    """:class:`AsyncPollingTail` whose poll loop lets the SDK's ``CancelledError`` (an
    ``Exception`` *and* an ``asyncio.CancelledError``) end the task, so ``stop()`` never hangs."""

    async def _run(self) -> None:
        while True:
            try:
                await self.poll_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - reported to on_error, polling continues
                if self._on_error is not None:
                    self._on_error(exc)
            await asyncio.sleep(self._interval)


def is_sql_query(query: str) -> bool:
    """True when ``query`` is raw SQL (starts with ``SELECT``, case-insensitive)."""
    return bool(_SQL.match(query))


def text_query_sql(stream: str, text: str, order: SearchOrder) -> str:
    """``SELECT * FROM "<stream>" WHERE match_all('<text>') ORDER BY _timestamp <order>``;
    an empty text matches every row."""
    where = "" if not text.strip() else f" WHERE match_all({sql_string(text)})"
    direction = "ASC" if order == "asc" else "DESC"
    return f"SELECT * FROM {sql_ident(stream)}{where} ORDER BY _timestamp {direction}"


def _rejected(stream: str, count: int, records: int, detail: str | None) -> ExportResult:
    rejected = min(records, max(0, count))
    error = None
    if rejected > 0:
        suffix = f" ({detail[:300]})" if detail else ""
        error = ValidationError(f"logging.export {stream}: {rejected} records rejected{suffix}")
    return ExportResult(accepted=records - rejected, rejected=rejected, error=error)


# --------------------------------------------------------------------------- exporters


class OpenObserveLogExporter:
    """:class:`LogExporter` that writes records with ``_json`` ingestion into one stream
    (per-stream counters give exact accepted/rejected numbers)."""

    def __init__(self, logs: OpenObserveLogs, stream: str) -> None:
        self._logs = logs
        #: Target stream.
        self.stream = stream_name(stream, "", "logging.export")

    def export(self, records: Sequence[LogRecord], *, gzip: bool, timeout_s: float) -> ExportResult:
        if not records:
            return ExportResult(0, 0)
        result = self._logs.ingest_json(
            self.stream, [log_record_to_row(r) for r in records], gzip=gzip
        )
        return _rejected(self.stream, result.failed, len(records), result.error)

    def shutdown(self) -> None:
        return None


class AsyncOpenObserveLogExporter:
    """Async :class:`OpenObserveLogExporter` (an :class:`AsyncLogExporter`)."""

    def __init__(self, logs: AsyncOpenObserveLogs, stream: str) -> None:
        self._logs = logs
        self.stream = stream_name(stream, "", "logging.export")

    async def export(
        self, records: Sequence[LogRecord], *, gzip: bool, timeout_s: float
    ) -> ExportResult:
        if not records:
            return ExportResult(0, 0)
        result = await self._logs.ingest_json(
            self.stream, [log_record_to_row(r) for r in records], gzip=gzip
        )
        return _rejected(self.stream, result.failed, len(records), result.error)

    async def shutdown(self) -> None:
        return None


class OpenObserveOtlpLogExporter:
    """:class:`LogExporter` that writes records as OTLP/JSON to ``/api/{org}/v1/logs`` with
    the ``stream-name`` header."""

    def __init__(self, logs: OpenObserveLogs, stream: str) -> None:
        self._logs = logs
        self.stream = stream_name(stream, "", "logging.export")

    def export(self, records: Sequence[LogRecord], *, gzip: bool, timeout_s: float) -> ExportResult:
        result = self._logs.ingest_otlp(records, stream=self.stream, gzip=gzip)
        return _rejected(self.stream, result.rejected, len(records), None)

    def shutdown(self) -> None:
        return None


class AsyncOpenObserveOtlpLogExporter:
    """Async :class:`OpenObserveOtlpLogExporter`."""

    def __init__(self, logs: AsyncOpenObserveLogs, stream: str) -> None:
        self._logs = logs
        self.stream = stream_name(stream, "", "logging.export")

    async def export(
        self, records: Sequence[LogRecord], *, gzip: bool, timeout_s: float
    ) -> ExportResult:
        result = await self._logs.ingest_otlp(records, stream=self.stream, gzip=gzip)
        return _rejected(self.stream, result.rejected, len(records), None)

    async def shutdown(self) -> None:
        return None


# --------------------------------------------------------------------------- query


def _offset(cursor: str | None) -> int:
    if cursor is None:
        return 0
    try:
        value = int(cursor)
    except ValueError:
        value = -1
    if value < 0:
        raise ValidationError("logging.search: invalid cursor")
    return value


def _check_limit(limit: int) -> int:
    if int(limit) <= 0:
        raise ValidationError("logging.search: limit must be positive")
    return int(limit)


def _page(result: SearchResult, offset: int, limit: int) -> LogSearchResult:
    return LogSearchResult(
        records=[row_to_log_record(h) for h in result.hits],
        next_cursor=str(offset + len(result.hits)) if len(result.hits) >= limit else None,
        total=result.total_hits,
    )


def _first(result: LogSearchResult, predicate: RecordPredicate | None) -> LogRecord | None:
    for record in result.records:
        if predicate is None or predicate(record):
            return record
    return None


class OpenObserveLogQuery(BaseLogQueryClient):
    """:class:`LogQueryClient` on one stream (default: the config's ``stream``).

    A query that starts with ``SELECT`` is raw SQL; any other text becomes
    ``match_all('<text>')`` (full-text search over the stream's full-text keys,
    case-insensitive) on :attr:`stream`. The cursor is a row offset. Use
    ``client.log_query(stream)``.
    """

    def __init__(self, logs: OpenObserveLogs, streams: OpenObserveStreams, stream: str) -> None:
        self._logs = logs
        self._streams = streams
        #: The stream queried by text queries, ``list_labels`` and ``tail``.
        self.stream = stream_name(stream, "", "logging.query")

    def search(
        self,
        query: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 100,
        order: SearchOrder = "desc",
        cursor: str | None = None,
        track_total_hits: bool = False,
    ) -> LogSearchResult:
        """One page of records; default window: the last 15 minutes, newest first, 100 rows.

        ``track_total_hits`` fills ``total`` with the exact number of matches (one extra
        request; DECISIONS 3).
        """
        size = _check_limit(limit)
        offset = _offset(cursor)
        sql = query if is_sql_query(query) else text_query_sql(self.stream, query, order)
        result = self._logs.search(
            sql, start=start, end=end, from_=offset, size=size, track_total_hits=track_total_hits
        )
        return _page(result, offset, size)

    def tail(
        self,
        query: str,
        on_record: Callable[[LogRecord], None],
        *,
        interval_s: float | timedelta = 1.0,
        start: Timestamp | None = None,
        on_error: Callable[[Exception], None] | None = None,
    ) -> TailHandle:
        """Follow new records. Text queries use the cursor-based ``logs.tail`` on
        :attr:`stream`; raw SQL uses the generic polling tail on top of :meth:`search`."""
        if is_sql_query(query):
            begin = start if isinstance(start, datetime) else None
            return PollingTail(
                self, query, on_record, interval_s=interval_s, start=begin, on_error=on_error
            )
        return self._logs.tail(
            self.stream,
            lambda row: on_record(row_to_log_record(row)),
            filter=f"match_all({sql_string(query)})" if query.strip() else None,
            start=start,
            interval_s=interval_s,
            on_error=on_error,
        )

    def list_streams(self) -> list[str]:
        """Names of the organization's log streams."""
        return [s.name for s in self._streams.list_streams("logs")]

    def list_labels(self) -> list[str]:
        """Field names of :attr:`stream` (from its schema)."""
        return [f.name for f in self._streams.get_stream_schema(self.stream).schema]

    def wait_for_log(
        self,
        query: str,
        *,
        timeout_s: float | timedelta = 30.0,
        interval_s: float | timedelta = 1.0,
        predicate: RecordPredicate | None = None,
    ) -> LogRecord:
        """Poll :meth:`search` until a record (matching ``predicate``) shows up; a stream that
        does not exist yet counts as "no record yet". :class:`TimeoutError` otherwise."""

        def attempt() -> LogRecord | None:
            try:
                return _first(self.search(query), predicate)
            except NotFoundError:
                return None

        return poll_until(
            attempt,
            timeout_s=to_seconds(timeout_s),
            interval_s=to_seconds(interval_s),
            operation="logs.wait_for_log",
        )


class AsyncOpenObserveLogQuery(AsyncBaseLogQueryClient):
    """Async :class:`OpenObserveLogQuery`; same methods, awaited."""

    def __init__(
        self, logs: AsyncOpenObserveLogs, streams: AsyncOpenObserveStreams, stream: str
    ) -> None:
        self._logs = logs
        self._streams = streams
        self.stream = stream_name(stream, "", "logging.query")

    async def search(
        self,
        query: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 100,
        order: SearchOrder = "desc",
        cursor: str | None = None,
        track_total_hits: bool = False,
    ) -> LogSearchResult:
        """See :meth:`OpenObserveLogQuery.search`."""
        size = _check_limit(limit)
        offset = _offset(cursor)
        sql = query if is_sql_query(query) else text_query_sql(self.stream, query, order)
        result = await self._logs.search(
            sql, start=start, end=end, from_=offset, size=size, track_total_hits=track_total_hits
        )
        return _page(result, offset, size)

    async def tail(
        self,
        query: str,
        on_record: Callable[[LogRecord], None],
        *,
        interval_s: float | timedelta = 1.0,
        start: Timestamp | None = None,
        on_error: Callable[[Exception], None] | None = None,
    ) -> AsyncTailHandle:
        """See :meth:`OpenObserveLogQuery.tail`."""
        if is_sql_query(query):
            begin = start if isinstance(start, datetime) else None
            return _AsyncSqlTail(
                self, query, on_record, interval_s=interval_s, start=begin, on_error=on_error
            )
        return self._logs.tail(
            self.stream,
            lambda row: on_record(row_to_log_record(row)),
            filter=f"match_all({sql_string(query)})" if query.strip() else None,
            start=start,
            interval_s=interval_s,
            on_error=on_error,
        )

    async def list_streams(self) -> list[str]:
        """See :meth:`OpenObserveLogQuery.list_streams`."""
        return [s.name for s in await self._streams.list_streams("logs")]

    async def list_labels(self) -> list[str]:
        """See :meth:`OpenObserveLogQuery.list_labels`."""
        return [f.name for f in (await self._streams.get_stream_schema(self.stream)).schema]

    async def wait_for_log(
        self,
        query: str,
        *,
        timeout_s: float | timedelta = 30.0,
        interval_s: float | timedelta = 1.0,
        predicate: RecordPredicate | None = None,
    ) -> LogRecord:
        """See :meth:`OpenObserveLogQuery.wait_for_log`."""

        async def attempt() -> LogRecord | None:
            try:
                return _first(await self.search(query), predicate)
            except NotFoundError:
                return None

        return await apoll_until(
            attempt,
            timeout_s=to_seconds(timeout_s),
            interval_s=to_seconds(interval_s),
            operation="logs.wait_for_log",
        )
