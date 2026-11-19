"""Grow Codes SDK for OpenObserve.

>>> from grow_openobserve import OpenObserveClient, OpenObserveConfig
>>> client = OpenObserveClient.from_env()  # doctest: +SKIP
>>> client.logs.ingest_json("app", [{"message": "hello"}])  # doctest: +SKIP
"""

from . import _core
from ._core import *  # noqa: F403
from ._version import __version__
from .admin import (
    AsyncOpenObserveAdmin,
    OpenObserveAdmin,
    Organization,
    Passcode,
    ServiceAccount,
    ServiceAccountToken,
)
from .client import AsyncOpenObserveClient, OpenObserveClient
from .config import (
    DEFAULT_ORG,
    DEFAULT_STREAM,
    DEFAULT_URL,
    OpenObserveConfig,
    normalize_stream_name,
)
from .log_query import (
    AsyncOpenObserveLogExporter,
    AsyncOpenObserveLogQuery,
    AsyncOpenObserveOtlpLogExporter,
    OpenObserveLogExporter,
    OpenObserveLogQuery,
    OpenObserveOtlpLogExporter,
    is_sql_query,
    text_query_sql,
)
from .logs import (
    DEFAULT_SEARCH_WINDOW,
    AsyncOpenObserveLogs,
    AsyncRowTail,
    BulkAction,
    BulkItem,
    BulkItemError,
    BulkResult,
    FieldValue,
    FieldValues,
    Histogram,
    HistogramBucket,
    IngestResult,
    OpenObserveLogs,
    OtlpIngestResult,
    RowTail,
    SearchResult,
    StreamType,
)
from .records import log_record_to_row, row_to_log_record, to_otlp_logs_request
from .streams import (
    AsyncOpenObserveStreams,
    ListDelta,
    OpenObserveStreams,
    PartitionKey,
    SchemaField,
    StreamInfo,
    StreamSettings,
    StreamStats,
)
from .traces import (
    DEFAULT_GET_TRACE_WINDOW,
    DEFAULT_SERVICES_WINDOW,
    DEFAULT_TRACE_SEARCH_WINDOW,
    OPENOBSERVE_GRPC_PORT,
    AsyncOpenObserveTraceQuery,
    OpenObserveTraceQuery,
    TraceTimeRange,
    create_span_exporter,
    grpc_endpoint,
    hit_to_summary,
    init_tracing,
    row_to_span,
    trace_filter,
    traces_endpoint,
    tracing_defaults,
)

__all__ = [
    "__version__",
    # client + config
    "OpenObserveClient",
    "AsyncOpenObserveClient",
    "OpenObserveConfig",
    "DEFAULT_URL",
    "DEFAULT_ORG",
    "DEFAULT_STREAM",
    "normalize_stream_name",
    # logs
    "DEFAULT_SEARCH_WINDOW",
    "OpenObserveLogs",
    "AsyncOpenObserveLogs",
    "RowTail",
    "AsyncRowTail",
    "BulkAction",
    "BulkItem",
    "BulkItemError",
    "BulkResult",
    "FieldValue",
    "FieldValues",
    "Histogram",
    "HistogramBucket",
    "IngestResult",
    "OtlpIngestResult",
    "SearchResult",
    "StreamType",
    "log_record_to_row",
    "row_to_log_record",
    "to_otlp_logs_request",
    # logging category
    "OpenObserveLogExporter",
    "AsyncOpenObserveLogExporter",
    "OpenObserveOtlpLogExporter",
    "AsyncOpenObserveOtlpLogExporter",
    "OpenObserveLogQuery",
    "AsyncOpenObserveLogQuery",
    "is_sql_query",
    "text_query_sql",
    # streams
    "OpenObserveStreams",
    "AsyncOpenObserveStreams",
    "ListDelta",
    "PartitionKey",
    "SchemaField",
    "StreamInfo",
    "StreamSettings",
    "StreamStats",
    # traces
    "DEFAULT_GET_TRACE_WINDOW",
    "DEFAULT_SERVICES_WINDOW",
    "DEFAULT_TRACE_SEARCH_WINDOW",
    "OPENOBSERVE_GRPC_PORT",
    "OpenObserveTraceQuery",
    "AsyncOpenObserveTraceQuery",
    "TraceTimeRange",
    "create_span_exporter",
    "grpc_endpoint",
    "hit_to_summary",
    "init_tracing",
    "row_to_span",
    "trace_filter",
    "traces_endpoint",
    "tracing_defaults",
    # admin
    "OpenObserveAdmin",
    "AsyncOpenObserveAdmin",
    "Organization",
    "Passcode",
    "ServiceAccount",
    "ServiceAccountToken",
]
__all__ += _core.__all__

from . import _logging  # noqa: E402
from ._logging import *  # noqa: E402, F403

__all__ += _logging.__all__

from . import _tracing  # noqa: E402
from ._tracing import *  # noqa: E402, F403

__all__ += _tracing.__all__
