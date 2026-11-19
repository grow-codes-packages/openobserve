"""Mapping between SDK :class:`~grow_openobserve.LogRecord` objects, OpenObserve rows and OTLP.

``log_record_to_row`` is what the ``_json`` log exporter sends; ``row_to_log_record``
normalizes rows returned by ``_search`` (both plain JSON rows and OTLP-ingested rows).
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Any

from ._http import as_int, flatten_key, micros_to_dt, to_micros
from ._logging import AttrValue, LogRecord, Severity

__all__ = [
    "log_record_to_row",
    "otlp_any_value",
    "otlp_attributes",
    "row_to_log_record",
    "to_otlp_logs_request",
]

#: Row columns mapped to ``LogRecord`` fields instead of attributes.
_RECORD_COLUMNS = frozenset(
    {
        "_timestamp",
        "_o2_ingest_ts",
        "body",
        "message",
        "severity",
        "level",
        "severity_number",
        "severity_text",
        "trace_id",
        "span_id",
        "trace_flags",
        "flags",
        "observed_timestamp",
    }
)

#: Flattened resource columns → resource attribute names.
_RESOURCE_COLUMNS: Mapping[str, str] = {
    "service_name": "service.name",
    "service_version": "service.version",
    "service_namespace": "service.namespace",
    "service_instance_id": "service.instance.id",
    "deployment_environment": "deployment.environment",
    "deployment_environment_name": "deployment.environment.name",
    "host_name": "host.name",
}

_SCOPE_NAME = "grow-codes-openobserve"


def _column_value(value: AttrValue) -> Any:
    if isinstance(value, (list, tuple)):
        return json.dumps(list(value), separators=(",", ":"), ensure_ascii=False, default=str)
    if isinstance(value, Mapping):
        return {str(k): _column_value(v) for k, v in value.items()}
    return value


def log_record_to_row(record: LogRecord) -> dict[str, Any]:
    """The row written by ``_json`` ingestion for ``record``.

    Flattened resource and attribute keys, plus ``_timestamp`` (µs), ``body``, ``severity``
    (text), ``severity_number``, ``trace_id``, ``span_id`` and ``trace_flags``. Record fields
    win over attributes with the same column name; lists are stored as JSON strings.
    """
    row: dict[str, Any] = {}
    for key, value in record.resource.items():
        row[flatten_key(key)] = _column_value(value)
    for key, value in record.attributes.items():
        row[flatten_key(key)] = _column_value(value)
    row["_timestamp"] = to_micros(record.timestamp, "logging.export")
    row["body"] = record.body
    row["severity"] = record.severity_text
    row["severity_number"] = int(record.severity)
    if record.trace_id:
        row["trace_id"] = record.trace_id
    if record.span_id:
        row["span_id"] = record.span_id
    if record.trace_flags is not None:
        row["trace_flags"] = record.trace_flags
    return row


def _text(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _severity(row: Mapping[str, Any]) -> tuple[Severity, str | None]:
    text = _text(row.get("severity")) or _text(row.get("severity_text")) or _text(row.get("level"))
    number = row.get("severity_number")
    if isinstance(number, int) and not isinstance(number, bool) and 1 <= number <= 24:
        return Severity.parse(number), text
    if text is not None:
        try:
            return Severity.parse(text), text
        except Exception:  # noqa: BLE001 - vendor words fall back to INFO
            return Severity.INFO, text
    return Severity.INFO, text


def _body(row: Mapping[str, Any]) -> str:
    for key in ("body", "message", "log", "msg"):
        value = row.get(key)
        if isinstance(value, str):
            return value
        if value is not None:
            return json.dumps(value, separators=(",", ":"), ensure_ascii=False)
    return ""


def _attr(value: Any) -> AttrValue:
    if isinstance(value, (str, int, float, bool)):
        return value
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def row_to_log_record(row: Mapping[str, Any]) -> LogRecord:
    """Normalize an OpenObserve row into a :class:`LogRecord`.

    ``_timestamp`` (µs), ``body``|``message``, ``severity``|``level`` (+ ``severity_number``),
    ``trace_id``, ``span_id`` and ``trace_flags`` become record fields; well-known resource
    columns (``service_name``, ``host_name``, …) go to ``resource``; every other non-null column
    goes to ``attributes`` under its flattened server name.
    """
    severity, text = _severity(row)
    attributes: dict[str, AttrValue] = {}
    resource: dict[str, AttrValue] = {}
    for key, value in row.items():
        if key in _RECORD_COLUMNS or value is None:
            continue
        resource_key = _RESOURCE_COLUMNS.get(key)
        if resource_key is not None:
            resource[resource_key] = _attr(value)
        else:
            attributes[key] = _attr(value)
    flags = row.get("trace_flags")
    trace_id = _text(row.get("trace_id"))
    span_id = _text(row.get("span_id"))
    return LogRecord(
        body=_body(row),
        severity=severity,
        severity_text=text or severity.name,
        timestamp=micros_to_dt(as_int(row.get("_timestamp"))),
        attributes=attributes,
        resource=resource,
        trace_id=trace_id.lower() if trace_id else None,
        span_id=span_id.lower() if span_id else None,
        trace_flags=flags if isinstance(flags, int) and not isinstance(flags, bool) else None,
    )


# --------------------------------------------------------------------------- OTLP/JSON


def otlp_any_value(value: Any) -> dict[str, Any]:
    """An attribute value as an OTLP/JSON ``AnyValue``."""
    if isinstance(value, bool):
        return {"boolValue": value}
    if isinstance(value, int):
        return {"intValue": str(value)}
    if isinstance(value, float):
        return {"doubleValue": value}
    if isinstance(value, str):
        return {"stringValue": value}
    if isinstance(value, Mapping):
        return {"kvlistValue": {"values": otlp_attributes(value)}}
    if isinstance(value, (list, tuple)):
        return {"arrayValue": {"values": [otlp_any_value(v) for v in value]}}
    return {"stringValue": str(value)}


def otlp_attributes(attributes: Mapping[str, Any]) -> list[dict[str, Any]]:
    """An attribute map as OTLP/JSON ``KeyValue`` objects."""
    return [{"key": str(k), "value": otlp_any_value(v)} for k, v in attributes.items()]


def _nanos(record_time: Any) -> str:
    return str(to_micros(record_time) * 1000)


def to_otlp_logs_request(records: Sequence[LogRecord], scope_version: str) -> dict[str, Any]:
    """An OTLP/JSON ``ExportLogsServiceRequest`` (grouped by resource, scope
    ``grow-codes-openobserve``)."""
    groups: dict[str, tuple[Mapping[str, Any], list[dict[str, Any]]]] = {}
    for record in records:
        key = json.dumps(record.resource, sort_keys=True, default=str)
        group = groups.setdefault(key, (record.resource, []))
        log: dict[str, Any] = {
            "timeUnixNano": _nanos(record.timestamp),
            "severityNumber": int(record.severity),
            "severityText": record.severity_text,
            "body": {"stringValue": record.body},
            "attributes": otlp_attributes(record.attributes),
        }
        if record.observed_timestamp is not None:
            log["observedTimeUnixNano"] = _nanos(record.observed_timestamp)
        if record.trace_id:
            log["traceId"] = record.trace_id
        if record.span_id:
            log["spanId"] = record.span_id
        if record.trace_flags is not None:
            log["flags"] = record.trace_flags
        group[1].append(log)
    return {
        "resourceLogs": [
            {
                "resource": {"attributes": otlp_attributes(resource)},
                "scopeLogs": [
                    {
                        "scope": {"name": _SCOPE_NAME, "version": scope_version},
                        "logRecords": logs,
                    }
                ],
            }
            for resource, logs in groups.values()
        ]
    }
