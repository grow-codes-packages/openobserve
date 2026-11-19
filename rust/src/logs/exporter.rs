// Service-owned. LogExporter implementations for the category BatchingLogClient.

use async_trait::async_trait;

use super::api::{IngestOptions, OpenObserveLogs, OtlpIngestOptions};
use super::records::log_record_to_row;
use crate::core::{Error, Result};
use crate::logging::{ExportOptions, ExportResult, LogExporter, LogRecord};

/// Exports records with `_json` ingestion into one stream (API.md §2 recommendation):
/// per-stream counters give exact accepted/rejected numbers.
#[derive(Debug, Clone)]
pub struct OpenObserveLogExporter {
    logs: OpenObserveLogs,
    stream: String,
}

impl OpenObserveLogExporter {
    /// Exporter writing to `stream` through `logs`.
    pub fn new(logs: OpenObserveLogs, stream: impl Into<String>) -> Self {
        OpenObserveLogExporter {
            logs,
            stream: stream.into(),
        }
    }

    /// Target stream.
    pub fn stream(&self) -> &str {
        &self.stream
    }
}

#[async_trait]
impl LogExporter for OpenObserveLogExporter {
    async fn export(&self, records: &[LogRecord], opts: ExportOptions) -> Result<ExportResult> {
        if records.is_empty() {
            return Ok(ExportResult::accepted(0));
        }
        let rows: Vec<serde_json::Value> = records
            .iter()
            .map(|r| serde_json::Value::Object(log_record_to_row(r)))
            .collect();
        let result = self
            .logs
            .ingest_json(&self.stream, &rows, IngestOptions::gzip(opts.gzip))
            .await?;
        let rejected = usize::try_from(result.failed)
            .unwrap_or(usize::MAX)
            .min(records.len());
        if rejected == 0 {
            return Ok(ExportResult::accepted(records.len()));
        }
        let detail = result
            .error
            .map(|e| format!(" ({})", e.chars().take(300).collect::<String>()))
            .unwrap_or_default();
        Ok(ExportResult::partial(
            records.len() - rejected,
            rejected,
            Some(Error::validation(format!(
                "logging.export {}: {rejected} records rejected{detail}",
                self.stream
            ))),
        ))
    }
}

/// Exports records as OTLP/JSON to `/api/{org}/v1/logs` with the `stream-name` header.
#[derive(Debug, Clone)]
pub struct OpenObserveOtlpLogExporter {
    logs: OpenObserveLogs,
    stream: String,
}

impl OpenObserveOtlpLogExporter {
    /// Exporter writing to `stream` through `logs`.
    pub fn new(logs: OpenObserveLogs, stream: impl Into<String>) -> Self {
        OpenObserveOtlpLogExporter {
            logs,
            stream: stream.into(),
        }
    }

    /// Target stream.
    pub fn stream(&self) -> &str {
        &self.stream
    }
}

#[async_trait]
impl LogExporter for OpenObserveOtlpLogExporter {
    async fn export(&self, records: &[LogRecord], opts: ExportOptions) -> Result<ExportResult> {
        let result = self
            .logs
            .ingest_otlp(
                records,
                OtlpIngestOptions {
                    gzip: opts.gzip,
                    stream: Some(self.stream.clone()),
                },
            )
            .await?;
        let error = (result.rejected > 0).then(|| {
            Error::validation(format!(
                "logging.export {}: {} records rejected",
                self.stream, result.rejected
            ))
        });
        Ok(ExportResult::partial(
            result.accepted,
            result.rejected,
            error,
        ))
    }
}
