// Skeleton created by sdk-kit 0.1.0; owned by this repository (edit freely).
//! Blocking wrapper (feature `blocking`). Each client owns a current-thread tokio runtime.
//!
//! Do not use it from inside an async runtime (calls would panic); use the async client there.
//! The most common calls have blocking twins here; run any other async call with
//! [`OpenObserveClient::block_on`], e.g. `c.block_on(c.async_client().streams().list_streams(…))`.

use std::future::Future;
use std::time::Duration;

use serde_json::Value;

use crate::admin::Organization;
use crate::core::{Error, HealthStatus, Result};
use crate::logs::{
    IngestOptions, IngestResult, ListStreamsOptions, SearchOptions, SearchResult, StreamInfo,
};
use crate::tracing::{Trace, TraceQueryClient, TraceSearchQuery, TraceSummary};
use crate::{OpenObserveClient as AsyncClient, OpenObserveConfig, StreamType};

/// Blocking client for OpenObserve with the same methods as the async client.
#[derive(Debug)]
pub struct OpenObserveClient {
    inner: AsyncClient,
    runtime: tokio::runtime::Runtime,
}

impl OpenObserveClient {
    /// Creates a client from a configuration.
    pub fn new(config: OpenObserveConfig) -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| Error::config("cannot start the tokio runtime").with_source(e))?;
        let inner = {
            let _guard = runtime.enter();
            AsyncClient::new(config)?
        };
        Ok(OpenObserveClient { inner, runtime })
    }

    /// Creates a client from the process environment.
    pub fn from_env() -> Result<Self> {
        Self::new(OpenObserveConfig::from_env()?)
    }

    /// The wrapped async client.
    pub fn async_client(&self) -> &AsyncClient {
        &self.inner
    }

    /// Runs a future of the async API to completion on this client's runtime.
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.runtime.block_on(future)
    }

    /// See [`AsyncClient::health`].
    pub fn health(&self) -> Result<HealthStatus> {
        self.block_on(self.inner.health())
    }

    /// See [`AsyncClient::ping`].
    pub fn ping(&self) -> Result<Duration> {
        self.block_on(self.inner.ping())
    }

    /// See [`AsyncClient::server_version`].
    pub fn server_version(&self) -> Result<String> {
        self.block_on(self.inner.server_version())
    }

    /// See [`OpenObserveLogs::ingest_json`](crate::logs::OpenObserveLogs::ingest_json).
    pub fn ingest_json(
        &self,
        stream: &str,
        records: &[Value],
        opts: IngestOptions,
    ) -> Result<IngestResult> {
        self.block_on(self.inner.logs().ingest_json(stream, records, opts))
    }

    /// See [`OpenObserveLogs::ingest_multi`](crate::logs::OpenObserveLogs::ingest_multi).
    pub fn ingest_multi(
        &self,
        stream: &str,
        ndjson: &str,
        opts: IngestOptions,
    ) -> Result<IngestResult> {
        self.block_on(self.inner.logs().ingest_multi(stream, ndjson, opts))
    }

    /// See [`OpenObserveLogs::search`](crate::logs::OpenObserveLogs::search).
    pub fn search(&self, sql: &str, opts: SearchOptions) -> Result<SearchResult> {
        self.block_on(self.inner.logs().search(sql, opts))
    }

    /// See [`OpenObserveStreams::list_streams`](crate::logs::OpenObserveStreams::list_streams).
    pub fn list_streams(
        &self,
        stream_type: StreamType,
        opts: ListStreamsOptions,
    ) -> Result<Vec<StreamInfo>> {
        self.block_on(self.inner.streams().list_streams(stream_type, opts))
    }

    /// See [`OpenObserveStreams::get_stream_schema`](crate::logs::OpenObserveStreams::get_stream_schema).
    pub fn get_stream_schema(&self, stream: &str, stream_type: StreamType) -> Result<StreamInfo> {
        self.block_on(self.inner.streams().get_stream_schema(stream, stream_type))
    }

    /// See [`OpenObserveStreams::delete_stream`](crate::logs::OpenObserveStreams::delete_stream).
    pub fn delete_stream(&self, stream: &str, stream_type: StreamType) -> Result<()> {
        self.block_on(self.inner.streams().delete_stream(stream, stream_type))
    }

    /// See [`TraceQueryClient::get_trace`] on [`AsyncClient::traces`].
    pub fn get_trace(&self, trace_id: &str) -> Result<Option<Trace>> {
        self.block_on(self.inner.traces().get_trace(trace_id))
    }

    /// See [`TraceQueryClient::search_traces`] on [`AsyncClient::traces`].
    pub fn search_traces(&self, query: &TraceSearchQuery) -> Result<Vec<TraceSummary>> {
        let traces = self.inner.traces();
        self.block_on(traces.search_traces(query))
    }

    /// See [`OpenObserveAdmin::list_organizations`](crate::admin::OpenObserveAdmin::list_organizations).
    pub fn list_organizations(&self) -> Result<Vec<Organization>> {
        self.block_on(self.inner.admin().list_organizations())
    }

    /// See [`AsyncClient::close`]. Idempotent.
    pub fn close(&self) {
        self.block_on(self.inner.close());
    }
}

impl Drop for OpenObserveClient {
    fn drop(&mut self) {
        self.inner.transport().close();
    }
}
