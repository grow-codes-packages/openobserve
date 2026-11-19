// Service-owned. LogQueryClient for OpenObserve (DECISIONS 2 and 3).

use std::sync::Arc;

use async_trait::async_trait;

use super::api::{LogTailOptions, OpenObserveLogs, SearchOptions as RawSearchOptions};
use super::records::row_to_log_record;
use crate::core::{Error, Result};
use crate::http::{sql_ident, sql_string, stream_name, StreamType, Timestamp};
use crate::logging::{
    poll_tail, LogQueryClient, LogSearchResult, RecordCallback, SearchOptions, SortOrder,
    TailHandle, TailOptions,
};
use crate::OpenObserveClient;

/// True when `query` is raw SQL (starts with `SELECT`, case-insensitive).
pub fn is_sql_query(query: &str) -> bool {
    let t = query.trim_start().as_bytes();
    t.len() > 6 && t[..6].eq_ignore_ascii_case(b"select") && t[6].is_ascii_whitespace()
}

/// Builds the SQL for a text query: `SELECT * FROM "<stream>" WHERE match_all('<text>')
/// ORDER BY _timestamp <order>`; an empty text matches every row.
pub fn text_query_sql(stream: &str, text: &str, order: SortOrder) -> String {
    let filter = if text.trim().is_empty() {
        String::new()
    } else {
        format!(" WHERE match_all({})", sql_string(text))
    };
    let order = match order {
        SortOrder::Asc => "ASC",
        SortOrder::Desc => "DESC",
    };
    format!(
        "SELECT * FROM {}{filter} ORDER BY _timestamp {order}",
        sql_ident(stream)
    )
}

/// [`LogQueryClient`] on one stream (default: the config's `stream`). A query that starts with
/// `SELECT` is raw SQL; any other text becomes `match_all('<text>')` (full-text search over the
/// stream's full-text keys, case-insensitive). The cursor is a row offset.
#[derive(Debug, Clone)]
pub struct OpenObserveLogQuery {
    client: OpenObserveClient,
    stream: String,
}

impl OpenObserveLogQuery {
    pub(crate) fn new(client: OpenObserveClient, stream: &str) -> Result<Self> {
        Ok(OpenObserveLogQuery {
            client,
            stream: stream_name(stream, "logging.query")?,
        })
    }

    /// The stream queried by text queries, `list_labels` and `tail`.
    pub fn stream(&self) -> &str {
        &self.stream
    }

    fn logs(&self) -> OpenObserveLogs {
        self.client.logs()
    }

    /// [`search`](LogQueryClient::search) that, with `track_total_hits`, also fetches the
    /// exact number of matches into `total` (one extra request). Default window: the last
    /// 15 minutes, newest first, 100 per page.
    pub async fn search_with(
        &self,
        query: &str,
        opts: SearchOptions,
        track_total_hits: bool,
    ) -> Result<LogSearchResult> {
        if opts.limit == 0 {
            return Err(Error::validation("logging.search: limit must be positive"));
        }
        let offset: u64 = match &opts.cursor {
            None => 0,
            Some(c) => c
                .trim()
                .parse()
                .map_err(|_| Error::validation("logging.search: invalid cursor"))?,
        };
        let sql = if is_sql_query(query) {
            query.to_string()
        } else {
            text_query_sql(&self.stream, query, opts.order)
        };
        let limit = opts.limit;
        let result = self
            .logs()
            .search(
                &sql,
                RawSearchOptions {
                    start: opts.start.map(Timestamp::from),
                    end: opts.end.map(Timestamp::from),
                    from: offset,
                    size: i64::try_from(limit).unwrap_or(i64::MAX),
                    track_total_hits,
                    stream_type: StreamType::Logs,
                },
            )
            .await?;
        let count = result.hits.len();
        Ok(LogSearchResult {
            records: result.hits.iter().map(row_to_log_record).collect(),
            next_cursor: (count >= limit).then(|| (offset + count as u64).to_string()),
            total: result.total_hits,
        })
    }
}

#[async_trait]
impl LogQueryClient for OpenObserveLogQuery {
    /// Searches records; default window: the last 15 minutes, newest first, 100 per page.
    async fn search(&self, query: &str, opts: SearchOptions) -> Result<LogSearchResult> {
        self.search_with(query, opts, false).await
    }

    /// Follows new records. Text queries use the cursor-based [`OpenObserveLogs::tail`] on the
    /// stream; raw SQL queries use the generic polling tail on top of `search`.
    async fn tail(
        &self,
        query: &str,
        on_record: RecordCallback,
        opts: TailOptions,
    ) -> Result<TailHandle> {
        if is_sql_query(query) {
            tokio::runtime::Handle::try_current()
                .map_err(|_| Error::config("logs.tail must be called inside a tokio runtime"))?;
            return Ok(poll_tail(Arc::new(self.clone()), query, on_record, opts));
        }
        let tail = LogTailOptions {
            filter: (!query.trim().is_empty()).then(|| format!("match_all({})", sql_string(query))),
            start: opts.start.map(Timestamp::from),
            interval: opts.interval,
            ..LogTailOptions::default()
        };
        self.logs().tail(
            &self.stream,
            Arc::new(move |row| on_record(row_to_log_record(&row))),
            tail,
        )
    }

    /// Names of the log streams of the organization.
    async fn list_streams(&self) -> Result<Vec<String>> {
        Ok(self
            .client
            .streams()
            .list_streams(StreamType::Logs, Default::default())
            .await?
            .into_iter()
            .map(|s| s.name)
            .collect())
    }

    /// Field names of the stream (from its schema).
    async fn list_labels(&self) -> Result<Vec<String>> {
        Ok(self
            .client
            .streams()
            .get_stream_schema(&self.stream, StreamType::Logs)
            .await?
            .schema
            .into_iter()
            .map(|f| f.name)
            .collect())
    }
}
