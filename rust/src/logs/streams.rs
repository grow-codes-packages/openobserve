// Service-owned. Stream management (API.md §4): list, schema, settings, create, delete.

use std::collections::HashSet;
use std::time::SystemTime;

use serde_json::{json, Value};

use crate::core::{Method, Result};
use crate::http::{
    field, int, items, micros_to_system_time, send_json, stream_name, text, uint, JsonObject,
    StreamType,
};
use crate::OpenObserveClient;

/// Options of [`OpenObserveStreams::list_streams`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListStreamsOptions {
    /// Only names containing this text.
    pub keyword: Option<String>,
    /// Skip this many streams.
    pub offset: Option<u64>,
    /// Return at most this many streams.
    pub limit: Option<u64>,
}

/// One schema field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaField {
    /// Field name.
    pub name: String,
    /// Arrow type name: `Utf8`, `Int64`, `Float64`, `Boolean`, …
    pub field_type: String,
}

impl SchemaField {
    /// A field.
    pub fn new(name: impl Into<String>, field_type: impl Into<String>) -> Self {
        SchemaField {
            name: name.into(),
            field_type: field_type.into(),
        }
    }
}

/// A partition key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionKey {
    /// Field name.
    pub field: String,
    /// `value` (default) or `hash`.
    pub types: String,
    /// Removed keys stay listed with `disabled: true`.
    pub disabled: bool,
}

/// Stream settings (selected fields; `raw` holds the full server object).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct StreamSettings {
    /// Partition keys (also disabled ones).
    pub partition_keys: Vec<PartitionKey>,
    /// Full-text search keys.
    pub full_text_search_keys: Vec<String>,
    /// Secondary index fields.
    pub index_fields: Vec<String>,
    /// Bloom filter fields.
    pub bloom_filter_fields: Vec<String>,
    /// Retention in days; 0 = the server default.
    pub retention_days: u64,
    /// The server's settings object.
    pub raw: JsonObject,
}

/// Stream statistics.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct StreamStats {
    /// Creation time.
    pub created_at: Option<SystemTime>,
    /// Number of documents.
    pub doc_num: u64,
    /// Storage size (MB as reported by the server).
    pub storage_size: f64,
    /// Compressed size (MB as reported by the server).
    pub compressed_size: f64,
}

/// A stream with its schema and settings.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct StreamInfo {
    /// Stream name.
    pub name: String,
    /// `logs`, `traces`, `metrics`, `metadata`, …
    pub stream_type: String,
    /// Storage type (`disk`, `s3`, …).
    pub storage_type: String,
    /// Statistics.
    pub stats: StreamStats,
    /// Schema fields.
    pub schema: Vec<SchemaField>,
    /// Settings.
    pub settings: StreamSettings,
    /// Number of fields.
    pub total_fields: u64,
}

/// A partition key in an update or create: a field name (type `value`) or field + type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionKeyInput {
    /// Field name.
    pub field: String,
    /// `value` (default) or `hash`.
    pub types: Option<String>,
}

impl PartitionKeyInput {
    fn wire(&self) -> Value {
        json!({ "field": self.field, "types": self.types.as_deref().unwrap_or("value") })
    }
}

impl From<&str> for PartitionKeyInput {
    fn from(field: &str) -> Self {
        PartitionKeyInput {
            field: field.to_string(),
            types: None,
        }
    }
}

impl From<String> for PartitionKeyInput {
    fn from(field: String) -> Self {
        PartitionKeyInput { field, types: None }
    }
}

/// A list setting: a full list (replace) or a delta.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListUpdate<T> {
    /// Replace the current list (the SDK computes add/remove deltas from the schema).
    Replace(Vec<T>),
    /// Apply this delta as given.
    Delta {
        /// Items to add.
        add: Vec<T>,
        /// Items to remove.
        remove: Vec<T>,
    },
}

/// Settings to change. `None` settings stay unchanged.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StreamSettingsUpdate {
    /// Retention in days (0 = server default).
    pub retention_days: Option<u64>,
    /// Partition keys.
    pub partition_keys: Option<ListUpdate<PartitionKeyInput>>,
    /// Full-text search keys.
    pub full_text_search_keys: Option<ListUpdate<String>>,
    /// Secondary index fields.
    pub index_fields: Option<ListUpdate<String>>,
    /// Bloom filter fields.
    pub bloom_filter_fields: Option<ListUpdate<String>>,
}

/// Options of [`OpenObserveStreams::create_stream`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CreateStreamOptions {
    /// Stream type (default logs).
    pub stream_type: StreamType,
    /// Initial schema.
    pub fields: Vec<SchemaField>,
    /// Retention in days (0 = server default).
    pub retention_days: u64,
    /// Partition keys.
    pub partition_keys: Vec<PartitionKeyInput>,
    /// Full-text search keys.
    pub full_text_search_keys: Vec<String>,
    /// Secondary index fields.
    pub index_fields: Vec<String>,
    /// Bloom filter fields.
    pub bloom_filter_fields: Vec<String>,
}

/// The OpenObserve streams module: `client.streams()`.
#[derive(Debug, Clone)]
pub struct OpenObserveStreams {
    client: OpenObserveClient,
}

impl OpenObserveStreams {
    pub(crate) fn new(client: OpenObserveClient) -> Self {
        OpenObserveStreams { client }
    }

    /// Lists streams of one type: `GET /api/{org}/streams?type=…` (sorted by name).
    pub async fn list_streams(
        &self,
        stream_type: StreamType,
        opts: ListStreamsOptions,
    ) -> Result<Vec<StreamInfo>> {
        let op = "streams.listStreams";
        let mut req = self
            .client
            .request(Method::GET, &self.client.org_path(&["streams"]))
            .query("type", stream_type.as_str())
            .operation(op);
        if let Some(k) = &opts.keyword {
            req = req.query("keyword", k.clone());
        }
        if let Some(o) = opts.offset {
            req = req.query("offset", o.to_string());
        }
        if let Some(l) = opts.limit {
            req = req.query("limit", l.to_string());
        }
        let data = send_json(req.query("sort", "name").query("asc", "true"), op).await?;
        Ok(items(field(&data, "list"))
            .iter()
            .map(stream_info)
            .collect())
    }

    /// Schema, stats and settings of one stream; `NOT_FOUND` when missing.
    pub async fn get_stream_schema(
        &self,
        stream: &str,
        stream_type: StreamType,
    ) -> Result<StreamInfo> {
        let name = stream_name(stream, "streams.getStreamSchema")?;
        let op = format!("streams.getStreamSchema {name}");
        let data = send_json(
            self.client
                .request(
                    Method::GET,
                    &self.client.org_path(&["streams", &name, "schema"]),
                )
                .query("type", stream_type.as_str())
                .operation(op.clone()),
            &op,
        )
        .await?;
        Ok(stream_info(&data))
    }

    /// Changes stream settings: `PUT /api/{org}/streams/{stream}/settings`. A
    /// [`ListUpdate::Replace`] replaces the current list (the SDK reads the schema and sends
    /// add/remove deltas); [`ListUpdate::Delta`] is sent as given. `NOT_FOUND` when the stream
    /// is missing.
    pub async fn update_stream_settings(
        &self,
        stream: &str,
        update: StreamSettingsUpdate,
        stream_type: StreamType,
    ) -> Result<()> {
        let name = stream_name(stream, "streams.updateStreamSettings")?;
        let needs_current = matches!(update.partition_keys, Some(ListUpdate::Replace(_)))
            || [
                &update.full_text_search_keys,
                &update.index_fields,
                &update.bloom_filter_fields,
            ]
            .iter()
            .any(|u| matches!(u, Some(ListUpdate::Replace(_))));
        let current = if needs_current {
            self.get_stream_schema(&name, stream_type).await?.settings
        } else {
            StreamSettings::default()
        };
        let mut body = JsonObject::new();
        if let Some(days) = update.retention_days {
            body.insert("data_retention".into(), json!(days));
        }
        if let Some(keys) = update.partition_keys {
            let enabled: Vec<Value> = current
                .partition_keys
                .iter()
                .filter(|k| !k.disabled)
                .map(|k| json!({ "field": k.field, "types": k.types }))
                .collect();
            let wanted = |list: Vec<PartitionKeyInput>| -> Vec<Value> {
                list.iter().map(PartitionKeyInput::wire).collect()
            };
            let key_of = |v: &Value| field(v, "field").as_str().unwrap_or("").to_string();
            body.insert(
                "partition_keys".into(),
                delta(keys, wanted, &enabled, key_of),
            );
        }
        let lists = [
            (
                update.full_text_search_keys,
                &current.full_text_search_keys,
                "full_text_search_keys",
            ),
            (update.index_fields, &current.index_fields, "index_fields"),
            (
                update.bloom_filter_fields,
                &current.bloom_filter_fields,
                "bloom_filter_fields",
            ),
        ];
        for (value, current, wire) in lists {
            let Some(value) = value else { continue };
            let current: Vec<Value> = current.iter().map(|s| json!(s)).collect();
            let to_wire =
                |l: Vec<String>| -> Vec<Value> { l.into_iter().map(Value::from).collect() };
            let key_of = |v: &Value| v.as_str().unwrap_or("").to_string();
            body.insert(wire.into(), delta(value, to_wire, &current, key_of));
        }
        let op = format!("streams.updateStreamSettings {name}");
        send_json(
            self.client
                .request(
                    Method::PUT,
                    &self.client.org_path(&["streams", &name, "settings"]),
                )
                .query("type", stream_type.as_str())
                .json(&Value::Object(body))
                .operation(op.clone()),
            &op,
        )
        .await?;
        Ok(())
    }

    /// Creates a stream with an optional schema and settings:
    /// `POST /api/{org}/streams/{stream}`.
    pub async fn create_stream(&self, stream: &str, opts: CreateStreamOptions) -> Result<()> {
        let name = stream_name(stream, "streams.createStream")?;
        let op = format!("streams.createStream {name}");
        let body = json!({
            "fields": opts.fields.iter().map(|f| json!({"name": f.name, "type": f.field_type})).collect::<Vec<_>>(),
            "settings": {
                "partition_keys": opts.partition_keys.iter().map(PartitionKeyInput::wire).collect::<Vec<_>>(),
                "index_fields": opts.index_fields,
                "full_text_search_keys": opts.full_text_search_keys,
                "bloom_filter_fields": opts.bloom_filter_fields,
                "data_retention": opts.retention_days,
            },
        });
        send_json(
            self.client
                .request(Method::POST, &self.client.org_path(&["streams", &name]))
                .query("type", opts.stream_type.as_str())
                .json(&body)
                .operation(op.clone()),
            &op,
        )
        .await?;
        Ok(())
    }

    /// Deletes a stream and its data: `DELETE /api/{org}/streams/{stream}` (alerts and
    /// dashboards are kept). `NOT_FOUND` when missing.
    pub async fn delete_stream(&self, stream: &str, stream_type: StreamType) -> Result<()> {
        let name = stream_name(stream, "streams.deleteStream")?;
        let op = format!("streams.deleteStream {name}");
        send_json(
            self.client
                .request(Method::DELETE, &self.client.org_path(&["streams", &name]))
                .query("type", stream_type.as_str())
                .operation(op.clone()),
            &op,
        )
        .await?;
        Ok(())
    }
}

fn delta<T>(
    update: ListUpdate<T>,
    to_wire: impl Fn(Vec<T>) -> Vec<Value>,
    current: &[Value],
    key_of: impl Fn(&Value) -> String,
) -> Value {
    match update {
        ListUpdate::Delta { add, remove } => {
            json!({ "add": to_wire(add), "remove": to_wire(remove) })
        }
        ListUpdate::Replace(list) => {
            let wanted = to_wire(list);
            let wanted_keys: HashSet<String> = wanted.iter().map(&key_of).collect();
            let current_keys: HashSet<String> = current.iter().map(&key_of).collect();
            let add: Vec<&Value> = wanted
                .iter()
                .filter(|w| !current_keys.contains(&key_of(w)))
                .collect();
            let remove: Vec<&Value> = current
                .iter()
                .filter(|c| !wanted_keys.contains(&key_of(c)))
                .collect();
            json!({ "add": add, "remove": remove })
        }
    }
}

fn strings(value: &Value) -> Vec<String> {
    items(value)
        .iter()
        .filter_map(|v| v.as_str().map(str::to_string))
        .collect()
}

fn stream_info(data: &Value) -> StreamInfo {
    let stats = field(data, "stats");
    let settings = field(data, "settings");
    let created = int(field(stats, "created_at"));
    let partition_keys = settings
        .get("partition_keys")
        .and_then(Value::as_object)
        .map(|m| {
            m.values()
                .map(|k| PartitionKey {
                    field: text(field(k, "field")).unwrap_or_default(),
                    types: match field(k, "types") {
                        Value::String(s) => s.clone(),
                        Value::Null => "value".to_string(),
                        other => other.to_string(),
                    },
                    disabled: field(k, "disabled") == &Value::Bool(true),
                })
                .collect()
        })
        .unwrap_or_default();
    StreamInfo {
        name: text(field(data, "name")).unwrap_or_default(),
        stream_type: text(field(data, "stream_type")).unwrap_or_default(),
        storage_type: text(field(data, "storage_type")).unwrap_or_default(),
        stats: StreamStats {
            created_at: (created > 0).then(|| micros_to_system_time(created)),
            doc_num: uint(field(stats, "doc_num")),
            storage_size: field(stats, "storage_size").as_f64().unwrap_or(0.0),
            compressed_size: field(stats, "compressed_size").as_f64().unwrap_or(0.0),
        },
        schema: items(field(data, "schema"))
            .iter()
            .map(|f| SchemaField {
                name: text(field(f, "name")).unwrap_or_default(),
                field_type: text(field(f, "type")).unwrap_or_default(),
            })
            .collect(),
        settings: StreamSettings {
            partition_keys,
            full_text_search_keys: strings(field(settings, "full_text_search_keys")),
            index_fields: strings(field(settings, "index_fields")),
            bloom_filter_fields: strings(field(settings, "bloom_filter_fields")),
            retention_days: uint(field(settings, "data_retention")),
            raw: settings.as_object().cloned().unwrap_or_default(),
        },
        total_fields: uint(field(data, "total_fields")),
    }
}
