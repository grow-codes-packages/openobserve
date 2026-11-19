# Streams

A stream is OpenObserve's table: `logs`, `traces`, `metrics` or `metadata`. Every call takes a
stream type (default `logs`); stream names are normalized like the server does.

| Function | Request | Result |
| - | - | - |
| `listStreams(type, {keyword, offset, limit})` | `GET /api/{org}/streams?type=` | streams with schema, settings and stats (Go and Java return a page with the total) |
| `getStreamSchema(stream)` | `GET /api/{org}/streams/{stream}/schema` | `StreamInfo` (fields, settings, stats); `NOT_FOUND` for a missing stream |
| `updateStreamSettings(stream, update)` | `PUT /api/{org}/streams/{stream}/settings` | — |
| `createStream(stream, {fields, settings})` | `POST /api/{org}/streams/{stream}` | creates the stream before the first write |
| `deleteStream(stream)` | `DELETE /api/{org}/streams/{stream}` | deletes the stream and its data; `NOT_FOUND` when missing |

## `updateStreamSettings`

Settings: `retentionDays`, `partitionKeys`, `fullTextSearchKeys`, `indexFields` and
`bloomFilterFields`. An omitted setting is left unchanged.

- A **list replaces** the current value: the SDK reads the schema and sends the add/remove deltas
  OpenObserve expects; nothing is sent when nothing changes.
- TypeScript, Python and Rust can also pass an explicit delta (`{ add, remove }`, `ListDelta`,
  `ListUpdate::Delta`), which is sent as given.
- Removed partition keys stay in the schema as **disabled** (`disabled: true`).
- Partition key types default to `value`.

=== "TypeScript"

    ```ts
    const s = client.streams;
    await s.createStream("orders", {
      fields: [{ name: "message", type: "Utf8" }],
      retentionDays: 3,
    });
    const list = await s.listStreams("logs", { keyword: "ord" });
    const info = await s.getStreamSchema("orders");
    await s.updateStreamSettings("orders", {
      retentionDays: 7,
      partitionKeys: ["region"],
      fullTextSearchKeys: ["message"],
      indexFields: { add: ["order_id"], remove: [] },
    });
    await s.deleteStream("orders");
    ```

=== "Rust"

    ```rust
    use grow_openobserve::logs::{CreateStreamOptions, ListStreamsOptions, ListUpdate, SchemaField, StreamSettingsUpdate};
    use grow_openobserve::StreamType;

    let s = client.streams();
    s.create_stream("orders", CreateStreamOptions {
        fields: vec![SchemaField::new("message", "Utf8")],
        retention_days: 3,
        ..Default::default()
    })
    .await?;
    let list = s.list_streams(StreamType::Logs, ListStreamsOptions { keyword: Some("ord".into()), ..Default::default() }).await?;
    let info = s.get_stream_schema("orders", StreamType::Logs).await?;
    s.update_stream_settings(
        "orders",
        StreamSettingsUpdate {
            retention_days: Some(7),
            partition_keys: Some(ListUpdate::Replace(vec!["region".into()])),
            full_text_search_keys: Some(ListUpdate::Replace(vec!["message".into()])),
            ..Default::default()
        },
        StreamType::Logs,
    )
    .await?;
    s.delete_stream("orders", StreamType::Logs).await?;
    ```

=== "Go"

    ```go
    s := client.Streams()
    err := s.CreateStream(ctx, "orders", openobserve.CreateStreamOptions{
        Fields:   []openobserve.SchemaField{{Name: "message", Type: "Utf8"}},
        Settings: openobserve.StreamSettings{RetentionDays: 3},
    })
    list, err := s.ListStreams(ctx, openobserve.ListStreamsOptions{Type: openobserve.StreamTypeLogs, Keyword: "ord"})
    info, err := s.GetStreamSchema(ctx, "orders", openobserve.StreamTypeLogs)
    days := 7
    err = s.UpdateStreamSettings(ctx, "orders", openobserve.StreamSettingsUpdate{
        RetentionDays:      &days,
        PartitionKeys:      []openobserve.PartitionKey{{Field: "region"}},
        FullTextSearchKeys: []string{"message"},
        IndexFields:        []string{"order_id"},
    })
    err = s.DeleteStream(ctx, "orders", openobserve.StreamTypeLogs)
    ```

=== "Python"

    ```python
    from grow_openobserve import ListDelta, SchemaField

    s = client.streams
    s.create_stream("orders", fields=[SchemaField("message", "Utf8")], retention_days=3)
    streams = s.list_streams("logs", keyword="ord")
    info = s.get_stream_schema("orders")
    s.update_stream_settings(
        "orders",
        retention_days=7,
        partition_keys=["region"],
        full_text_search_keys=["message"],
        index_fields=ListDelta(add=["order_id"]),
    )
    s.delete_stream("orders")
    ```

=== "Java"

    ```java
    Streams s = client.streams();
    s.createStream("orders", CreateStreamOptions.defaults());
    StreamList list = s.listStreams(ListStreamsOptions.of(StreamType.LOGS));
    StreamInfo info = s.getStreamSchema("orders");
    s.updateStreamSettings("orders", StreamSettingsUpdate.builder()
        .retentionDays(7)
        .partitionKeys(List.of(PartitionKey.of("region")))
        .fullTextSearchKeys(List.of("message"))
        .indexFields(List.of("order_id"))
        .build());
    s.deleteStream("orders");
    ```
