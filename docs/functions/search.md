# Search and tail

SQL search over any stream plus the helpers OpenObserve offers next to it.

| Function | Request | Defaults |
| - | - | - |
| `search(sql, {start, end, from, size, trackTotalHits, type})` | `POST /api/{org}/_search?type=logs` | last 15 min, `from` 0, `size` 100 |
| `searchAround(stream, timestamp, size)` | `GET /api/{org}/{stream}/_around?key=<µs>&size=` | 10 rows, anchor included |
| `valuesOf(stream, fields, {start, end, size})` | `GET /api/{org}/{stream}/_values?fields=a,b` | last 15 min, 10 values per field |
| `histogram(stream, {start, end, interval, filter})` | `SELECT histogram(_timestamp, '<interval>') …` via `_search` | last 15 min, interval chosen by the server |
| `tail(stream, onRow, {filter, start, interval, overlap, pageSize, onError})` | `_search` polling with a `_timestamp` cursor | start now, every 1 s, 1 s overlap, 1000 rows per poll |

## `search`

- Quote stream names with double quotes: `SELECT * FROM "app"`. Without `ORDER BY` rows come
  newest first. Find text with `match_all('x')`, `str_match(field, 'x')` or `re_match(field, 're')`.
- `start`/`end` are converted to epoch **microseconds**; `start > end` → `VALIDATION`.
- A `LIMIT` in the SQL overrides `size`; `size` 0 or -1 means the server's default limit.
- The server's `total` is the number of rows **in this page**. With `trackTotalHits` the SDK sends a
  second count request (`track_total_hits: true`, `from: 0`) and fills `totalHits` with the exact
  number of matching rows.
- The result carries `hits` (flat rows with `_timestamp` in µs), `total`, `from`, `size`, `took`
  (ms), `scanRecords`, `isPartial`, `histogramInterval` and `requestId` (the server's search id).

=== "TypeScript"

    ```ts
    const res = await client.logs.search(
      `SELECT * FROM "app" WHERE level = 'error' ORDER BY _timestamp DESC`,
      { start: new Date(Date.now() - 3_600_000), size: 50, trackTotalHits: true },
    );
    console.log(res.totalHits, res.hits.map((h) => h.message));
    ```

=== "Rust"

    ```rust
    use std::time::{Duration, SystemTime};
    use grow_openobserve::logs::SearchOptions;
    use grow_openobserve::Timestamp;

    let res = client
        .logs()
        .search(
            "SELECT * FROM \"app\" WHERE level = 'error' ORDER BY _timestamp DESC",
            SearchOptions::default()
                .range(Some(Timestamp::Time(SystemTime::now() - Duration::from_secs(3600))), None)
                .page(0, 50)
                .track_total_hits(true),
        )
        .await?;
    println!("{:?} {}", res.total_hits, res.hits.len());
    ```

=== "Go"

    ```go
    res, err := client.Logs().Search(ctx,
        `SELECT * FROM "app" WHERE level = 'error' ORDER BY _timestamp DESC`,
        openobserve.SQLSearchOptions{Start: time.Now().Add(-time.Hour), Size: 50, TrackTotalHits: true})
    if err != nil {
        return err
    }
    fmt.Println(*res.Total, len(res.Hits))
    ```

=== "Python"

    ```python
    from datetime import datetime, timedelta, timezone

    res = client.logs.search(
        "SELECT * FROM \"app\" WHERE level = 'error' ORDER BY _timestamp DESC",
        start=datetime.now(timezone.utc) - timedelta(hours=1),
        size=50,
        track_total_hits=True,
    )
    print(res.total_hits, [h["message"] for h in res.hits])
    ```

=== "Java"

    ```java
    SqlSearchResult res = client.logs().search(
        "SELECT * FROM \"app\" WHERE level = 'error' ORDER BY _timestamp DESC",
        SqlSearchOptions.builder()
            .start(Instant.now().minus(Duration.ofHours(1)))
            .size(50)
            .trackTotalHits(true)
            .build());
    System.out.println(res.total() + " " + res.hits().size());
    ```

## `searchAround`, `valuesOf`, `histogram`

`searchAround` returns up to `size` rows around one timestamp, split before and after it, with
the anchor row included; pass the exact `_timestamp` (µs) of a row or a time. TypeScript, Python
and Rust also accept a `sql` option, which OpenObserve v1.1.2 accepts but **ignores** (the rows are
not filtered); see [Compatibility](../compatibility.md).

`valuesOf` returns the most frequent values of each field with their counts. `histogram` returns
`{start, key, count}` buckets plus the bucket width in seconds; `interval` looks like `"1 minute"`
(Go and Java take a duration and format it), `filter` is a SQL `WHERE` condition.

=== "TypeScript"

    ```ts
    const anchor = res.hits[0]!._timestamp as number;
    const around = await client.logs.searchAround("app", anchor, { size: 20 });
    const values = await client.logs.valuesOf("app", ["level", "service_name"], { size: 5 });
    const hist = await client.logs.histogram("app", { interval: "1 minute", filter: "level = 'error'" });
    hist.buckets.forEach((b) => console.log(b.start.toISOString(), b.count));
    ```

=== "Rust"

    ```rust
    use grow_openobserve::logs::{HistogramOptions, SearchAroundOptions, ValuesOptions};
    use grow_openobserve::Timestamp;

    let around = logs
        .search_around("app", Timestamp::Micros(anchor), SearchAroundOptions { size: 20, ..Default::default() })
        .await?;
    let values = logs.values_of("app", &["level", "service_name"], ValuesOptions::default()).await?;
    let hist = logs
        .histogram("app", HistogramOptions {
            interval: Some("1 minute".into()),
            filter: Some("level = 'error'".into()),
            ..Default::default()
        })
        .await?;
    ```

=== "Go"

    ```go
    around, err := client.Logs().SearchAround(ctx, "app", anchorTime, 20)
    values, err := client.Logs().ValuesOf(ctx, "app", []string{"level", "service_name"}, openobserve.ValuesOptions{Size: 5})
    hist, err := client.Logs().Histogram(ctx, "app", openobserve.HistogramOptions{Interval: time.Minute, Filter: "level = 'error'"})
    for _, b := range hist.Buckets {
        fmt.Println(b.Start, b.Count)
    }
    ```

=== "Python"

    ```python
    around = client.logs.search_around("app", anchor_us, size=20)
    values = client.logs.values_of("app", ["level", "service_name"], size=5)
    hist = client.logs.histogram("app", interval="1 minute", filter="level = 'error'")
    for b in hist.buckets:
        print(b.start, b.count)
    ```

=== "Java"

    ```java
    SqlSearchResult around = client.logs().searchAround("app", anchor, 20);
    List<FieldValues> values = client.logs().valuesOf("app", List.of("level", "service_name"));
    HistogramResult hist = client.logs().histogram("app",
        HistogramOptions.of(null, null, Duration.ofMinutes(1), "level = 'error'"));
    hist.buckets().forEach(b -> System.out.println(b.start() + " " + b.count()));
    ```

## `tail`

OpenObserve has no live tail, so `tail` polls `_search` with a `_timestamp` cursor:

1. Every poll queries `_timestamp >= max(start, cursor − overlap)` in ascending order and pages
   through full pages. The overlap catches rows that arrive late; it never reaches back before
   `start`, so rows older than `start` are never delivered.
2. Rows already delivered are dropped by **full-row equality**, so two identical rows within the
   overlap window are delivered once.
3. Query errors go to `onError` (Go: a debug log line when no callback is set) and polling continues;
   a throwing callback does not stop the tail. `stop()` is idempotent.

The category method `logQuery(stream).tail(query, onRecord)` ([Log client and search](logging.md))
uses the same mechanism and delivers normalized `LogRecord`s instead of raw rows.

=== "TypeScript"

    ```ts
    const handle = client.logs.tail("app", (row) => console.log(row.message), {
      filter: "level = 'error'",
      intervalMs: 1000,
      onError: (e) => console.warn(e),
    });
    // later
    handle.stop();
    ```

=== "Rust"

    ```rust
    use std::sync::Arc;
    use grow_openobserve::logs::LogTailOptions;

    let handle = client.logs().tail(
        "app",
        Arc::new(|row| println!("{:?}", row.get("message"))),
        LogTailOptions::default().filter("level = 'error'"),
    )?;
    handle.stop();
    ```

=== "Go"

    ```go
    h, err := client.Logs().Tail(ctx, "app", func(row map[string]any) {
        fmt.Println(row["message"])
    }, openobserve.LogTailOptions{Filter: "level = 'error'", OnError: func(err error) { log.Print(err) }})
    if err != nil {
        return err
    }
    defer h.Stop()
    ```

=== "Python"

    ```python
    tail = client.logs.tail(
        "app", lambda row: print(row["message"]), filter="level = 'error'", on_error=print
    )
    # later
    tail.stop()
    ```

=== "Java"

    ```java
    TailHandle handle = client.logs().tail("app",
        row -> System.out.println(row.get("message")),
        LogTailOptions.builder().filter("level = 'error'").onError(Throwable::printStackTrace).build());
    // later
    handle.stop();
    ```
