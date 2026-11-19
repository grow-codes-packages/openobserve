package codes.grow.packages.openobserve.logs;

import codes.grow.packages.openobserve.core.Json;
import codes.grow.packages.openobserve.internal.Api;
import codes.grow.packages.openobserve.internal.Values;
import codes.grow.packages.openobserve.logging.TailHandle;
import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.LongFunction;

/**
 * Poll state of one tail subscription, shared by {@link Logs#tail} and {@link
 * OpenObserveLogQuery#tail}. Each poll re-reads the overlap before the cursor, never before the
 * start ({@code floor}), and drops rows already delivered (by content).
 */
final class RowTailer {
  /** Default re-read window before the cursor. */
  static final Duration OVERLAP = Duration.ofSeconds(1);

  /** Default rows per poll request. */
  static final int PAGE_SIZE = 1000;

  private final Api api;
  private final LongFunction<String> sqlFor;
  private final String type;
  private final int pageSize;
  private final long overlap;
  private final Consumer<Map<String, Object>> onRow;
  private final Consumer<Throwable> onError;
  private final long floor;
  private final Map<String, Long> seen = new HashMap<>();
  private long cursor;
  volatile boolean stopped;

  RowTailer(
      Api api,
      LongFunction<String> sqlFor,
      StreamType type,
      int pageSize,
      Duration overlap,
      Instant start,
      Consumer<Map<String, Object>> onRow,
      Consumer<Throwable> onError) {
    this.api = api;
    this.sqlFor = sqlFor;
    this.type = type.wireValue();
    this.pageSize = pageSize;
    this.overlap = overlap.toNanos() / 1000;
    this.floor = Values.micros(start);
    this.cursor = floor;
    this.onRow = onRow;
    this.onError = onError;
  }

  /** Polls every {@code interval} on a daemon thread until the handle is stopped. */
  TailHandle start(Duration interval) {
    ScheduledExecutorService exec =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread th = new Thread(r, "openobserve-tail");
              th.setDaemon(true);
              return th;
            });
    exec.scheduleWithFixedDelay(this::poll, 0, interval.toMillis(), TimeUnit.MILLISECONDS);
    AtomicBoolean done = new AtomicBoolean();
    return () -> {
      if (done.compareAndSet(false, true)) {
        stopped = true;
        exec.shutdownNow();
        try {
          exec.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    };
  }

  void poll() {
    long from = Math.max(floor, cursor - overlap);
    String sql = sqlFor.apply(from);
    long end = Values.micros(Instant.now().plusSeconds(1));
    try {
      for (int offset = 0; !stopped; offset += pageSize) {
        JsonNode body =
            Api.await(
                api.searchAsync("logs.tail", sql, from, end, offset, pageSize, false, type),
                "logs.tail");
        int n = 0;
        for (JsonNode h : body.path("hits")) {
          if (stopped) {
            return;
          }
          n++;
          deliver(Values.row(h));
        }
        if (n < pageSize) {
          break;
        }
      }
    } catch (RuntimeException e) {
      if (!stopped) {
        safe(() -> onError.accept(e));
      }
    }
    long limit = cursor - overlap;
    seen.values().removeIf(ts -> ts < limit);
  }

  void deliver(Map<String, Object> row) {
    long ts = row.get("_timestamp") instanceof Long l ? l : 0L;
    if (ts < floor) {
      return;
    }
    if (seen.putIfAbsent(hash(row), ts) != null) {
      return;
    }
    cursor = Math.max(cursor, ts);
    safe(() -> onRow.accept(row));
  }

  private static void safe(Runnable r) {
    try {
      r.run();
    } catch (RuntimeException ignored) {
      // a failing callback must not stop the tail
    }
  }

  /** Content key of a row (JSON with sorted keys), so equal rows give equal keys. */
  static String hash(Map<String, Object> row) {
    return new String(Json.encode(new TreeMap<>(row)), StandardCharsets.UTF_8);
  }
}
