package codes.grow.packages.openobserve.internal;

import codes.grow.packages.openobserve.core.ValidationException;
import codes.grow.packages.openobserve.logging.LogRecordJson;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

/**
 * Value helpers shared by the OpenObserve service modules: name normalization, SQL quoting, time
 * conversion (OpenObserve uses Unix microseconds) and JSON conversions.
 *
 * <p>Internal API: it may change without notice.
 */
public final class Values {
  /** Time window searched when a log call gives no start (15 minutes). */
  public static final Duration DEFAULT_SEARCH_WINDOW = Duration.ofMinutes(15);

  private Values() {}

  /**
   * Lower-cases a field or stream name and replaces every character other than {@code [a-z0-9_]}
   * with {@code _} (OpenObserve's rule, {@code "Sdk-Mixed.x"} becomes {@code "sdk_mixed_x"}).
   *
   * @param name name
   * @return the normalized name
   */
  public static String normalizeKey(String name) {
    String lower = name.trim().toLowerCase(Locale.ROOT);
    StringBuilder sb = new StringBuilder(lower.length());
    for (int i = 0; i < lower.length(); i++) {
      char c = lower.charAt(i);
      boolean ok = (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_';
      sb.append(ok ? c : '_');
    }
    return sb.toString();
  }

  /**
   * Validates and normalizes a stream argument.
   *
   * @param operation operation name for the error
   * @param stream stream name
   * @return the normalized name
   * @throws ValidationException when the name is null or blank
   */
  public static String stream(String operation, String stream) {
    String s = stream == null ? "" : normalizeKey(stream);
    if (s.isEmpty()) {
      throw new ValidationException(operation + ": stream name is required");
    }
    return s;
  }

  /**
   * Quotes an SQL identifier ({@code "default"} is reserved).
   *
   * @param name identifier
   * @return the quoted identifier
   */
  public static String sqlIdent(String name) {
    return "\"" + name.replace("\"", "\"\"") + "\"";
  }

  /**
   * Quotes an SQL string literal (single quotes doubled).
   *
   * @param text text
   * @return the literal
   */
  public static String sqlString(String text) {
    return "'" + text.replace("'", "''") + "'";
  }

  /**
   * Returns whether a query is raw SQL (starts with {@code SELECT}, case-insensitive).
   *
   * @param query query
   * @return true for SQL
   */
  public static boolean isSql(String query) {
    String q = query.trim();
    return q.length() >= 6 && q.substring(0, 6).equalsIgnoreCase("select");
  }

  /**
   * Converts an instant to Unix microseconds.
   *
   * @param t instant
   * @return microseconds
   */
  public static long micros(Instant t) {
    return Math.addExact(Math.multiplyExact(t.getEpochSecond(), 1_000_000L), t.getNano() / 1_000L);
  }

  /**
   * Converts Unix microseconds to an instant.
   *
   * @param micros microseconds
   * @return the instant
   */
  public static Instant fromMicros(long micros) {
    return Instant.EPOCH.plusNanos(Math.multiplyExact(micros, 1_000L));
  }

  /**
   * Resolves a time window: {@code end} defaults to now, {@code start} to {@code end - fallback}.
   *
   * @param operation operation name for the error
   * @param start start or null
   * @param end end or null
   * @param fallback window length when start is null
   * @return {@code [start, end]}
   * @throws ValidationException when start is after end
   */
  public static Instant[] window(String operation, Instant start, Instant end, Duration fallback) {
    Instant e = end == null ? Instant.now() : end;
    Instant s = start == null ? e.minus(fallback) : start;
    if (s.isAfter(e)) {
      throw new ValidationException(operation + ": start must not be after end");
    }
    return new Instant[] {s, e};
  }

  /**
   * Reads a JSON number or numeric string as a long.
   *
   * @param node node (may be null)
   * @param fallback value when the node is not numeric
   * @return the number
   */
  public static long asLong(JsonNode node, long fallback) {
    if (node == null) {
      return fallback;
    }
    if (node.isNumber()) {
      return node.asLong();
    }
    if (node.isTextual()) {
      try {
        return Long.parseLong(node.asText().trim());
      } catch (NumberFormatException e) {
        return fallback;
      }
    }
    return fallback;
  }

  /**
   * Renders a JSON scalar as text ({@code ""} for null or missing; objects as JSON).
   *
   * @param node node (may be null)
   * @return text
   */
  public static String text(JsonNode node) {
    if (node == null || node.isNull() || node.isMissingNode()) {
      return "";
    }
    return node.isValueNode() ? node.asText() : node.toString();
  }

  /**
   * Converts a JSON object row into an attribute map (longs, doubles, booleans, strings, lists,
   * maps); {@code null} fields are dropped.
   *
   * @param row JSON object
   * @return ordered map
   */
  public static Map<String, Object> row(JsonNode row) {
    Map<String, Object> out = new LinkedHashMap<>();
    if (row != null && row.isObject()) {
      for (Map.Entry<String, JsonNode> e : row.properties()) {
        if (!e.getValue().isNull()) {
          out.put(e.getKey(), LogRecordJson.toAttrValue(e.getValue()));
        }
      }
    }
    return out;
  }

  /**
   * Converts a JSON array of strings into a list (non-text items are rendered as text).
   *
   * @param node array node (may be null)
   * @return list
   */
  public static List<String> strings(JsonNode node) {
    List<String> out = new ArrayList<>();
    if (node != null && node.isArray()) {
      for (JsonNode n : node) {
        out.add(text(n));
      }
    }
    return out;
  }

  /**
   * Compresses bytes with gzip.
   *
   * @param data data
   * @return compressed bytes
   */
  public static byte[] gzip(byte[] data) {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    try (GZIPOutputStream gz = new GZIPOutputStream(buf)) {
      gz.write(data);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return buf.toByteArray();
  }
}
