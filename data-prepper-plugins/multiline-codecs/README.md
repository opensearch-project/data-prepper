# Multiline Codecs

This plugin provides a multiline input codec for Data Prepper that groups consecutive lines from an input stream into single events based on a configurable regex pattern.

## Usages

The multiline input codec can be configured with source plugins (e.g. S3 source, file source) in the pipeline file.

### Use Cases

- **Java/Kotlin stack traces**: Exception messages followed by `at ...` lines
- **Python tracebacks**: `Traceback` blocks spanning multiple lines
- **Timestamp-prefixed logs**: Logs where each entry starts with a timestamp and continuation lines don't
- **Multi-line JSON/XML in logs**: Structured data embedded across multiple lines within log entries
- **Custom log formats**: Any format where a recognizable pattern marks the start or end of a new event

## Configuration Options

Exactly one of the four pattern fields must be specified:

| Option | Required | Type | Default | Description |
|---|---|---|---|---|
| `event_start_pattern` | One of four | String (regex) | - | A new event begins at each line matching this pattern |
| `event_end_pattern` | One of four | String (regex) | - | An event ends at each line matching this pattern (inclusive) |
| `continuation_line_start_pattern` | One of four | String (regex) | - | Lines matching this pattern are continuations of the previous event |
| `continuation_line_end_pattern` | One of four | String (regex) | - | Lines matching this pattern are prepended to the next event |
| `omit_matched_section` | No | Boolean | `false` | When true, the matched portion of the line is omitted from the output |
| `max_lines` | No | Integer | `500` | Maximum number of lines that can be combined into a single event |
| `max_length` | No | Integer | `10000` | Maximum character length of a combined multiline event. Note: a single line exceeding this limit will still be emitted as a complete event without truncation |
| `line_separator` | No | String | `\n` | Separator string used when joining lines into a single event message. Note: `BufferedReader.readLine()` strips original line endings, so the codec normalizes joined lines using this separator. Set to `""` for no separator |
| `encoding` | No | String | `UTF-8` | Character encoding to use when reading the input stream |

## How It Works

The codec reads lines from the input stream and uses the configured pattern to determine event boundaries:

1. **`event_start_pattern`** (most common): Each line matching the pattern starts a new event. All subsequent non-matching lines are appended to it.

2. **`event_end_pattern`**: Lines are accumulated until a line matches the pattern. The matching line is included in the current event, and the next line starts a new event.

3. **`continuation_line_start_pattern`**: Lines matching the pattern are continuations of the previous event. Non-matching lines start new events.

4. **`continuation_line_end_pattern`**: Lines matching the pattern are prepended to the next non-matching line's event.

## Examples

### Java Stack Traces

Each log entry starts with a timestamp. Lines without a timestamp (stack frames) are part of the previous entry.

```yaml
pipeline:
  source:
    s3:
      codec:
        multiline:
          event_start_pattern: "^\\d{4}-\\d{2}-\\d{2}"
```

Input:
```
2024-01-01 12:00:00 ERROR NullPointerException
  at com.example.Service.method(Service.java:42)
  at com.example.Main.run(Main.java:10)
2024-01-01 12:00:01 INFO Application recovered
```

Result: 2 events (stack trace grouped with its ERROR line)

### Delimiter-Separated Entries

Log entries are separated by a `---` line.

```yaml
pipeline:
  source:
    s3:
      codec:
        multiline:
          event_end_pattern: "^---$"
```

### Stack Traces (continuation pattern)

Lines starting with whitespace followed by `at ` or `Caused by:` are continuations.

```yaml
pipeline:
  source:
    s3:
      codec:
        multiline:
          continuation_line_start_pattern: "^\\s+(at |\\.\\.\\.|Caused by:)"
```

### Omitting Timestamps from Output

Strip the timestamp from each event's first line:

```yaml
pipeline:
  source:
    s3:
      codec:
        multiline:
          event_start_pattern: "^\\d{4}-\\d{2}-\\d{2}\\s+\\d{2}:\\d{2}:\\d{2}\\s+"
          omit_matched_section: true
```

## Developer Guide

This plugin is compatible with Java 11. See below:

- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)

The following command runs the unit and integration tests:

```
./gradlew :data-prepper-plugins:multiline-codecs:test
```
