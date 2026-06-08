# Multiline Codecs

This plugin provides a multiline input codec for Data Prepper that groups consecutive lines from an input stream into single events based on a configurable regex pattern.

## Usages

The multiline input codec can be configured with source plugins (e.g. S3 source, file source) in the pipeline file.

### Use Cases

- **Java/Kotlin stack traces**: Exception messages followed by `at ...` lines
- **Python tracebacks**: `Traceback` blocks spanning multiple lines
- **Timestamp-prefixed logs**: Logs where each entry starts with a timestamp and continuation lines don't
- **Multi-line JSON/XML in logs**: Structured data embedded across multiple lines within log entries
- **Custom log formats**: Any format where a recognizable pattern marks the start of a new event

## Configuration Options

| Option | Required | Type | Default | Description |
|---|---|---|---|---|
| `match` | Yes | String (regex) | - | A regular expression pattern used to identify line boundaries |
| `negate` | No | Boolean | `false` | When `false`, lines matching the pattern are continuation lines. When `true`, lines NOT matching the pattern are continuation lines |
| `what` | No | String | `previous` | Whether continuation lines belong to the `previous` or `next` event |
| `max_lines` | No | Integer | `500` | Maximum number of lines that can be combined into a single event |
| `max_length` | No | Integer | `10000` | Maximum character length of a combined multiline event |
| `line_separator` | No | String | `\n` | Separator string used when joining lines into a single event message |

## How It Works

The codec reads lines from the input stream and uses the `match` regex to determine event boundaries:

1. **`negate=true` + `what=previous`** (most common): A new event starts when a line matches the pattern. Lines that do NOT match are appended to the preceding event.

2. **`negate=false` + `what=previous`**: Lines that match the pattern are appended to the preceding event.

3. **`negate=true` + `what=next`**: Lines that do NOT match the pattern are prepended to the next matching line.

4. **`negate=false` + `what=next`**: Lines that match the pattern are prepended to the next non-matching line.

## Examples

### Java Stack Traces (timestamp-based grouping)

Each log entry starts with a timestamp. Lines without a timestamp are continuations of the previous entry.

```yaml
pipeline:
  source:
    s3:
      codec:
        multiline:
          match: "^\\d{4}-\\d{2}-\\d{2}"
          negate: true
          what: previous
```

Input:
```
2024-01-01 12:00:00 ERROR NullPointerException
  at com.example.Service.method(Service.java:42)
  at com.example.Main.run(Main.java:10)
2024-01-01 12:00:01 INFO Application recovered
```

Result: 2 events
- Event 1: The ERROR line with its full stack trace grouped together
- Event 2: The INFO line as a single event

### Java Stack Traces (pattern-based grouping)

Lines starting with whitespace followed by `at `, `...`, or `Caused by:` are continuations.

```yaml
pipeline:
  source:
    s3:
      codec:
        multiline:
          match: "^\\s+(at |\\.\\.\\.|Caused by:)"
          negate: false
          what: previous
```

### Python Tracebacks

```yaml
pipeline:
  source:
    s3:
      codec:
        multiline:
          match: "^Traceback|^\\s|^\\w+Error"
          negate: false
          what: previous
```

### Log Entries with Preamble (next mode)

Lines starting with whitespace are prepended to the next non-indented line.

```yaml
pipeline:
  source:
    s3:
      codec:
        multiline:
          match: "^\\s"
          negate: false
          what: next
```

## Developer Guide

This plugin is compatible with Java 11. See below:

- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)

The following command runs the unit and integration tests:

```
./gradlew :data-prepper-plugins:multiline-codecs:test
```
