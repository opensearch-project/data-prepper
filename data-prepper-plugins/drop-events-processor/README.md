# Drop Events Processor
This is a processor that drops all messages that are passed into it.

## Basic Usage
To get started, create the following `pipeline.yaml`.
```yaml
drop-pipeline:
  source:
    file:
      path: "/full/path/to/logs_json.log"
      record_type: "event"
      format: "json"
  processor:
    - drop_events:
        drop_when: true
  sink:
    - stdout:
```

Create the following file named `logs_json.log` and replace the `path` in the file source of your `pipeline.yaml` with the path of this 
file.

```
{"message": "127.0.0.1 198.126.12 [10/Oct/2000:13:55:36 -0700] 200"}
```

When run, the processor will filter out and drop all messages.

## Configuration Options

### `drop_when`
**Required**

Accepts a Data Prepper Expression String following the [Data Prepper Expression syntax](../../docs/expression_syntax.md). Configuring 
`drop_events` processor with `drop_when: true` will drop all events received.

### `handle_failed_events`
**Optional**

While evaluating an event if an exception occurs `handle_failed_events` specifies how the exception will be handled
    * `drop` **Default** - The event will be dropped and a warning will be logged.
    * `drop_silently` - The event will be dropped without warning.
    * `skip` - The event will not be dropped and a warning will be logged.
    * `skip_silently` - The event will not be dropped without warning.


## Conditional Drop Events
The `drop_when` parameter can be used to drop selected events. The `drop_when` parameter takes a single String expression. See
[Data Prepper Expression syntax](../../docs/expression_syntax.md) for a complete list of features supported by a Data Prepper Expression.

## Developer Guide
This plugin is compatible with Java 14. See
- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
