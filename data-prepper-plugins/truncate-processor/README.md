# Truncate Processor

This is a processor that truncates key's value at the beginning or at the end or at both sides of a string as per the configuration. If the key's value is a list, then each of the string members of the list are truncated. Non-string members of the list are left untouched. If `truncate_when` option is provided, the truncation of the input is done only when the condition specified is true for the event being processed.

## Basic Usage
To get started, create the following `pipeline.yaml`.
```yaml
pipeline:
  source:
    file:
      path: "/full/path/to/logs_json.log"
      record_type: "event"
      format: "json"
  processor:
    - truncate:
        entries:
          - source_keys: ["message1", "message2"]
            length: 5
          - source_keys: ["info"]
            length: 6
            start_at: 4
          - source_keys: ["log"]
            start_at: 5
  sink:
    - stdout:
```

Create the following file named `logs_json.log` and replace the `path` in the file source of your `pipeline.yaml` with the path of this file.

```json
{"message1": "hello,world", "message2": "test message", "info", "new information", "log": "test log message"}
```
When you run Data Prepper with this `pipeline.yaml`, you should see the following output:

```json
{"message1":"hello", "message2":"test ", "info":"inform", "log": "log message"}
```
where `message1` and `message2` have input values truncated to length 5, starting from index 0, `info` input value truncated to length 6 starting from index 4 and `log` input value truncated at the front by 5 characters.

Example configuration with `truncate_when` option:
```yaml
pipeline:
  source:
    file:
      path: "/full/path/to/logs_json.log"
      record_type: "event"
      format: "json"
  processor:
    - truncate:
        entries:
          - source_keys: ["message"]
            length: 5
            start_at: 8
            truncate_when: '/id == 1'
  sink:
    - stdout:
```

When the pipeline started with the above configuration receives the following two events
```json
{"message": "hello, world", "id": 1}
{"message": "hello, world,not-truncated", "id": 2}
```
the output would be
```json
{"message": "world", "id": 1}
{"message": "hello, world,not-truncated", "id": 2}
```

### Configuration
* `entries` - (required) - A list of entries to add to an event
    * `source_keys` - (required) - The list of key to be modified
    * `truncate_when` - (optional) - a condition, when it is true the truncate operation is performed.
    * `start_at` - (optional) - starting index of the string. Defaults to 0.
    * `length` - (optional) - length of the string after truncation. Defaults to end of the string.
Either `start_at` or `length` or both must be present
