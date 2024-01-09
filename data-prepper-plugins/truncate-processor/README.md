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
    - trucate_string:
        entries:
          - source: "message"
            length: 5
  sink:
    - stdout:
```

Create the following file named `logs_json.log` and replace the `path` in the file source of your `pipeline.yaml` with the path of this file.

```json
{"message": "hello,world"}
```
When you run Data Prepper with this `pipeline.yaml`, you should see the following output:

```json
{"message":["hello"]}
```

If the above yaml file has additional config of `start_at: 2`, then the output would be following:

```json
{"message":["llo,w"]}
```

If the above yaml file has additional config of `start_at: 2`, and does not have `length: 5` in the config, then the output would be following:

```json
{"message":["llo,world"]}
```

If the source has an list of strings, then the result will be an array of strings where each of the member of the list is truncated. The following input
```json
{"message": ["hello_one", "hello_two", "hello_three"]}
```
is transformed to the following:

```json
{"message": ["hello", "hello", "hello"]}
```

Example configuration with `truncate_when` option:
```yaml
pipeline:
  source:
    file:
      path: "/full/path/to/logs_json.log"
      record_type: "event"
      format: "json"
  processor:
    - trucate_string:
        entries:
          - source: "message"
            length: 5
            start_at: 7
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
    * `source` - (required) - The key to be modified
    * `start_at` - (optional) - starting index of the string. Defaults to 0.
    * `length` - (optional) - length of the string after truncation. Defaults to end of the string.
Either `start_at` or `length` or both must be present
  
