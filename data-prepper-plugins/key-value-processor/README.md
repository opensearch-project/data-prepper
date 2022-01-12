# Key Value Processor
This is a processor that takes in a message and parses it into key/value pairs.

## Basic Usage
To get started, create the following `pipeline.yaml`.
```yaml
kv-pipeline:
  source:
    file:
      path: "/full/path/to/logs_json.log"
      record_type: "event"
      format: "json"
  processor:
    - kv:
  sink:
    - stdout:
```

Create the following file named `logs_json.log` and replace the `path` in the file source of your `pipeline.yaml` with the path of this file.

```json
{"message": "key1=value&key2=value2"}
```

When run, the processor will parse the message into the following output:

```json
{"message": "key1=value&key2=value2", "destination": {"key1": "value1", "key2": "value2"}}
```

##Configuration
* `source` - The field in the message that will be parsed. 
  * Default: `message`
* `destination` - The field the parsed source will be output to.
  * Default: `parsed_message`
* `field_delimiter_regex` - A regex specifying the delimiter between key/value pairs.
  * Default: `&`
* `key_value_delimiter_regex` - A regex specifying the delimiter between a key and a value.
  * Default: `=`
* `non_match_value` - When a key/value cannot be successfully split, the key/value will be placed in the key field and the specified value in the value field.
  * Default: `null`
  * Example: `key1value1&key2=value2` will parse into `{"key1value1": null, "key2": "value2"}`
* `prefix` - A prefix given to all keys.
  * Default is an empty string
* `trim_key_regex` - A regex that will be used to trim away characters from the key.
  * There is no default
  * Non-empty string is the only valid value
  * Example: `trim_key_regex` is `"\s"`. `{"key1 =value1"}` will parse into `{"key1": "value1"}`
* `trim_value_regex` - A regex that will be used to trim away characters from the value.
  * There is no default
  * Non-empty string is the only valid value
  * Example: `trim_value_regex` is `"\s"`. `{"key1=value1 "}` will parse into `{"key1": "value1"}`

## Developer Guide
This plugin is compatible with Java 14. See
- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/readme/monitoring.md)
