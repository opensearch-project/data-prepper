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
    - key_value:
  sink:
    - stdout:
```

Create the following file named `logs_json.log` and replace the `path` in the file source of your `pipeline.yaml` with the path of this file.

```json
{"message": "key1=value&key2=value2"}
```

When run, the processor will parse the message into the following output:

```json
{"message": "key1=value&key2=value2", "parsed_message": {"key1": "value1", "key2": "value2"}}
```

## Configuration
* `source` - The field in the message that will be parsed. 
  * Default: `message`
* `destination` - The field the parsed source will be output to. This will overwrite any preexisting data for that key.
  * Default: `parsed_message`
* `field_delimiter_regex` - A regex specifying the delimiter between key/value pairs. Special regex characters such as `[` and `]` must be escaped using `\\`.
  * There is no default.
  * Note: This cannot be defined at the same time as `field_split_characters`
* `field_split_characters` - A string of characters to split between key/value pairs. Special regex characters such as `[` and `]` must be escaped using `\\`.
  * Default: `&`
  * Note: This cannot be defined at the same time as `field_delimiter_regex`
* `include_keys` - An array specifying the keys which should be added to parse. By default, all keys will be added.
  * Default: `[]`
  * Example: `include_keys` is `["key2"]`. `key1=value1&key2=value2` will parse into `{"key2": "value2"}`
* `key_value_delimiter_regex` - A regex specifying the delimiter between a key and a value. Special regex characters such as `[` and `]` must be escaped using `\\`.
  * There is no default.
  * Note: This cannot be defined at the same time as `value_split_characters`
* `value_split_characters` - A string of characters to split between keys and values. Special regex characters such as `[` and `]` must be escaped using `\\`.
  * Default: `=`
  *   * Note: This cannot be defined at the same time as `key_value_delimiter_regex`
* `non_match_value` - When a key/value cannot be successfully split, the key/value will be placed in the key field and the specified value in the value field.
  * Default: `null`
  * Example: `key1value1&key2=value2` will parse into `{"key1value1": null, "key2": "value2"}`
* `prefix` - A prefix given to all keys.
  * Default is an empty string
* `delete_key_regex` - A regex that will be used to delete characters from the key. Special regex characters such as `[` and `]` must be escaped using `\\`.
  * There is no default
  * Non-empty string is the only valid value
  * Example: `delete_key_regex` is `"\s"`. `{"key1 =value1"}` will parse into `{"key1": "value1"}`
* `delete_value_regex` - A regex that will be used to delete characters from the value. Special regex characters such as `[` and `]` must be escaped using `\\`.
  * There is no default
  * Cannot be an empty string
  * Example: `delete_value_regex` is `"\s"`. `{"key1=value1 "}` will parse into `{"key1": "value1"}`
* `transform_key` - Change keys to lowercase, uppercase, or all capitals.
  * Default is an empty string (no transformation)
  * Example: `transform_key` is `lowercase`. `{"Key1=value1"}` will parse into `{"key1": "value1"}`
  * Example: `transform_key` is `uppercase`. `{"key1=value1"}` will parse into `{"Key1": "value1"}`
  * Example: `transform_key` is `capitalize`. `{"key1=value1"}` will parse into `{"KEY1": "value1"}`
* `whitespace` - Specify whether to be lenient or strict with the acceptance of unnecessary whitespace surrounding the configured value-split sequence.
  * Default: `lenient`
  * Example: `whitespace` is `"lenient"`. `{"key1  =  value1"}` will parse into `{"key1  ": "  value1"}`
  * Example: `whitespace` is `"strict"`. `{"key1  =  value1"}` will parse into `{"key1": "value1"}`
* `skip_duplicate_values` - A boolean option for removing duplicate key/value pairs. When set to true, only one unique key/value pair will be preserved.
  * Default: `false`
  * Example: `skip_duplicate_values` is `false`. `{"key1=value1&key1=value1"}` will parse into `{"key1": ["value1", "value1"]}`
  * Example: `skip_duplicate_values` is `true`. `{"key1=value1&key1=value1"}` will parse into `{"key1": "value1"}`
* `remove_brackets` - Specify whether to treat square brackets, angle brackets, and parentheses as value "wrappers" that should be removed from the value.
  * Default: `false`
  * Example: `remove_brackets` is `true`. `{"key1=(value1)"}` will parse into `{"key1": value1}`
  * Example: `remove_brackets` is `false`. `{"key1=(value1)"}` will parse into `{"key1": "(value1)"}`

## Developer Guide
This plugin is compatible with Java 14. See
- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
