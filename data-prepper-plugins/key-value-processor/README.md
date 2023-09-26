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
* `destination` - The field the parsed source will be output to. This will overwrite any preexisting data for that key. If `destination` is set to `null`, the parsed fields will be written to the root of the event.
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
* `exclude_keys` - An array specifying the parsed keys which should not be added to the event. By default no keys will be excluded.
  * Default: `[]`
  * Example: `exclude_keys` is `["key2"]`. `key1=value1&key2=value2` will parse into `{"key1": "value1"}`
* `default_values` - A hash specifying the default keys and their values which should be added to the event in case these keys do not exist in the source field being parsed.
  * Default: `{}`
  * Example: `default_values` is `{"defaultkey": "defaultvalue"}`. `key1=value1` will parse into `{"key1": "value1", "defaultkey": "defaultvalue"}`
  * If the default key already exists in the message, the value is not changed.
  * Example: `default_values` is `{"key1": "abc"}`. `key1=value1` will parse into `{"key1": "value1"}`
  * It should be noted that the include_keys filter will be applied to the message first, and then default keys.
  * Example: `include_keys` is `["key1"]`, and `default_values` is `{"key2": "value2"}`. `key1=value1&key2=abc` will parse into `{"key1": "value1", "key2": "value2"}`
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
  * Example: `transform_key` is `capitalize`. `{"key1=value1"}` will parse into `{"Key1": "value1"}`
  * Example: `transform_key` is `uppercase`. `{"key1=value1"}` will parse into `{"KEY1": "value1"}`
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
  * In the case of a key-value pair with a brackets and a split character, the splitting will take priority over `remove_brackets=true`. `{"key1=(value1&value2)"}` will parse into `{"key1":"value1","value2)":null}`
* `recursive` - Specify whether to drill down into values and recursively get more key-value pairs from it. The extra key-value pairs will be stored as subkeys of the root key.
  * Default: `false`
  * The levels of recursive parsing must be defined by different brackets for each level: `[]`, `()`, and `<>` in this order.
  * Example: `recursive` is true. `{"item1=[item1-subitem1=item1-subitem1-value&item1-subitem2=(item1-subitem2-subitem2A=item1-subitem2-subitem2A-value&item1-subitem2-subitem2B=item1-subitem2-subitem2B-value)]&item2=item2-value"}` will parse into `"item1": {"item1-subitem1": "item1-subitem1-value", "item1-subitem2": {"item1-subitem2-subitem2A": "item1-subitem2-subitem2A-value", "item1-subitem2-subitem2B": "item1-subitem2-subitem2B-value"}}`
  * Example: `recursive` is false. `{"item1=[item1-subitem1=item1-subitem1-value&item1-subitem2=(item1-subitem2-subitem2A=item1-subitem2-subitem2A-value&item1-subitem2-subitem2B=item1-subitem2-subitem2B-value)]&item2=item2-value"}` will parse into `"item1-subitem2": "(item1-subitem2-subitem2A=item1-subitem2-subitem2A-value", "item2": "item2-value","item1": "[item1-subitem1=item1-subitem1-value", "item1-subitem2-subitem2B": "item1-subitem2-subitem2B-value)]"`
  * Any other configurations specified will only be applied on the OUTER keys.
  * While `recursive` is `true`, `remove_brackets` cannot also be `true`.
  * While `recursive` is `true`, `skip_duplicate_values` will always be `true`.
  * While `recursive` is `true`, `whitespace` will always be `"strict"`.
* `overwrite_if_destination_exists` - Specify whether to overwrite existing fields if there are key conflicts when writing parsed fields to the event. 
  * Default: `true` 

* `tags_on_failure` - When a kv operation causes a runtime exception to be thrown within the processor, the operation is safely aborted without crashing the processor, and the event is tagged with the provided tags.
  * Example: if `tags_on_failure` is set to `["keyvalueprocessor_failure"]`, in the case of a runtime exception, `{"tags": ["keyvalueprocessor_failure"]}` will be added to the event's metadata.

## Developer Guide
This plugin is compatible with Java 14. See
- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
