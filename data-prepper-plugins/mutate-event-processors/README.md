# Mutate Event Processors
The following is a list of processors available to mutate an event.

___

## AddEntryProcessor
A processor that adds entries to an event

### Basic Usage
To get started, create the following `pipeline.yaml`.
```yaml
pipeline:
  source:
    file:
      path: "/full/path/to/logs_json.log"
      record_type: "event"
      format: "json"
  processor:
    - add_entries:
        entries:
        - key: "newMessage"
          value: 3
          overwrite_if_key_exists: true
  sink:
    - stdout:
```

Create the following file named `logs_json.log` and replace the `path` in the file source of your `pipeline.yaml` with the path of this file.

```json
{"message": "value"}
```

When run, the processor will parse the message into the following output:

```json
{"message": "value", "newMessage": 3}
```

> If `newMessage` had already existed, its existing value would have been overwritten with `3`

We can also use `format` option to form the value for the new entry from existing entries. For example, if we update the above processor configuration to:
```yaml
  processor:
    - add_entries:
        entries:
        - key: "newMessage"
          format: "new ${message}"
          overwrite_if_key_exists: true
```
then when we run with the same input, the processor will parse the message into the following output:

```json
{"message": "value", "newMessage": "new value"}
```

### Configuration
* `entries` - (required) - A list of entries to add to an event
  * `key` - (required) - The key of the new entry to be added
  * `value` - (optional) - The value of the new entry to be added. Strings, booleans, numbers, null, nested objects, and arrays containing the aforementioned data types are valid to use. Required if `format` is not specified.
  * `format` - (optional) - A format string to use as value of the new entry to be added. For example, `${key1}-${ke2}` where `key1` and `key2` are existing keys in the event. Required if `value` is not specified.
  * `overwrite_if_key_exists` - (optional) - When set to `true`, if `key` already exists in the event, then the existing value will be overwritten. The default is `false`. 

___

## CopyValueProcessor
A processor that copies values within an event

### Basic Usage
To get started, create the following `pipeline.yaml`.
```yaml
pipeline:
  source:
    file:
      path: "/full/path/to/logs_json.log"
      record_type: "event"
      format: "json"
  processor:
    - copy_values:
        entries:
        - from_key: "message"
          to_key: "newMessage"
          overwrite_if_to_key_exists: true
  sink:
    - stdout:
```

Create the following file named `logs_json.log` and replace the `path` in the file source of your `pipeline.yaml` with the path of this file.

```json
{"message": "value"}
```

When run, the processor will parse the message into the following output:

```json
{"message": "value", "newMessage": "value"}
```

> If `newMessage` had already existed, its existing value would have been overwritten with `value`

### Configuration
* `entries` - (required) - A list of entries to be copied in an event
    * `from_key` - (required) - The key of the entry to be copied
    * `to_key` - (required) - The key of the new entry to be added
    * `overwrite_if_to_key_exists` - (optional) - When set to `true`, if `to_key` already exists in the event, then the existing value will be overwritten. The default is `false`.

___

## DeleteEntryProcessor
A processor that deletes entries in an event

### Basic Usage
To get started, create the following `pipeline.yaml`.
```yaml
pipeline:
  source:
    file:
      path: "/full/path/to/logs_json.log"
      record_type: "event"
      format: "json"
  processor:
    - delete_entries:
        with_keys: ["message"]
  sink:
    - stdout:
```

Create the following file named `logs_json.log` and replace the `path` in the file source of your `pipeline.yaml` with the path of this file.

```json
{"message": "value", "message2": "value2"}
```

When run, the processor will parse the message into the following output:

```json
{"message2": "value2"}
```

> If `message` had not existed in the event, then nothing would have happened

### Configuration
* `with_keys` - (required) - An array of keys of the entries to be deleted

___

## RenameKeyProcessor
A processor that renames keys in an event

### Basic Usage
To get started, create the following `pipeline.yaml`.
```yaml
pipeline:
  source:
    file:
      path: "/full/path/to/logs_json.log"
      record_type: "event"
      format: "json"
  processor:
    - rename_keys:
        entries:
        - from_key: "message"
          to_key: "newMessage"
          overwrite_if_to_key_exists: true
  sink:
    - stdout:
```

Create the following file named `logs_json.log` and replace the `path` in the file source of your `pipeline.yaml` with the path of this file.

```json
{"message": "value"}
```

When run, the processor will parse the message into the following output:

```json
{"newMessage": "value"}
```

> If `newMessage` had already existed, its existing value would have been overwritten with `value`

### Configuration
* `entries` - (required) - A list of entries to rename in an event
    * `from_key` - (required) - The key of the entry to be renamed
    * `to_key` - (required) - The new key of the entry
    * `overwrite_if_to_key_exists` - (optional) - When set to `true`, if `to_key` already exists in the event, then the existing value will be overwritten. The default is `false`.

### Special Consideration
The renaming operation occurs in the order defined. This means that chaining is implicit with the RenameKeyProcessor. Take the following `piplines.yaml` for example:
```yaml
pipeline:
  source:
    file:
      path: "/full/path/to/logs_json.log"
      record_type: "event"
      format: "json"
  processor:
    - rename_key:
        entries:
        - from_key: "message"
          to_key: "message2"
        - from_key: "message2"
          to_key: "message3"
  sink:
    - stdout:
```

Let the contents of `logs_json.log` be the following:
```json
{"message": "value"}
```

After the processor runs, this will be the output
```json
{"message3": "value"}
```

___

## ConvertEntryProcessor
A processor that converts the type of value associated with the specified key in a message to the specified type. Basically this is a "casting" processor that changes types of some fields in the event/message.
Some of the data in the input may need to be converted to different types (ex integer or double) for passing the events through "condition" based processors or to do conditional routing.

## Basic Usage

To get started with type conversion processor using Data Prepper, create the following `pipeline.yaml`.
```yaml
type-conv-pipeline:
  source:
    file:
      path: "/full/path/to/logs_json.log"
      record_type: "event"
      format: "json"
  processor:
    - grok:
        match:
          message: ['%{IPORHOST:clientip} \[%{HTTPDATE:timestamp}\] %{NUMBER:response_status}']
    - convert_entry_type:
        key: "response_status"
        type: "integer"
  sink:
    - stdout:
```

Create the following file named `logs_json.log` and replace the `path` in the file source of your `pipeline.yaml` with the path of this file.

```json
{"message": "10.10.10.19 [19/Feb/2015:15:50:36 -0500] 200"}
```

When run, the grok processor will parse the message into the following output:
```json
{"message": "10.10.10.10 [19/Feb/2015:15:50:36 -0500] 200", "clientip":"10.10.10.10", "timestamp": "19/Feb/2015:15:50:36 -0500", "response_status": "200"}
```
and the type conversion processor will change it to the following output, where type of `response_status` value is changed to integer
```json
{"message": "10.10.10.10 [19/Feb/2015:15:50:36 -0500] 200", "clientip":"10.10.10.10", "timestamp": "19/Feb/2015:15:50:36 -0500", "response_status": 200}
```
### Configuration
* `key` - (required) - keys whose value needs to be converted to a different type
* `type` - target type for the value of the key. Possible values are `integer`, `double`, `string`, and `boolean`. Default is `integer`.


## Developer Guide
This plugin is compatible with Java 14. See
- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
