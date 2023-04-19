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

### Basic Usage

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

## List-to-map Processor
A processor that converts a list of objects from an event, where each object has a key field, to a map of keys to objects.

### Basic Usage

To get started with list-to-map processor using Data Prepper, create the following `pipeline.yaml`:
```yaml
pipeline:
  source:
    file:
      path: "/full/path/to/logs_json.log"
      record_type: "event"
      format: "json"
  processor:
    - list_to_map:
        key: "name"
        source: "mylist"
        value_key: "value"
        flatten: true
  sink:
    - stdout:
```

Create the file named `logs_json.log` with the following line and replace the `path` in the file source of your `pipeline.yaml` with the path of this file.

```json
{"mylist":[{"name":"a","value":"val-a"},{"name":"b","value":"val-b1"},{"name":"b","value":"val-b2"},{"name":"c","value":"val-c"}]}
```

> Note that the output json data below is reformatted for readability. The actual output you see will be formatted as a single line.

When run, the processor will parse the message into the following output:
```json
{
  "mylist": [
    {
      "name": "a",
      "value": "val-a"
    },
    {
      "name": "b",
      "value": "val-b1"
    },
    {
      "name": "b",
      "value": "val-b2"
    },
    {
      "name": "c",
      "value": "val-c"
    }
  ],
  "a": "val-a",
  "b": "val-b1",
  "c": "val-c"
}
```

If we set a `target`:
```yaml
    - list_to_map:
        key: "name"
        source: "mylist"
        target: "mymap"
        value_key: "value"
        flatten: true
```
the generated map will be put under the target key:
```json
{
  "mylist": [
    {
      "name": "a",
      "value": "val-a"
    },
    {
      "name": "b",
      "value": "val-b1"
    },
    {
      "name": "b",
      "value": "val-b2"
    },
    {
      "name": "c",
      "value": "val-c"
    }
  ],
  "mymap": {
    "a": "val-a",
    "b": "val-b1",
    "c": "val-c"
  }
}
```

If we do not specify a `value_key`:
```yaml
    - list_to_map:
        key: "name"
        source: "mylist"
        flatten: true
```
the values of the map will be original objects from the source list:
```json
{
  "mylist": [
    {
      "name": "a",
      "value": "val-a"
    },
    {
      "name": "b",
      "value": "val-b1"
    },
    {
      "name": "b",
      "value": "val-b2"
    },
    {
      "name": "c",
      "value": "val-c"
    }
  ],
  "a": {
    "name": "a",
    "value": "val-a"
  },
  "b": {
    "name": "b",
    "value": "val-b1"
  },
  "c": {
    "name": "c",
    "value": "val-c"
  }
}
```

If `flatten` option is not set or set to false:
```yaml
    - list_to_map:
        key: "name"
        source: "mylist"
        value_key: "value"
        flatten: false
```
the values of the map will be lists, and some entries may have more than one element in their values:
```json
{
  "mylist": [
    {
      "name": "a",
      "value": "val-a"
    },
    {
      "name": "b",
      "value": "val-b1"
    },
    {
      "name": "b",
      "value": "val-b2"
    },
    {
      "name": "c",
      "value": "val-c"
    }
  ],
  "a": [
    "val-a"
  ],
  "b": [
    "val-b1",
    "val-b2"
  ],
  "c": [
    "val-c"
  ]
}
```

If we specify `flattened_element` as "last":
```yaml
    - list_to_map:
        key: "name"
        source: "mylist"
        value_key: "value"
        flatten: true
        flattened_element: "last"
```
the last element will be kept:
```json
{
  "mylist": [
    {
      "name": "a",
      "value": "val-a"
    },
    {
      "name": "b",
      "value": "val-b1"
    },
    {
      "name": "b",
      "value": "val-b2"
    },
    {
      "name": "c",
      "value": "val-c"
    }
  ],
  "a": "val-a",
  "b": "val-b2",
  "c": "val-c"
}
```

### Configuration
* `key` - (required) - The key of the fields that will be extracted as keys in the generated map
* `source` - (required) - The key in the event with a list of objects that will be converted to map
* `target` - (optional) - The key of the field that will hold the generated map. If not specified, the generated map will be put in the root.
* `value_key` - (optional) - If specified, the values of the given `value_key` in the objects of the source list will be extracted and put into the values of the generated map; otherwise, original objects in the source list will be put into the values of the generated map.
* `flatten` - (optional) - A boolean value, default to false. If it's false, the values in the generated map will be lists; if it's true, the lists will be flattened into single items.
* `flattened_element` - (optional) - Valid options are "first" and "last", default is "first". This specifies which element, first one or last one, to keep if `flatten` option is true.


## Developer Guide
This plugin is compatible with Java 11 and 17. Refer to the following developer guides for plugin development:
- [Developer Guide](https://github.com/opensearch-project/data-prepper/blob/main/docs/developer_guide.md)
- [Contributing Guidelines](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [Plugin Development](https://github.com/opensearch-project/data-prepper/blob/main/docs/plugin_development.md)
- [Monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
