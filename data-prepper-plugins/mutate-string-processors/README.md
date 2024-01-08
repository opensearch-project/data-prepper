# Mutate String Processors
The following is a list of processors to mutate a string.
* [substitute_string](#substitutestringprocessor)
* [split_string](#splitstringprocessor)
* [uppercase_string](#uppercasestringprocessor)
* [lowercase_string](#lowercasestringprocessor)
* [trim_string](#trimstringprocessor)

---
## SubstituteStringProcessor
A processor that matches a key's value against a regular expression and replaces all matches with a replacement string.

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
    - substitute_string:
        entries:
          - source: "message"
            from: ":"
            to: "-"
  sink:
    - stdout:
```

Create the following file named `logs_json.log` and replace the `path` in the file source of your `pipeline.yaml` with the path of this file.

```json
{"message": "ab:cd:ab:cd"}
```
When you run Data Prepper with this `pipeline.yaml`, you should see the following output:

```json
{"message": "ab-cd-ab-cd"}
```
If `from` regex string does not have a match, the key will be returned as it is.

### Configuration
* `entries` - (required) - A list of entries to add to an event
    * `source` - (required) - The key to be modified
    * `from` - (required) - The Regex String to be replaced. Special regex characters such as `[` and `]` 
must be escaped using `\\` when using double quotes and `\ ` when using single quotes. See [here](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/regex/Pattern.html) for more information.
    * `to` - (required) - The String to be substituted for each match of `from`
    
---

## SplitStringProcessor
A processor that splits a field into an array using a delimiter character.

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
    - split_string:
        entries:
          - source: "message"
            delimiter: ","
  sink:
    - stdout:
```

Create the following file named `logs_json.log` and replace the `path` in the file source of your `pipeline.yaml` with the path of this file.

```json
{"message": "hello,world"}
```
When you run Data Prepper with this `pipeline.yaml`, you should see the following output:

```json
{"message":["hello","world"]}
```

### Configuration
* `entries` - (required) - A list of entries to add to an event
    * `source` - (required) - The key to be split
    * `delimiter` - (optional) - The separator character responsible for the split. Cannot be defined at the same time as `delimiter_regex`. At least `delimiter` or `delimiter_regex` must be defined.
    * `delimiter_regex` - (optional) - A regex string responsible for the split. Cannot be defined at the same time as `delimiter`. At least `delimiter` or `delimiter_regex` must be defined.

---

## UppercaseStringProcessor
A processor that converts a key to its uppercase counterpart.

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
    - uppercase_string:
        with_keys:
          - "uppercaseField"
  sink:
    - stdout:
```

Create the following file named `logs_json.log` and replace the `path` in the file source of your `pipeline.yaml` with the path of this file.

```json
{"uppercaseField": "hello"}
```
When you run Data Prepper with this `pipeline.yaml`, you should see the following output:

```json
{"uppercaseField": "HELLO"}
```

### Configuration
* `with_keys` - (required) - A list of keys to convert to Uppercase

---

## LowercaseStringProcessor
A processor that converts a string to its lowercase counterpart.

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
    - lowercase_string:
        with_keys:
          - "lowercaseField"
  sink:
    - stdout:
```

Create the following file named `logs_json.log` and replace the `path` in the file source of your `pipeline.yaml` with the path of this file.

```json
{"lowercaseField": "TESTmeSSage"}
```
When you run Data Prepper with this `pipeline.yaml`, you should see the following output:

```json
{"lowercaseField": "testmessage"}
```

### Configuration
* `with_keys` - (required) - A list of keys to convert to Lowercase

---

## TrimStringProcessor
A processor that strips whitespace from the beginning and end of a key.

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
    - trim_string:
        with_keys:
          - "trimField"
  sink:
    - stdout:
```

Create the following file named `logs_json.log` and replace the `path` in the file source of your `pipeline.yaml` with the path of this file.

```json
{"trimField": " Space Ship "}
```
When you run Data Prepper with this `pipeline.yaml`, you should see the following output:

```json
{"trimField": "Space Ship"}
```

### Configuration
* `with_keys` - (required) - A list of keys to trim the whitespace from

## TruncateStringProcessor
A processor that truncates string by removing user configured number of characters at beginning or at the end or both sides of a string.

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

### Configuration
* `entries` - (required) - A list of entries to add to an event
    * `source` - (required) - The key to be modified
    * `start_at` - (optional) - starting index of the string. Defaults to 0.
    * `length` - (optional) - length of the string after truncation. Defaults to end of the string.
Either `start_at` or `length` or both must be present
  
---

## Developer Guide
This plugin is compatible with Java 14. See
- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
