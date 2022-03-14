# Mutate String Processors
The following is a list of processors to mutate a string.

## SubstituteStringProcessor
A processor that matches a field value against a regular expression and replaces all matches with a replacement string.

### Basic Usage
To start using Substitute String Processor with Data Prepper, create the following `pipeline.yaml`.

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
          - source: "source"
            from: "from"
            to: "to"
          - source: "source2"
            from: "from2"
            to: "to2"
  sink:
    - stdout:
```

### Configuration
* `entries` - (required) - A list of entries to add to an event
    * `source` - (required) - The entry to be modified
    * `from` - (required) - The string to be replaced
    * `to` - (required) - The string it will be substituted to
    
---

## SplitStringProcessor
A processor that splits a field into an array using a delimiter character.

### Basic Usage
To start using Split String Processor with Data Prepper, create the following `pipeline.yaml`.

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
          - source: "hello,world"
            delimiter: ","
  sink:
    - stdout:
```

### Configuration
* `entries` - (required) - A list of entries to add to an event
    * `source` - (required) - The entry to be split
    * `delimiter` - (required) - The separator character responsible for the split

---

## UppercaseStringProcessor
A processor that converts a string to its uppercase counterpart.

### Basic Usage
To start using Uppercase String Processor with Data Prepper, create the following `pipeline.yaml`.

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

### Configuration
* `with_keys` - (required) - A list of strings to convert to Uppercase

---

## LowercaseStringProcessor
A processor that converts a string to its lowercase counterpart.

### Basic Usage
To start using Lowercase String Processor with Data Prepper, create the following `pipeline.yaml`.

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

### Configuration
* `with_keys` - (required) - A list of strings to convert to Lowercase

---

## TrimStringProcessor
A processor that strips whitespace from a string.

### Basic Usage
To start using Trim String Processor with Data Prepper, create the following `pipeline.yaml`.

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

### Configuration
* `with_keys` - (required) - A list of strings to trim the whitespace from

---

## Developer Guide
This plugin is compatible with Java 14. See
- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)




