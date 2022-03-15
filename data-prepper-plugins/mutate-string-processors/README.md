# Mutate String Processors
The following is a list of processors to mutate a string.

## SubstituteStringProcessor
A processor that matches a key's value against a regular expression and replaces all matches with a replacement string.

### Example
The following `substitute_string` processor example will replace all ':' with '-' for the `message` key.

```yaml
processor:
  - substitute_string:
      entries:
        - source: "message"
          from: ":"
          to: "-"
```
If `from` regex string does not have a match, the key will be returned as it is.

### Configuration
* `entries` - (required) - A list of entries to add to an event
    * `source` - (required) - The key to be modified
    * `from` - (required) - The Regex String to be replaced
    * `to` - (required) - The String to be substituted for each match of `from`
    
---

## SplitStringProcessor
A processor that splits a field into an array using a delimiter character.

### Example
The following `split_string` processor example will split the key `"hello,world"` into `[hello, world]` array.

```yaml
processor:
  - split_string:
      entries:
        - source: "hello,world"
            delimiter: ","
```

### Configuration
* `entries` - (required) - A list of entries to add to an event
    * `source` - (required) - The key to be split
    * `delimiter` - (required) - The separator character responsible for the split

---

## UppercaseStringProcessor
A processor that converts a key to its uppercase counterpart.

### Example
The following `uppercase_string` processor example will convert the key to its uppercase counterpart.

```yaml
processor:
  - uppercase_string:
      with_keys:
        - "uppercaseField"
```

### Configuration
* `with_keys` - (required) - A list of keys to convert to Uppercase

---

## LowercaseStringProcessor
A processor that converts a string to its lowercase counterpart.

### Example
The following `lowercase_string` processor example will convert the key to its lowercase counterpart.

```yaml
processor:
  - lowercase_string:
      with_keys:
        - "lowercaseField"
```

### Configuration
* `with_keys` - (required) - A list of keys to convert to Lowercase

---

## TrimStringProcessor
A processor that strips whitespace from a string.

### Example
The following `trim_string` processor example will strip whitespace.

```yaml
processor:
  - trim_string:
      with_keys:
        - "trimField"
```

### Configuration
* `with_keys` - (required) - A list of keys to trim the whitespace from

---

## Developer Guide
This plugin is compatible with Java 14. See
- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
