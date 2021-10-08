# Grok Prepper

This is a prepper that takes unstructured data and utilizes pattern matching
to structure and extract important fields for easier and more insightful aggregation and analysis.

## Usages

Example `.yaml` configuration

```
prepper:
  - grok:
     match:
       message: [ "%{COMMONAPACHELOG}" ]
```

## Configuration

TODO: provide examples for using each configuration

* `match` (Optional): A `Map<String, List<String>>` that specifies which keys of a Record to match which patterns against. Default value is `{}`


* `keep_empty_captures` (Optional): A `boolean` that specifies whether `null` captures should be kept. Default value is `false`
  
  
* `named_captures_only` (Optional): A `boolean` that specifies whether to only keep named captures. Default value is `true`
  
  
* `break_on_match` (Optional): A `boolean` that specifies whether to match all patterns from `match` against a Record, 
  or to stop once the first successful pattern match is found. Default value is `true`
  

* `keys_to_overwrite` (Optional): A `List<String>` that specifies which existing keys of a Record to overwrite if there is a capture with the same key value. Default value is `[]`


* `pattern_definitions` (Optional): A `Map<String, String>` that allows for custom pattern use inline. Default value is `{}`


* `patterns_directories` (Optional): A `List<String>` that specifies that path of directories that contain custom pattern files you would like to use. Default value is `[]`


* `patterns_files_glob` (Optional): A glob `String` that describes which pattern files to use from the directories specified for `patterns_directories`. Default value is `*`


* `target_key` (Optional): A `String` that will wrap all captures for a Record in an additional outer key value. Default value is `null`

## Notes on Patterns

The Grok Prepper uses the [java-grok Library](https://github.com/thekrakken/java-grok) internally and supports all java-grok Library compatible patterns.

[Default Patterns](https://github.com/thekrakken/java-grok/blob/master/src/main/resources/patterns/patterns)

## Metrics

TBD

## Developer Guide
This plugin is compatible with Java 14. See
- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/readme/monitoring.md)
