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


* `timeout_millis` (Optional): An `int` that specifies the maximum amount of time, in milliseconds, that matching will be performed on an individual Record before it times out and moves on to the next Record.
Setting a `timeout_millis = 0` will make it so that matching a Record never times out. Default value is `30,000`

## Notes on Patterns

The Grok Prepper uses the [java-grok Library](https://github.com/thekrakken/java-grok) internally and supports all java-grok library compatible patterns. The java-grok library is built using the `java.util.regex` regular expression library.

[Default Patterns](https://github.com/thekrakken/java-grok/blob/master/src/main/resources/patterns/patterns)

## Metrics

Counter

* `grokProcessingMatchFailure`: records the number of Records that did not match any of the patterns specified in the match field
  

* `grokProcessingMatchSuccess`: records the number of Records that found at least one pattern match from the match field
  

* `grokProcessingErrors`: records the total number of processing errors for Records


* `grokProcessingTimeouts`: records the total number of Records that timed out while matching

Timer

* `grokProcessingTime`: the time each individual Record takes matching against patterns from `match`. The `avg` is the most useful metric for this Timer.

## Developer Guide
This plugin is compatible with Java 14. See
- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/readme/monitoring.md)
