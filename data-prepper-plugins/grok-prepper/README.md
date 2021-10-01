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

* `match` (Optional): A `Map<String, List<String>>` that specifies which fields of a Record to match which patterns against. Default value is `{}`

* `keep_empty_captures` (Optional): A `boolean` that specifies whether `null` captures should be kept. Default value is `false`

* `named_captures_only` (Optional): A `boolean` that specifies whether to only keep named captures. Default value is `true`

## Notes on Patterns

The Grok Prepper uses the [java-grok Library](https://github.com/thekrakken/java-grok) internally and supports all java-grok Library compatible patterns.

[Default Patterns](https://github.com/thekrakken/java-grok/blob/master/src/main/resources/patterns/patterns)

## Metrics

TBD

## Developer Guide
This plugin is compatible with Java 14. See
- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/readme/monitoring.md)
