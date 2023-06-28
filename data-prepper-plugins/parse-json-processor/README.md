# Parse JSON Processor
This is a processor that takes in an Event and parses its JSON data, including any nested fields.
## Basic Usage
To get started, create the following `pipelines.yaml`.
```yaml
parse-json-pipeline:
  source:
    stdin:
  processor:
    - parse_json:
  sink:
    - stdout:
```
#### Basic Example:
If you wish to test the JSON Processor with the above config then you may find the following example useful.
Run the pipeline and paste the following line into your console, and then enter `exit` on a new line.
```
{"outer_key": {"inner_key": "inner_value"}}
```

The processor will parse the message into the following:
```
{"message": {"outer_key": {"inner_key": "inner_value"}}", "outer_key":{"inner_key":"inner_value"}}}
```
#### Example with JSON Pointer:
If you wish to parse a selection of the JSON data, you can specify a JSON Pointer using the `pointer` option in the configuration.
The following configuration file and example demonstrates a basic pointer use case.
```yaml
parse-json-pipeline:
  source:
    stdin:
  processor:
    - parse_json:
        pointer: "outer_key/inner_key"
  sink:
    - stdout:
```
Run the pipeline and paste the following line into your console, and then enter `exit` on a new line.
```
{"outer_key": {"inner_key": "inner_value"}}
```

The processor will parse the message into the following:
```
{"message": {"outer_key": {"inner_key": "inner_value"}}", "inner_key": "inner_value"}
```
## Configuration
* `source` (Optional) — The field in the `Event` that will be parsed.
    * Default: `message`

* `destination` (Optional) — The destination field of the parsed JSON. Defaults to the root of the `Event`.
    * Defaults to writing to the root of the `Event` (The processor will write to root when `destination` is `null`).
    * Cannot be `""`, `/`, or any whitespace-only `String` because these are not valid `Event` fields.

* `pointer` (Optional) — A JSON Pointer to the field to be parsed.
    * There is no `pointer` by default, meaning the entire `source` is parsed.
    * The `pointer` can access JSON Array indices as well.
    * If the JSON Pointer is invalid then the entire `source` data is parsed into the outgoing `Event`.
    * If the pointed-to key already exists in the `Event` and the `destination` is the root, then the entire path of the key will be used.

* `tags_on_failure` (Optional): A `List` of `String`s that specifies the tags to be set in the event the processor fails to parse or an unknown exception occurs while parsing. This tag may be used in conditional expressions in other parts of the configuration

# JSON Sink/Output Codec

This is an implementation of JSON Sink Codec that parses the Dataprepper Events into JSON Objects and writes them into the underlying OutputStream.

## Usages

JSON Output Codec can be configured with sink plugins (e.g. S3 Sink) in the Pipeline file.

## Configuration Options

```
pipeline:
  ...
  sink:
    - s3:
        aws:
          region: us-east-1
          sts_role_arn: arn:aws:iam::123456789012:role/Data-Prepper
          sts_header_overrides:
        max_retries: 5
        bucket: bucket_name
        object_key:
          path_prefix: my-elb/%{yyyy}/%{MM}/%{dd}/
        threshold:
          event_count: 2000
          maximum_size: 50mb
          event_collect_timeout: 15s
       codec:
          json:
            exclude_keys:
              - s3
        buffer_type: in_memory
```

## AWS Configuration

### Codec Configuration:

1) `exclude_keys`: Those keys of the events that the user wants to exclude while converting them to avro records.

## Developer Guide
This plugin is compatible with Java 8 and up. See
- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
