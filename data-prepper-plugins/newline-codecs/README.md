# Newline Delimited JSON Sink/Output Codec

This is an implementation of Newline Sink Codec that parses the Dataprepper Events into Newline rows and writes them into the underlying OutputStream.

## Usages

Newline Output Codec can be configured with sink plugins (e.g. S3 Sink) in the Pipeline file.

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
          newline:
            header_destination: header
            exclude_keys:
              - s3
        buffer_type: in_memory
```

## AWS Configuration

### Codec Configuration:

1) `header_destination`: The key corresponding to which the header value has to be placed by the codec.
2) `exclude_keys`: Those keys of the events that the user wants to exclude while converting them to newline rows.


## Developer Guide

This plugin is compatible with Java 11. See below

- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)

The integration tests for this plugin do not run as part of the Data Prepper build.

The following command runs the integration tests:

```
./gradlew :data-prepper-plugins:s3-sink:integrationTest -Dtests.s3sink.region=<your-aws-region> -Dtests.s3sink.bucket=<your-bucket>
```
