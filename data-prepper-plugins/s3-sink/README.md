# S3 Sink

This is the Data Prepper S3 sink plugin that sends records to an S3 bucket via S3Client.

## Usages

The s3 sink should be configured as part of Data Prepper pipeline yaml file.

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
          ndjson:
        buffer_type: in_memory
```

## AWS Configuration

- `region` (Optional) : The AWS region to use for credentials. Defaults to [standard SDK behavior to determine the region](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/region-selection.html).

- `sts_role_arn` (Optional) : The AWS STS role to assume for requests to S3. which will use the [standard SDK behavior for credentials](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials.html). 

- `sts_external_id` (Optional) : The external ID to attach to AssumeRole requests.

- `max_retries` (Optional) : An integer value indicates the maximum number of times that single request should be retired in-order to ingest data to amazon s3. Defaults to `5`.

- `bucket` (Required) : The name of the S3 bucket to write to.

- `object_key` (Optional) : It contains `path_prefix` and `file_pattern`. Defaults to s3 object `events-%{yyyy-MM-dd'T'hh-mm-ss}` inside bucket root directory.

- `path_prefix` (Optional) : path_prefix nothing but directory structure inside bucket in-order to store objects. Defaults to `none`.

## Threshold Configuration

- `event_count` (Required) : An integer value indicates the maximum number of events required to ingest into s3-bucket as part of threshold.

- `maximum_size` (Optional) : A String representing the count or size of bytes required to ingest into s3-bucket as part of threshold. Defaults to `50mb`.

- `event_collect_timeout` (Required) : A String representing how long events should be collected before ingest into s3-bucket as part of threshold. All Duration values are a string that represents a duration. They support ISO_8601 notation string ("PT20.345S", "PT15M", etc.) as well as simple notation Strings for seconds ("60s") and milliseconds ("1500ms").

## Buffer Type Configuration

- `buffer_type` (Optional) : Records stored temporary before flushing into s3 bucket. Possible values are `local_file` and `in_memory`. Defaults to `in_memory`.


## Developer Guide

This plugin is compatible with Java 11. See below

- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)

The integration tests for this plugin do not run as part of the Data Prepper build.

The following command runs the integration tests:

```
./gradlew :data-prepper-plugins:s3-sink:integrationTest -Dtests.s3sink.region=<your-aws-region> -Dtests.s3sink.bucket=<your-bucket>
```
