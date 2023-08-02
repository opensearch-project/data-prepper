# CloudWatch Logs Sink

This Data Prepper sink allows the sending of log data to CloudWatch Logs via a CloudWatchLogsClient.

## Usages

The CloudWatch Logs sink should be configured as part of Data Prepper pipeline yaml file.

## Configuration Options

```
pipeline:
  ...
  sink:
    - cloudwatch_logs:
        aws:
          region: us-east-1
          sts_role_arn: arn:aws:iam::123456789012:role/Data-Prepper
          sts_header_overrides:
        log_group: "sample_group"
        log_stream: "sample_stream"
        buffer_type: "in_memory"
        threshold:
          batch_size: 10000
          max_event_size: "256kb"
          max_request_size: "1mb"
          retry_count: 5
          back_off_time: "500ms"
```

## AWS Configuration

- `region` (Optional) : The AWS region to use for credentials. Defaults to [standard SDK behavior to determine the region](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/region-selection.html).

- `sts_role_arn` (Optional) : The AWS STS role to assume for requests to CloudWatchLogs. which will use the [standard SDK behavior for credentials](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials.html). 

- `sts_external_id` (Optional) : The external ID to attach to AssumeRole requests.

## Threshold Configuration

- `batch_size` (Optional) : An integer value that indicates how many events we hold until we make a call to CloudWatch Logs. Defaults to 25.

- `max_event_size` (Optional) : A string representing the max size in bytes of the allowed events. Defaults to "256kb". (Min = "1b", Max = "256 kb")

- `max_request_size` (Optional) : A string representing the count or size of bytes we hold until we make a call to CloudWatch Logs. Default is "1mb". (Min = "1b", Max = "1mb")

- `retry_count` (Optional) : An integer value that indicates the number of retries we make when encountering errors sending logs to CloudWatch Logs. Defaults to 5. (Min = 1, Max = 15)

- `log_send_interval` (Optional) : A string representing the amount of time in seconds between making requests. Defaults to "60s". (Min = "5s", Max = "300s") 

- `back_off_time` (Optional) : A string representing the amount of time in milliseconds between errored transmission re-attempts. Defaults to "500ms". (Min = "500ms", Max = "1000ms")

## Buffer Type Configuration

- `buffer_type` (Optional) : A string representing the type of buffer to use to hold onto events. Currently only supports `in_memory`.

## Plugin Functionality
The cloudwatch_logs sink plugin uses credentials to establish a client to CloudWatch Logs. It currently uses the system
default timestamp for publishing (ex: System.currentTimeMillis()) and also implements an exponential back off strategy
for retransmission. 

## Metrics

### Counters

* `cloudWatchLogsEventsSucceeded` - The number of log events successfully published to CloudWatch Logs.
* `cloudWatchLogsEventsFailed` - The number of log events failed while publishing to CloudWatch Logs.
* `cloudWatchLogsRequestsSucceeded` - The number of log requests successfully made to CloudWatch Logs.
* `cloudWatchLogsRequestsFailed` - The number of log requests failed to reach CloudWatch Logs.

## Developer Guide

This plugin is compatible with Java 11. See below

- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
