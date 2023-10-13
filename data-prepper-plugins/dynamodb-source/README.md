# DynamoDB Source

This is a source plugin that supports retrieve data from DynamoDB tables. Basic use case of this source plugin is to
sync the data from DynamoDB tables to OpenSearch indexes. With this CDC support, customer can run the end to end data
sync pipeline and capture changed data in near real-time without writing any codes and without any downtime of business.
Such pipeline can run on multiple nodes in parallel to support data capture of large scale tables.

This plugin can support below three different modes:

1. Full load only:  One time full data export and load
2. CDC Only:  DynamoDB Stream
3. Full load + CDC:  One time full export and load + DynamoDB Stream.

## Usages

To get started with this DynamoDB source, create the following source configuration:

```yaml
source:
  dynamodb:
    tables:
      - table_arn: "arn:aws:dynamodb:us-west-2:123456789012:table/my-table"
        stream:
          start_position:
        export:
          s3_bucket: "my-bucket"
          s3_prefix: "export/"
    aws:
      region: "us-west-2"
      sts_role_arn: "arn:aws:iam::123456789012:role/DataPrepperRole"

    coordinator:
      dynamodb:
        table_name: "coordinator-demo"
        region: "us-west-2"


```

## Configurations

### Shared Configurations:

* coordinator (Required):  Coordination store setting. This design create a custom coordinator based on existing
  coordination store implementation. Only DynamoDB is tested so far.
* aws (Required):  High level AWS Auth. Note Data Prepper will use the same AWS auth to access all tables, check
  Security for more details.
    * region
    * sts_role_arn

### Export Configurations:

* s3_bucket (Required):  The destination bucket to store the exported data files
* s3_prefix (Optional):  Custom prefix.

### Stream Configurations

* start_position (Optional):  start position of the stream, can be either TRIM_HORIZON or LATEST. If export is required,
  this value will be ignored and set to LATEST by default. This is useful if customer don’t want to run initial export,
  so they can
  choose either from the beginning of the stream (up to 24 hours) or from latest (from the time point when pipeline is
  started)

## Metrics

### Counter

- `exportJobsSuccess`: measures total number of export jobs run with status completed.
- `exportJobsErrors`: measures total number of export jobs cannot be submitted or run with status failed.
- `exportFilesTotal`: measures total number of export files generated.
- `exportFilesSuccess`: measures total number of export files read (till the last line) successfully.
- `exportRecordsTotal`: measures total number of export records generated
- `exportRecordsSuccess`: measures total number of export records processed successfully .
- `exportRecordsErrors`: measures total number of export records processed failed
- `changeEventsSucceeded`: measures total number of changed events in total processed successfully
- `changeEventsFailed`:  measures total number of changed events in total processed failed

## Developer Guide

This plugin is compatible with Java 17. See

- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
