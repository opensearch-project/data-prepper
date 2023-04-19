# Dead Letter Queues (DLQ)

DLQ Plugins which can be used to offload failed events.

### Available Plugins

* [S3](#S3) 

## S3

An S3 DLQ writer. Failed events will be stored in an S3 bucket.

```
pipeline:
  ...
  sink:
    opensearch:
      dlq:
        s3:
          bucket: "my-dlq-bucket"
          key_path_prefix: "dlq-files/"
          region: "us-west-2"
          sts_role_arn: "arn:aws:iam::123456789012:role/dlq-role"
```

Files written to the S3 DLQ will have the following name pattern. `version` will be the version of Data Prepper. 
`pipelineName` is associated with the pipeline name in the pipeline configuration file. `pluginId` would be the id of the
plugin associated with the DLQ event.

```
dlq-v${version}-${pipelineName}-${pluginId}-${timestampIso8601}-${uniqueId}
```
Example:

```
dlq-v2-apache-log-pipeline-opensearch-2023-04-05T15:26:19.152938Z-e7eb675a-f558-4048-8566-dac15a4f8343
```

The DLQ file will JSON file with an array of failed [DLQ Objects](#DLQ-Objects).

### Configurations

* `bucket`: The bucket name for the DLQ failed output records.
* `key_path_prefix` (Optional) : The key_prefix to use in the S3 bucket.  Defaults to “” . This field supports time value patterns variables like: `/%{yyyy}/%{MM}/%{dd}`. The pattern supports all the symbols that represent one hour or above and are listed in [Java DateTimeFormatter](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html). For example, with a pattern like `/%{yyyy}/%{MM}/%{dd}`, the following key prefix will be used: `/2023/01/24`.
* `region` (Optional) : The AWS region of the S3 Bucket. Defaults to us-east-1.
* `sts_role_arn` (Optional) : The STS role to assume to write to the AWS S3 bucket. Defaults to null, which will use the standard SDK behavior for credentials. The role or credentials used must have S3:PutObject permissions on the configured S3 Bucket.

### Metrics

#### Counter

- `dlqS3RecordsSuccess`: measures number of successful records sent to S3.
- `dlqS3RecordsFailed`: measures number of failed records failed to be sent to S3.
- `dlqS3RequestSuccess`: measures number of successful S3 requests.
- `dlqS3RequestFailed`: measures number of failed S3 requests.

#### Distribution Summary

- `dlqS3RequestSizeBytes`: measures the distribution of the S3 request's payload size in bytes.

#### Timer

- `dlqS3RequestLatency`: measures latency of sending each S3 request including retries.

## DLQ Objects

* `pluginId` : the id of the plugin which resulted in the event being DLQ’ed.
* `pluginName` : the name of the plugin.
* `failedData` : an object with the plugin’s failure options and the failed object. This object is unique to each plugin.
* `pipelineName` : the name of the Data Prepper pipeline where the event failed.
* `timestamp` : the timestamp of the failures is ISO8601
