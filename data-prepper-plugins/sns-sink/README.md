# SNS Sink

This is the Data Prepper SNS Sink plugin that sends records to an SNS Topic.

## Usages

The SNS sink should be configured as part of Data Prepper pipeline yaml file.

## Configuration Options

```
pipeline:
  ...
    sink:
    - sns:
        topic_arn: arn:aws:sns:ap-south-1:524239988922:my-topic
        message_group_id: /type
        message_deduplication_id: /id
        batch_size: 10
        aws:
          region: ap-south-1
          sts_role_arn: arn:aws:iam::524239988922:role/app-test
        dlq:
          s3:
            bucket: test-bucket
            key_path_prefix: dlq/
        codec:
          ndjson:
        max_retries: 5
```

## SNS Pipeline Configuration

- `topic_arn` (Optional) : The SNS Topic Arn of the Topic to push events.

- `batch_size` (Optional) : An integer value indicates the maximum number of events required to ingest into sns topic. Defaults to 10.

- `message_group_id` (optional): A string of message group identifier which is used as `message_group_id` for the message group when it is stored in the sns topic. Default to Auto generated Random key.

- `message_deduplication_id` (Optional) : A string of message deduplication identifier which is used as `message_deduplication_id` for the message deduplication when it is stored in the sns topic. Default to Auto generated Random key.

- `dlq_file`(optional): A String of absolute file path for DLQ failed output records. Defaults to null.
  If not provided, failed records will be written into the default data-prepper log file (`logs/Data-Prepper.log`). If the `dlq` option is present along with this, an error is thrown.

- `dlq` (optional): DLQ configurations. See [DLQ](https://github.com/opensearch-project/data-prepper/tree/main/data-prepper-plugins/failures-common/src/main/java/org/opensearch/dataprepper/plugins/dlq/README.md) for details. If the `dlq_file` option is present along with this, an error is thrown.

- `codec` : This plugin is integrated with sink codec

- `aws` (Optional) : AWS configurations. See [AWS Configuration](#aws_configuration) for details. SigV4 is enabled by default when this option is used. If this option is present, `aws_` options are not expected to be present. If any of `aws_` options are present along with this, error is thrown.

- `max_retries` (Optional) : An integer value indicates the maximum number of times that single request should be retired in-order to ingest data to amazon SNS and S3. Defaults to `5`.

### <a name="aws_configuration">AWS Configuration</a>

* `region` (Optional) : The AWS region to use for credentials. Defaults to [standard SDK behavior to determine the region](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/region-selection.html).
* `sts_role_arn` (Optional) : The AWS STS role to assume for requests to SNS and S3. Defaults to null, which will use the [standard SDK behavior for credentials](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials.html).


### Counters

* `snsSinkObjectsEventsSucceeded` - The number of events that the SNS sink has successfully sent to Topic.
* `snsSinkObjectsEventsFailed` - The number of events that the SNS sink has successfully sent to Topic.

## Developer Guide

This plugin is compatible with Java 11. See below

- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)

The integration tests for this plugin do not run as part of the Data Prepper build.

The following command runs the integration tests:

Note: Subscribe sns topic to sqs queues to run the integration tests.

```
./gradlew :data-prepper-plugins:sns-sink:integrationTest -Dtests.sns.sink.region=<<aws-region>> -Dtests.sns.sink.sts.role.arn=<<aws-sts-role-arn>> -Dtests.sns.sink.standard.topic=<<standard-topic-arn>> -Dtests.sns.sink.fifo.topic=<<fifo-topic-arn>> -Dtests.sns.sink.dlq.file.path=<<dlq-file-path>> -Dtests.sns.sink.standard.sqs.queue.url=<<sqs-standard-queue>> -Dtests.sns.sink.fifo.sqs.queue.url=<<sqs-fifo-queue>>
```
