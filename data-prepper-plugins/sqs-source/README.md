# SQS Source

This source allows Data Prepper to use SQS as a source. It uses SQS for notifications
of which data are new and loads those messages to push out events.

### Example:

The following configuration shows a minimum configuration for reading and Sqs messages and push out events.

```
sqs-pipeline:
  source:
    sqs:
      acknowledgments: true
      queues:
        urls:
          - https://sqs.us-east-1.amazonaws.com/895099421235/MyQueue-1
          - https://sqs.us-east-1.amazonaws.com/895099421235/MyQueue-2
          - https://sqs.us-east-1.amazonaws.com/895099421235/MyQueue-3
        polling_frequency: PT1M
        batch_size: 10
        number_of_threads: 1
      aws:
        region: us-east-1
        role_arn: arn:aws:iam::895099421235:role/test-arn
```

## Configuration Options

All Duration values are a string that represents a duration. They support ISO_8601 notation string ("PT20.345S", "PT15M", etc.) as well as simple notation Strings for seconds ("60s") and milliseconds ("1500ms").

* `queues` (Required) : The SQS configuration. See [SQS Configuration](#sqs_configuration) for details.

* `aws` (Optional) : AWS configurations. See [AWS Configuration](#aws_configuration) for details.

* `acknowledgments` (Optional) : Enables End-to-end acknowledgments. If set to `true`, sqs message is deleted only after all events from the sqs message are successfully acknowledged by all sinks. Default value `false`.

### <a name="sqs_configuration">SQS Configuration</a>

* `urls` (Required) : The SQS queue URL of the queue to read from.
* `number_of_threads` (Optional) : define no of threads for sqs queue processing. default to 1.
* `batch_size` (Optional) : define batch size for sqs messages processing. default to 10.
* `polling_frequency` (Optional) : Duration - A delay to place between reading and processing a batch of SQS messages and making a subsequent request. Defaults to 0 seconds.

### <a name="aws_configuration">AWS Configuration</a>

The AWS configuration is the same for both SQS.

* `region` (Optional) : The AWS region to use for credentials. Defaults to [standard SDK behavior to determine the region](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/region-selection.html).
* `role_arn` (Optional) : The AWS STS role to assume for requests to SQS. Defaults to null, which will use the [standard SDK behavior for credentials](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials.html). 
* `sts_header_overrides` (Optional): A map of header overrides to make when assuming the IAM role for the sink plugin.

## Metrics

* `sqsMessagesReceived` - The number of SQS messages received from the queue by the SQS Source.
* `sqsMessagesDeleted` - The number of SQS messages deleted from the queue by the SQS Source.
* `sqsMessagesFailed` - The number of SQS messages that the SQS Source failed to parse.
* `sqsMessagesDeleteFailed` - The number of SQS messages that the SQS Source failed to delete from the SQS queue.
* `acknowledgementSetCallbackCounter` - The number of SQS messages processed by SQS Source and successfully acknowledge by sink.

## Developer Guide

The integration tests for this plugin do not run as part of the Data Prepper build.

The following command runs the integration tests:

```
./gradlew :data-prepper-plugins:sqs-source:integrationTest -Dtests.sqs.source.aws.region=<your-aws-region> -Dtests.sqs.source.queue.url=<your-queue-url>
```
