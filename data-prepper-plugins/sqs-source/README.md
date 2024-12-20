# SQS Source 

This source allows Data Prepper to use SQS as a source. It reads messages from specified SQS queues and processes them into events.

## Example Configuration

```yaml
sqs-pipeline:
  source:
    sqs:
      queues:
        - url: <SQS_QUEUE_URL_1>
          batch_size: 10
          workers: 1
        - url: <SQS_QUEUE_URL_2>
          batch_size: 10
          workers: 1
      aws:
        region: <AWS_REGION>
        sts_role_arn: <IAM_ROLE_ARN>
  sink:
    - stdout:
