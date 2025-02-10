# SQS Source 

This source allows Data Prepper to use SQS as a source. It reads messages from specified SQS queues and processes them into events.

## Minimal Configuration

```yaml
sqs-pipeline:
  source:
    sqs:
      queues:
        - url: <SQS_QUEUE_URL_1>
      aws:
        region: <AWS_REGION>
        sts_role_arn: <IAM_ROLE_ARN>
 ```
## Full Configuration

```yaml
sqs-pipeline:
  source:
    sqs:
      queues:
        - url: <SQS_QUEUE_URL_1>
          workers: 2
          maximum_messages: 10
          poll_delay: 0s
          wait_time: 20s
          visibility_timeout: 30s
          visibility_duplication_protection: true
          visibility_duplicate_protection_timeout: "PT1H"
          on_error: "retain_messages"
          codec:
            json:
              key_name: "events"
        - url: <SQS_QUEUE_URL_2>
          # This queue will use the defaults for optional properties.
      acknowledgments: true
      aws:
        region: <AWS_REGION>
        sts_role_arn: <IAM_ROLE_ARN>
```
## Key Features

- **Multi-Queue Support:**  
  Process messages from multiple SQS queues simultaneously.


- **Configurable Polling:**  
  Customize batch size, poll delay, wait time, and visibility timeout per queue.


- **Error Handling:**  
  Use an `on_error` option to control behavior on errors (e.g., delete or retain messages)


- **Codec Support:**  
  Configure codecs (e.g., JSON, CSV, newline-delimited) to parse incoming messages.


## IAM Permissions

Ensure that the SQS queues have the following AWS permissions
- `sqs:ReceiveMessage`  

- `sqs:DeleteMessageBatch`  

- `sqs:ChangeMessageVisibility`
