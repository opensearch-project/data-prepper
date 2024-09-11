
# Lambda Processor

This plugin enables you to send data from your Data Prepper pipeline directly to AWS Lambda functions for further processing.

## Usage
```aidl
lambda-pipeline:
...
  processor:
    - aws_lambda:
        aws:
            region: "us-east-1"
            sts_role_arn: "<arn>"
        function_name: "uploadToS3Lambda"
        max_retries: 3
        invocation_type: "RequestResponse"
        payload_model: "batch_event"
        batch:
            key_name: "osi_key"
            threshold:
                event_count: 10
                event_collect_timeout: 15s
                maximum_size: 3mb
```

`invocation_type` as RequestResponse will be used when the response from aws lambda comes back to dataprepper.
`invocation_type` as Event is used when the response from aws lambda goes to an s3 bucket.

In batch options, an implicit batch threshold option is that if events size is 3mb, we flush it.
`payload_model` this is used to define how the payload should be constructed from a dataprepper event.
`payload_model` as batch_event is used when the output needs to be formed as a batch of multiple events,
if batch option is not mentioned along with payload_model: batch_event , then batch will assume default options as follows:
default batch options:
    batch_key: "events"
    threshold: 
        event_count: 10
        maximum_size: 3mb
        event_collect_timeout: 15s


## Developer Guide

The integration tests for this plugin do not run as part of the Data Prepper build.
The following command runs the integration tests:

```
./gradlew :data-prepper-plugins:aws-lambda:integrationTest -Dtests.processor.lambda.region="us-east-1" -Dtests.processor.lambda.functionName="lambda_test_function"  -Dtests.processor.lambda.sts_role_arn="arn:aws:iam::123456789012:role/dataprepper-role

```


# Lambda Sink

This plugin enables you to send data from your Data Prepper pipeline directly to AWS Lambda functions for further processing.

## Usage
```aidl
lambda-pipeline:
...
  sink:
    - aws_lambda:
        aws:
            region: "us-east-1"
            sts_role_arn: "<arn>"
        function_name: "uploadToS3Lambda"
        max_retries: 3
        batch:
            key_name: "osi_key"
            threshold:
                event_count: 3
                maximum_size: 6mb
                event_collect_timeout: 15s
        dlq:
            s3:
                bucket: test-bucket
                key_path_prefix: dlq/
```

## Developer Guide

The integration tests for this plugin do not run as part of the Data Prepper build.
The following command runs the integration tests:

```
./gradlew :data-prepper-plugins:aws-lambda:integrationTest -Dtests.sink.lambda.region="us-east-1" -Dtests.sink.lambda.functionName="lambda_test_function"  -Dtests.sink.lambda.sts_role_arn="arn:aws:iam::123456789012:role/dataprepper-role

```
