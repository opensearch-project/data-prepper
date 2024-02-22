# Lambda Sink

This plugin enables you to send data from your Data Prepper pipeline directly to AWS Lambda functions for further processing.

## Usage
```aidl
lambda-pipeline:
...
  sink:
    - lambda:
        aws:
            region: "us-east-1"
            sts_role_arn: "<arn>"
        function_name: "uploadToS3Lambda"
        max_retries: 3
        batch:
            batch_key: "osi_key"
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
./gradlew :data-prepper-plugins:lambda-sink:integrationTest -Dtests.sink.lambda.region="us-east-1" -Dtests.sink.lambda.functionName="lambda_test_function"  -Dtests.sink.lambda.sts_role_arn="arn:aws:iam::123456789012:role/dataprepper-role

```
