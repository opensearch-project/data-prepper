# Log Data Ingestion End-to-end Tests

This module includes e2e tests for log data ingestion supported by data-prepper.

## Basic Grok Ingestion Pipeline End-to-end test

Run from current directory
```
./gradlew :basicLogEndToEndTest
```
or from project root directory
```
./gradlew :e2e-test:log:basicLogEndToEndTest
```

## Basic Grok Ingestion Pipeline with AWS secrets End-to-end test

This test requires AWS secrets `opensearch-sink-basic-credentials` with the basic credentials for opensearch sink to be created in `us-east-1` within the test AWS account. The IAM credentials needs to be passed in through specifying [`AWS_PROFILE` environment variable](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html).

Run from current directory
```
AWS_PROFILE=<your-aws-profile-name> ./gradlew :basicLogWithAwsSecretsEndToEndTest
```
or from project root directory
```
AWS_PROFILE=<your-aws-profile-name> ./gradlew :e2e-test:log:basicLogWithAwsSecretsEndToEndTest
```

## Parallel Grok and string substitute End-to-End test

Run from current directory
```
./gradlew :parallelGrokStringSubstituteTest
```
or from project root directory
```
./gradlew :e2e-test:log:parallelGrokStringSubstituteTest
```
