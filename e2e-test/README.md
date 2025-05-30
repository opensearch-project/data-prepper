# Data Prepper End-to-end Tests

This module includes all e2e tests for data-prepper.

## Running against a specific image

The end-to-end tests can run against a specific remote or local image.

Use the following Gradle build parameters:

* `endToEndDataPrepperImage` - specify the image
* `endToEndDataPrepperTag` - specify the tag

### Running from a released image

This example shows running from the DockerHub image using version 2.11.0.

```shell
./gradlew -PendToEndDataPrepperImage=opensearchproject/data-prepper -PendToEndDataPrepperTag=2.11.0 :e2e-test:log:basicLogEndToEndTest
```

This example shows running from the ECR image using version 2.11.0

```shell
./gradlew -PendToEndDataPrepperImage=public.ecr.aws/opensearchproject/data-prepper -PendToEndDataPrepperTag=2.11.0 :e2e-test:peerforwarder:localAggregateEndToEndTest
```
