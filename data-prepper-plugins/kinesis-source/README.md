# Kinesis Source

This source ingests data into Data Prepper from [Amazon Kinesis Data Streams](https://aws.amazon.com/kinesis/data-streams/).

See the [`kinesis` source documentation](https://opensearch.org/docs/latest/data-prepper/pipelines/configuration/sources/kinesis/) for details on usage.


## Developer Guide

The integration tests for this plugin do not run as part of the Data Prepper build.

The following command runs the integration tests:

```
./gradlew data-prepper-plugins:kinesis-source:integrationTest -Dtests.kinesis.source.aws.region=<your-aws-region> --tests KinesisSourceIT
```
