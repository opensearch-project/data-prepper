# S3 sink

The `s3` sink saves batches of events to Amazon Simple Storage Service (Amazon S3) objects.

## Usage

For information on usage, see the [s3 sink documentation](https://opensearch.org/docs/latest/data-prepper/pipelines/configuration/sinks/s3/).


## Developer Guide

See the [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md) guide for general information on contributions.

The integration tests for this plugin do not run as part of the Data Prepper build.

The following command runs the integration tests:

```
./gradlew :data-prepper-plugins:s3-sink:integrationTest -Dtests.s3sink.region=<your-aws-region> -Dtests.s3sink.bucket=<your-bucket>
```
