# S3 Source

This source ingests data into Data Prepper from [Amazon S3](https://aws.amazon.com/s3/).

See the [`s3` source documentation](https://opensearch.org/docs/latest/data-prepper/pipelines/configuration/sources/s3/) for details on usage.


## Developer Guide

The integration tests for this plugin do not run as part of the Data Prepper build.

The following command runs the integration tests:

```
./gradlew :data-prepper-plugins:s3-source:integrationTest -Dtests.s3source.region=<your-aws-region> -Dtests.s3source.bucket=<your-bucket> -Dtests.s3source.queue.url=<your-queue-url>
```
