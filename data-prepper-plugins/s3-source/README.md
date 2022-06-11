# S3 Source

This source allows Data Prepper to use S3 as a source. It uses SQS for notifications
of which S3 objects are new and loads those new objects to parse out events.

_This plugin and its documentation are currently a work-in-progress._

## Developer Guide

The integration tests for this plugin do not run as part of the Data Prepper build.

You can run them via:

```
./gradlew :data-prepper-plugins:s3-source:integrationTest -Dtests.s3source.region=<your-aws-region> -Dtests.s3source.bucket=<your-bucket>
```
