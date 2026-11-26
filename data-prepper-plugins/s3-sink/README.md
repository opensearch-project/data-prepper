# S3 sink

The `s3` sink saves batches of events to Amazon Simple Storage Service (Amazon S3) objects.

## Usage

For information on usage, see the [s3 sink documentation](https://opensearch.org/docs/latest/data-prepper/pipelines/configuration/sinks/s3/).

## S3-Compatible Storage

The `s3` sink can write to S3-compatible storage such as MinIO, Ceph, or Garage by configuring a custom endpoint.

```
pipeline:
  ...
  sink:
    - s3:
        aws:
          region: us-east-1
        bucket: my-bucket
        endpoint: "http://minio.example.com:9000"
        force_path_style: true
        legacy_md5_checksum: true
        ...
```

- `endpoint` (Optional) : A string representing a custom S3 endpoint URL to override the default service endpoint. Must start with `http://` or `https://`. When set, cross-region access is disabled.

- `force_path_style` (Optional) : A boolean that enables path-style addressing (`http://endpoint/bucket/key`) instead of virtual-hosted-style addressing (`http://bucket.endpoint/key`). Most S3-compatible storage requires this. Defaults to `false`.

- `legacy_md5_checksum` (Optional) : A boolean that uses legacy MD5 checksums instead of the default integrity protections introduced in AWS SDK 2.30. Enable this for S3-compatible storage that does not support the newer request checksums. Defaults to `false`.

## Developer Guide

See the [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md) guide for general information on contributions.

The integration tests for this plugin do not run as part of the Data Prepper build.

The following command runs the integration tests:

```
./gradlew :data-prepper-plugins:s3-sink:integrationTest -Dtests.s3sink.region=<your-aws-region> -Dtests.s3sink.bucket=<your-bucket>
```

To run the integration tests against S3-compatible storage, add the optional endpoint properties:

```
./gradlew :data-prepper-plugins:s3-sink:integrationTest -Dtests.s3sink.region=us-east-1 -Dtests.s3sink.bucket=<your-bucket> -Dtests.s3sink.endpoint=http://localhost:9000 -Dtests.s3sink.force_path_style=true
```
