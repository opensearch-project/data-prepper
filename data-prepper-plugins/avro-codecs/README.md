# Avro codecs

This project provides [Apache Avro](https://avro.apache.org/) support for Data Prepper. It includes an input codec, and output codec, and common libraries which can be used by other projects using Avro.

## Usage

For usage information, see the Data Prepper documentation:

* [S3 source](https://opensearch.org/docs/latest/data-prepper/pipelines/configuration/sources/s3/) 
* [S3 sink](https://opensearch.org/docs/latest/data-prepper/pipelines/configuration/sinks/s3/) 


## Developer Guide

See the [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md) guide for general information on contributions.

The integration tests for this plugin do not run as part of the Data Prepper build.
They are included only with the S3 source or S3 sink for now.

See the README files for those projects for information on running those tests.
