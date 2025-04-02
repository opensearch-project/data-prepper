# X-Ray OTLP Sink

The `xray_otlp_sink` plugin sends span data to [AWS X-Ray](https://docs.aws.amazon.com/xray/) using the OTLP (OpenTelemetry Protocol) format.

## Usage

For information on usage, see the forthcoming documentation in the [Data Prepper Sink Plugins section](https://opensearch.org/docs/latest/data-prepper/pipelines/configuration/sinks/).

A sample pipeline configuration will be added once the plugin is ready for testing.

## Developer Guide

See the [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md) guide for general information on contributions.

The integration tests for this plugin do not run as part of the main Data Prepper build.

To run unit tests for this plugin locally:

```bash
./gradlew :data-prepper-plugins:xray-otlp-sink:test
