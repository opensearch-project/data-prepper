# X-Ray OTLP Sink

The `xray_otlp_sink` plugin sends span data to [AWS X-Ray](https://docs.aws.amazon.com/xray/) using the OTLP (OpenTelemetry Protocol) format.

## Usage

For information on usage, see the forthcoming documentation in the [Data Prepper Sink Plugins section](https://opensearch.org/docs/latest/data-prepper/pipelines/configuration/sinks/).

A sample pipeline configuration will be added once the plugin is ready for testing.

## Developer Guide

See the [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md) guide for general information on contributions.

The integration tests for this plugin do not run as part of the main Data Prepper build.

#### Run unit tests locally

```bash
./gradlew :data-prepper-plugins:xray-otlp-sink:test
```

#### Run integration tests locally

```
./gradlew :data-prepper-plugins:xray-otlp-sink:integrationTest
```

#### Run a local pipeline that uses this sink

1. Install `grpcurl` – Used to send OTLP span data to the running pipeline.
2. Build the plugin and Data Prepper: 
```
./gradlew build`
```
3. Start the pipeline:
```
cd release/archives/linux/build/install/opensearch-data-prepper-2.11.0-SNAPSHOT-linux-x64

bin/data-prepper \
      /path/to/data-prepper-plugins/xray-otlp-sink/src/test/resources/pipelines.yaml \
      /path/to/data-prepper-plugins/xray-otlp-sink/src/test/resources/data-prepper-config.yaml
```
4. Send test spans to the local pipeline:
```
cd /path/to/opentelemetry-proto

grpcurl -plaintext \
  -import-path . \
  -proto opentelemetry/proto/collector/trace/v1/trace_service.proto \
  -proto opentelemetry/proto/common/v1/common.proto \
  -proto opentelemetry/proto/resource/v1/resource.proto \
  -proto opentelemetry/proto/trace/v1/trace.proto \
  -d @ \
  localhost:21890 \
  opentelemetry.proto.collector.trace.v1.TraceService/Export \
  < /path/to/data-prepper-plugins/xray-otlp-sink/src/test/resources/sample-trace.json
```

You should see log output from XRayOTLPSink that confirms the span data was received and parsed correctly.
