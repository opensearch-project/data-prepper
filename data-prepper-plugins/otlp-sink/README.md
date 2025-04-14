# X-Ray OTLP Sink

The `otlp_sink` plugin sends span data to [AWS X-Ray](https://docs.aws.amazon.com/xray/) using the OTLP (OpenTelemetry Protocol) format.

## Usage

For information on usage, see the forthcoming documentation in the [Data Prepper Sink Plugins section](https://opensearch.org/docs/latest/data-prepper/pipelines/configuration/sinks/).

A sample pipeline configuration will be added once the plugin is ready for testing.

### Configuration Options

#### aws (Required)
Configuration options for AWS authentication and region settings.

* `region` (Required): The AWS region where X-Ray service is located
    * Must be a valid AWS region identifier (e.g., us-east-1, us-west-2)
    * Cannot be empty

* `sts_role_arn` (Required): AWS STS Role ARN for assuming role-based access
    * Format: arn:aws:iam::{account}:role/{role-name}
    * Length must be between 20 and 2048 characters

* `sts_external_id` (Optional): External ID for additional security when assuming an IAM role
    * Required only if the trust policy requires an external ID
    * Length must be between 2 and 1224 characters

### Sample Pipeline Configuration

```yaml
pipeline:
  source:
    otel_trace_source:
      ssl: true
      
  buffer:
    bounded_blocking:
      buffer_size: 10
      batch_size: 5
      
  sink:
    - otlp:
        aws:
          region: us-west-2
          sts_role_arn: arn:aws:iam::123456789012:role/XrayRole
```
        
## Developer Guide

See the [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md) guide for general information on contributions.

The integration tests for this plugin do not run as part of the main Data Prepper build.

#### Run unit tests locally

```bash
./gradlew :data-prepper-plugins:otlp-sink:test
```

#### Run integration tests locally

```
./gradlew :data-prepper-plugins:otlp-sink:integrationTest
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
      /path/to/data-prepper-plugins/otlp-sink/src/test/resources/pipelines.yaml \
      /path/to/data-prepper-plugins/otlp-sink/src/test/resources/data-prepper-config.yaml
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
  < /path/to/data-prepper-plugins/otlp-sink/src/test/resources/sample-trace.json
```

You should see log output from XRayOTLPSink that confirms the span data was received and parsed correctly.
