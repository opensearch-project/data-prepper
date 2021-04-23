# Trace Analytics Setup

## OpenTelemetry Collector

As mentioned in the [overview](trace_overview.md#Introduction) you will have to run OpenTelemetry collector in your service environment. You can find the installation guide of OpenTelemetry collector [here](https://opentelemetry.io/docs/collector/getting-started/#getting-started).  Please ensure you that you configure the collector with an exporter configured to the data prepper. Below is an example otel-collector-config.yaml that receives data from various instrumentations and export it to Data Prepper. 

```
receivers:
  jaeger:
    protocols:
      grpc:
  otlp:
    protocols:
      grpc:
  zipkin:

processors:
  batch/traces:
    timeout: 1s
    send_batch_size: 50

exporters:
  otlp/data-prepper:
    endpoint: <data-prepper-address>:21890
    insecure: true

service:
  pipelines:
    traces:
      receivers: [jaeger, otlp, zipkin]
      exporters: [otlp/data-prepper]
```


## Data Prepper Setup
As mentioned in the [overview](trace_overview.md) we have a specific Data Prepper configuration setup for achieving Trace Analytics in Data Prepper. Below is the yaml file that should be used to launch Data Prepper to collect trace data.

```
otel-trace-pipeline:
  # workers is the number of threads processing data in each pipeline. 
  # We recommend same value for all pipelines.
  # default value is 1, set a value based on the machine you are running Data Prepper
  workers: 8 
  # delay in milliseconds is how often the worker threads should process data.
  # Recommend not to change this config as we want the otel-trace-pipeline to process as quick as possible
  # default value is 3_000 ms
  delay: "100" 
  source:
    otel_trace_source:
      ssl: false # Change this to enable encryption in transit
  buffer:
    bounded_blocking:
       # buffer_size is the number of ExportTraceRequest from otel-collector the data prepper should hold in memeory. 
       # We recommend to keep the same buffer_size for all pipelines. 
       # Make sure you configure sufficient heap
       # default value is 512
       buffer_size: 512
       # This is the maximum number of request each worker thread will process within the delay.
       # Default is 8.
       # Make sure buffer_size >= workers * batch_size
       batch_size: 8
  sink:
    - pipeline:
        name: "raw-pipeline"
    - pipeline:
        name: "service-map-pipeline"
raw-pipeline:
  # Configure same as the otel-trace-pipeline
  workers: 8 
  # We recommend using the default value for the raw-pipeline.
  delay: "3000" 
  source:
    pipeline:
      name: "otel-trace-pipeline"
  buffer:
      bounded_blocking:
         # Configure the same value as in otel-trace-pipeline
         # Make sure you configure sufficient heap
         # default value is 512
         buffer_size: 512
         # The raw prepper does bulk request to your OpenSearch sink, so configure the batch_size higher.
         # If you use the recommended otel-collector setup each ExportTraceRequest could contain max 50 spans. https://github.com/opensearch-project/data-prepper/tree/v0.7.x/deployment/aws
         # With 64 as batch size each worker thread could process upto 3200 spans (64 * 50)
         batch_size: 64
  prepper:
    - otel_trace_raw_prepper:
  sink:
    - opensearch:
        hosts: [ "your-es-endpoint" ]
        trace_analytics_raw: true
service-map-pipeline:
  workers: 8
  delay: "100"
  source:
    pipeline:
      name: "otel-trace-pipeline"
  prepper:
    - service_map_stateful:
        # The window duration is the maximum length of time the data prepper stores the most recent trace data to evaluvate service-map relationships. 
        # The default is 3 minutes, this means we can detect relationships between services from spans reported in last 3 minutes.
        # Set higher value if your applications have higher latency. 
        window_duration: 180 
  buffer:
      bounded_blocking:
         # buffer_size is the number of ExportTraceRequest from otel-collector the data prepper should hold in memeory. 
         # We recommend to keep the same buffer_size for all pipelines. 
         # Make sure you configure sufficient heap
         # default value is 512
         buffer_size: 512
         # This is the maximum number of request each worker thread will process within the delay.
         # Default is 8.
         # Make sure buffer_size >= workers * batch_size
         batch_size: 8
  sink:
    - opensearch:
        hosts: [ "your-es-endpoint" ]
        trace_analytics_service_map: true
        # Add aws_sigv4 configuration for Amazon Elasticsearch
        # Add certificates/user cerdentials for Opendistro for Elasticsearch
```

### Sink Setup

In the above trace analytics configuration yaml file, we use opensearch as the sink. This sink could be Amazon Elasticsearch domain or Opendistro for Elasticsearch. You can configure this according to your setup. Please check the [Data Prepper OpenSearch Sink](../../data-prepper-plugins/opensearch/README.md#Configuration) for more details. 


## Getting Started

Please check this [link](../../deployment/aws/README.md) for launching Data Prepper in AWS to send data to AWS Elasticsearch service.


Please check this [link](https://opendistro.github.io/for-elasticsearch-docs/docs/trace/data-prepper/)  for launching Data Prepper to send data to Open Distro For Elasticsearch.

