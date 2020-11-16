# Trace Analytics

In the first release of SITUP, we will support only one SITUP pipeline for the Trace Analytics feature. Below is design of the Trace Analytics feature pipeline .

![Trace Analytics Pipeline](images/TraceAnalyticsFeature.jpg)

<br />
<br />

## OpenTelemetry Trace Source

The OpenTelemetry source will be accepting trace data from the OpenTelemetry collector. The source will depend on [OpenTelemetry Protocol](https://github.com/open-telemetry/opentelemetry-specification/tree/master/specification/protocol). This would
mean we will support transport over gRPC( In the future HTTP/Proto and HTTP/JSON). The source will support industry-standard encryption (TLS/HTTPS). 

## Processors

We are building two processors for the Trace Analytics feature,
* *otel_trace_raw_processor* -  This processor will be responsible for converting the trace data in [OpenTelemetry specification](https://github.com/open-telemetry/opentelemetry-proto/tree/master/opentelemetry/proto/trace/v1) to elasticsearch friendly (JSON) docs. These elasticsearch friendly docs will have minimal additional fields like duration which are not part of the original specification. These additional fields are to make the instant kibana dashboards user-friendly.
* *service_map_stateful* -  This processor will perform the required preprocessing on the trace data and build metadata to display the service-map kibana dashboards.


## Elasticsearch sink

We will build a generic sink that will write the data to elasticsearch as the destination. The elasticsearch sink will have configuration options related to elasticsearch cluster like endpoint, SSL/Username, index name, index template, index state management, etc. 
For the trace analytics feature, the sink will have specific configurations which will make the sink to use indices and index templates specific to the feature. Trace analytics specific elasticsearch indices are,
                                                                                                                                                                 
* *otel-v1-apm-span* -  This index will store the output from otel-trace-raw-processor. 
* *otel-v1-apm-service-map* - This index will store the output from the service-map-processor.


## Trace Analytics Pipeline Configuration

Below is the pipeline configuration that does trace analytics processing

```
entry-pipeline:
  delay: "100"
  source:
    otel_trace_source:
  sink:
    - pipeline:
        name: "raw-pipeline"
    - pipeline:
        name: "service-map-pipeline"
raw-pipeline:
  source:
    pipeline:
      name: "entry-pipeline"
  processor:
    - otel_trace_raw_processor:
  sink:
    - elasticsearch:
        hosts: [ "https://node-0.example.com:9200" ]
        cert: "/app/root-ca.pem"
        username: "admin"
        password: "admin"
        trace_analytics_raw: true
service-map-pipeline:
  delay: "100"
  source:
    pipeline:
      name: "entry-pipeline"
  processor:
    - service_map_stateful:
  sink:
    - elasticsearch:
        hosts: ["https://node-0.example.com:9200"]
        cert: "/app/root-ca.pem"
        username: "admin"
        password: "admin"
        trace_analytics_service_map: true
```
