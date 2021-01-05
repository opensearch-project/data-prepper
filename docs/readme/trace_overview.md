# Trace Analytics

In the first release of Data Prepper, we officially  support only one Data Prepper pipeline for the Trace Analytics feature. Below diagram represents the Trace Analytics pipelines,

![Trace Analytics Pipeline](images/TraceAnalyticsFeature.jpg)

<br />
<br />

## OpenTelemetry Trace Source

The OpenTelemetry source accepts trace data from the OpenTelemetry collector. The source depends on [OpenTelemetry Protocol](https://github.com/open-telemetry/opentelemetry-specification/tree/master/specification/protocol). The source officially support transport over gRPC. The source also supports industry-standard encryption (TLS/HTTPS). 

## Preppers

We have two preppers for the Trace Analytics feature,
* *otel_trace_raw_prepper* -  This prepper is responsible for converting the trace data in [OpenTelemetry specification](https://github.com/open-telemetry/opentelemetry-proto/tree/master/opentelemetry/proto/trace/v1) to elasticsearch friendly (JSON) docs. These elasticsearch friendly docs have certain additional fields like duration which are not part of the original OpenTelemetry specification. These additional fields are to make the instant kibana dashboards user-friendly.
* *service_map_stateful* -  This prepper performs the required preprocessing on the trace data and build metadata to display the service-map kibana dashboards.


## Elasticsearch sink

We have a generic sink that writes the data to elasticsearch as the destination. The elasticsearch sink has configuration options related to elasticsearch cluster like endpoint, SSL/Username, index name, index template, index state management, etc. 
For the trace analytics feature, the sink has specific configurations which enables the sink to use indices and index templates specific to this feature. Trace analytics specific elasticsearch indices are,
                                                                                                                                                                 
* *otel-v1-apm-span* -  This index stores the output from otel-trace-raw-prepper. 
* *otel-v1-apm-service-map* - This index stores the output from the service-map-prepper.


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
  prepper:
    - otel_trace_raw_prepper:
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
  prepper:
    - service_map_stateful:
  sink:
    - elasticsearch:
        hosts: ["https://node-0.example.com:9200"]
        cert: "/app/root-ca.pem"
        username: "admin"
        password: "admin"
        trace_analytics_service_map: true
```
