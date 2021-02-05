# Trace Analytics

## Introduction

Data Prepper is a key component in providing [Trace Analytics](https://opendistro.github.io/for-elasticsearch-docs/docs/trace/) feature in OpenDistro For Elasticsearch and Amazon Elasticsearch service. Data Prepper is a last mile server-side component which collects telemetry data from [AWS Distro OpenTelemetry collector](https://aws-otel.github.io/docs/getting-started/collector) or [OpenTelemetry collector](https://github.com/open-telemetry/opentelemetry-collector) and transforms it for Elasticsearch.
The transformed trace data is the visualized using the  [Trace Analytics Kibana plugin](https://opendistro.github.io/for-elasticsearch-docs/docs/trace/ta-kibana/), which provides at-a-glance visibility into your application performance, along with the ability to drill down on individual traces. 

Here is how all the components work in trace analytics,
<br />
<br />
![Trace Analytics Pipeline](images/Components.jpg)
<br />
<br />

In your service environment you will have to run AWS Distro OpenTelemetry collector or OpenTelemetry Collector. You can run it as a sidecar or daemonset for EKS, a sidecar for ECS, or an agent on EC2. You should configure the collector to export trace data to Data Prepper. You will then have to deploy Data Prepper as an intermediate component and configure it to send the enriched trace data to your Opendistro For Elasticsearch cluster or Amazon Elasticsearch Service domain. Then using Kibana you can visualize and detect problems in your distributed applications. 


## Data Prepper In Detail

To achieve trace analytics in Data Prepper we have the following  set of pipelines running,

![Trace Analytics Pipeline](images/TraceAnalyticsFeature.jpg)


<br />

### OpenTelemetry Trace Source

The [OpenTelemetry source](../../data-prepper-plugins/otel-trace-source/README.md) accepts trace data from the OpenTelemetry collector. The source depends on [OpenTelemetry Protocol](https://github.com/open-telemetry/opentelemetry-specification/tree/master/specification/protocol). The source officially support transport over gRPC. The source also supports industry-standard encryption (TLS/HTTPS). 

### Processors

We have two processors for the Trace Analytics feature,
* *otel_trace_raw_processor* -  This processor is responsible for converting the trace data in [OpenTelemetry specification](https://github.com/open-telemetry/opentelemetry-proto/tree/master/opentelemetry/proto/trace/v1) to Elasticsearch friendly (JSON) docs. These elasticsearch friendly docs have certain additional fields like duration which are not part of the original OpenTelemetry specification. These additional fields are to make the instant kibana dashboards user-friendly.
* *service_map_stateful* -  This processor performs the required preprocessing on the trace data and build metadata to display the service-map kibana dashboards.


### Elasticsearch sink

We have a generic sink that writes the data to Elasticsearch as the destination. The [elasticsearch sink](../../data-prepper-plugins/elasticsearch/README.md) has configuration options related to elasticsearch cluster like endpoint, SSL/Username, index name, index template, index state management, etc. 
For the trace analytics feature, the sink has specific configurations which enables the sink to use indices and index templates specific to this feature. Trace analytics specific Elasticsearch indices are,
                                                                                                                                                                 
* *otel-v1-apm-span* -  This index stores the output from [otel-trace-raw-processor](../../data-prepper-plugins/otel-trace-raw-processor/README.md). 
* *otel-v1-apm-service-map* - This index stores the output from the [service-map-processor](../../data-prepper-plugins/service-map-stateful/README.md).


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
## Setup
As mentioned in the above we have a specific [pipeline.yml](trace_overview.md#trace-analytics-pipeline-configuration) setup for achieving Trace Analytics in Data Prepper. Below are more detailed links of launching Data Prepper.

##Open distro For Elasticsearch

Please check this [link](https://opendistro.github.io/for-elasticsearch-docs/docs/trace/data-prepper/)  for launching Data Prepper to send data to Open Distro For Elasticsearch docs. 

##AWS Elasticsearch

Please check this [link](../../deployment/aws/README.md)  for launching Data Prepper to send data to AWS Elasticsearch service.
