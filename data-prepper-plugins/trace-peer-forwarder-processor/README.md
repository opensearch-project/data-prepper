# Trace Peer Forwarder Processor

This processor is used to reduce the number of Events that will be forwarded in a Trace Analytics pipeline by half when using [Peer Forwarder](https://github.com/opensearch-project/data-prepper/blob/main/docs/peer_forwarder.md). 
It groups the events based on `trace_id` similar to `service_map` and `otel_traces ` processors. 

In [Trace Analytics pipeline](https://github.com/opensearch-project/data-prepper/blob/main/docs/trace_analytics.md#trace-analytics-pipeline) each event is duplicated, when it is sent from `otel-trace-pipeline` to `raw-pipeline` and `service-map-pipeline`.
So, the event will be forwarded once in each pipeline. Using this processor event will be forwarded only once in `otel-trace-pipeline` to correct peer. 

## Basic Usage
To get started with trace peer forwarder, create the following `pipeline.yaml` along with [Peer Forwarder](https://github.com/opensearch-project/data-prepper/blob/main/docs/peer_forwarder.md) in `data-prepper-config.yaml`

```yaml
otel-trace-pipeline:
  delay: "100"
  source:
    otel_trace_source:
  processor:
    - trace_peer_forwarder:
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
    - otel_traces:
  sink:
    - opensearch:
service-map-pipeline:
  delay: "100"
  source:
    pipeline:
      name: "entry-pipeline"
  processor:
    - service_map:
  sink:
    - opensearch:
```

In the above pipeline, events will be forwarded in the `otel-trace-pipeline` to correct peer and no forwarding will be done in `raw-pipeline` or `service-map-pipeline`.
