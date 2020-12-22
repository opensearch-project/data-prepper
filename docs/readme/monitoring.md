# Monitoring
Metrics in Data Prepper are instrumented using [Micrometer.io](https://micrometer.io/).
Each plugin (and if needed, each class within a plugin) is responsible for posting relevant 
metrics. Prometheus is used as the metrics backend, and metrics are served from the "/metrics/prometheus"
endpoint.

## Naming
Metrics follow a naming convention of **PIPELINE_NAME_PLUGIN_NAME_METRIC_NAME** . For example, a 
**recordsIn** metric for the **elasticsearch-sink** plugin in a pipeline named **output-pipeline**
would have a qualified name of **output-pipeline_elasticsearch-sink_recordsIn**.

## Serving
Metrics are served from the **metrics/prometheus** endpoint on the Data Prepper server. The format
is a text Prometheus scrape. This port can be used for any frontend which accepts Prometheus metrics.

[Example](https://prometheus.io/docs/visualization/grafana/) setup for Grafana frontend.