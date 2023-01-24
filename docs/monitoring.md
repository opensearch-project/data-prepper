# Monitoring
Metrics in Data Prepper are instrumented using [Micrometer.io](https://micrometer.io/). There are two types of metrics: 
(1) JVM and system metrics; (2) Plugin metrics. Prometheus is used as the default metrics backend.

## JVM and system metrics

JVM and system metrics are Data Prepper instance runtime metrics including metrics on classloaders, memory, 
garbage collection, threads, etc. See https://micrometer.io/docs/ref/jvm for details.

### Naming

JVM and system metrics in Data Prepper follows pre-defined names in Micrometer.io, e.g. `jvm_classes_loaded`, `jvm_memory_used`.

### Serving

By default, metrics are served from the **/metrics/sys** endpoint on the Data Prepper server. The format
is a text Prometheus scrape. This port can be used for any frontend which accepts Prometheus metrics, e.g. 
[Grafana](https://prometheus.io/docs/visualization/grafana/). The configuration can be updated to serve metrics to other 
registries like CloudWatch which does not require/host the endpoint but publishes the metrics directly to cloudwatch.

## Plugin metrics

Each plugin (and if needed, each class within a plugin) is responsible for posting relevant 
metrics. Besides custom metrics introduced by specific plugins, data-prepper-api already introduced the following common metrics for
plugin types.

1. AbstractBuffer
    - Counter
        - `recordsWritten`: number of records written into a buffer.
        - `recordsRead`: number of records read from a buffer.
        - `recordsProcessed`: number of records read from a buffer and marked as processed.
        - `recordsWriteFailed`: number of records failed to be written into a buffer.
        - `writeTimeouts`: count of write timeouts in a buffer.
    - Gauge
        - `recordsInBuffer`: number of records in the buffer.
        - `recordsInFlight`: number of records read from a buffer and being processed by data-prepper downstreams (e.g. processor, sink).
    - Timer
        - `readTimeElapsed`: time elapsed while reading from a buffer
        - `checkpointTimeElapsed`: time elapsed while checkpointing.
2. AbstractProcessor
    - Counter
        - `recordsIn`: number of ingress records into a processor.
        - `recordsOut`: number of egress records from a processor.
    - Timer
        - `timeElapsed`: time elapsed during execution of a processor.
3. AbstractSink
    - Counter
        - `recordsIn`: number of ingress records into a sink.
    - Timer
        - `timeElapsed`: time elapsed during execution of a sink. 

### Naming
Metrics follow a naming convention of **PIPELINE_NAME_PLUGIN_NAME_METRIC_NAME** . For example, a 
**recordsIn** metric for the **opensearch-sink** plugin in a pipeline named **output-pipeline**
would have a qualified name of **output-pipeline_opensearch_sink_recordsIn**.

### Serving
By default, metrics are served from the **metrics/prometheus** endpoint on the Data Prepper server. The format
is a text Prometheus scrape. This port can be used for any frontend which accepts Prometheus metrics. The configuration 
can be updated to serve metrics to other registries like CloudWatch which does not require/host the endpoint but 
publishes the metrics directly to cloudwatch.