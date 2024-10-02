# DataPrepper Metrics Ingestion from OpenTelemetry Collector

This is an example of using the OpenTelemetry Collector to send metrics data to Data Prepper and then to OpenSearch.
The Data Prepper OTLP/gRPC endpoint is exposed at port 21891. 
The same protocol can be used with the OpenTelemetry Collector, which listens at the OTLP default port 4317.
This setup allows to compare both endpoints.
The Collector will forward any data to Data Prepper for indexing in OpenSearch.

To generate some demo data, the OpenTelemetry Collector uses its host metrics receiver to acquire cpu and memory metrics on the machine it is running on.
Additionally, it scrapes the Prometheus metrics endpoint of the Data Prepper instance.
This also let's you investigate the Data Prepper metrics in OpenSearch.

To run:

1. Run `docker compose up`
2. Wait for everything to come up.
3. Log into OpenSearch Dashboards at <http://localhost:5601> using username `admin` and password `Developer@123`.
4. Create an Index Pattern for index `otel_metrics` choosing `time` as the time field.
5. Inspect the data in the Discovery plugin.

Useful changes and additions:

1. The OpenTelemetry Collector has its [Logging Exporter](https://github.com/open-telemetry/opentelemetry-collector/blob/main/exporter/loggingexporter/README.md) in use. Changing the `loglevel` to `debug` or setting the `verbosity` to `detailed` will log all data to stdout. This is useful for troubleshooting.
2. The OpenTelemetry Collector can push its own metrics to Data Prepper. Follow its documentation in [Internal telemetry](https://opentelemetry.io/docs/collector/internal-telemetry/#use-internal-telemetry-to-monitor-the-collector) for details. These metrics allow comparing the event counts between the Collector and Data Prepper.
3. The OpenTelemetry Collector can be configured to translate between OTLP/HTTP and OTLP/gRPC. It can be used to proxy between sources only capable of OTLP/HTTP and Data Prepper, which only supports OTLP/gRPC.
4. The OpenTelemetry Collector can receive data from the Docker host. It can attach metadata describing the containers. Unfortunately, the required processor does not work with MacOS, so this config was not provided in this example.
