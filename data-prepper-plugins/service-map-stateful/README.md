# Service-Map Stateful Processor

This is a special processor that consumes Opentelemetry traces, stores them in a MapDB data store and evaluate relationships at fixed ```window_duration```.

> Note: `service_map_stateful` processor has been renamed to `service_map`. You can use either name for now but the support for `service_map_stateful` will be removed in major version 3.0.

# Usages
Example `.yaml` configuration:
```
processor:
    service_map_stateful:
```

## Configurations

* window_duration(Optional) => An `int` represents the fixed time window in seconds to evaluate service-map relationships. Default is ```180```.

## Metrics
Besides common metrics in [AbstractProcessor](https://github.com/opensearch-project/data-prepper/blob/main/data-prepper-api/src/main/java/org/opensearch/dataprepper/model/processor/AbstractProcessor.java), service-map-stateful processor introduces the following custom metrics.

### Gauge
- `spansDbSize`: measures total spans byte sizes in MapDB across the current and previous window durations. This only tracks the byte size for the file (if used).
- `traceGroupDbSize`: measures total trace group byte sizes in MapDB across the current and previous trace group window durations. This only tracks the byte size for the file (if used).
- `spansDbCount`: measures the total spans across the current and previous window durations.
- `traceGroupDbCount`: measures the total trace groups across the current and previous trace group window durations.
- `relationshipCount`: measures the total relationships stored

## Developer Guide
This plugin is compatible with Java 8. See 
- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md) 
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
