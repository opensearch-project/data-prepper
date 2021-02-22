# Service-Map Stateful Prepper

This is a special prepper that consumes Opentelemetry traces, stores them in a MapDB data store and evaluate relationships at fixed ```window_duration```. 
The lmdb databases are stored in the ```data/service-map/*``` path.

# Usages
Example `.yaml` configuration:
```
prepper:
    service-map-stateful:
```

## Configurations

* window_duration(Optional) => An `int` represents the fixed time window in seconds to evaluate service-map relationships. Default is ```180```. 

## Metrics
Besides common metrics in [AbstractPrepper](https://github.com/opendistro-for-elasticsearch/data-prepper/blob/master/data-prepper-api/src/main/java/com/amazon/dataprepper/model/prepper/AbstractPrepper.java), service-map-stateful prepper introduces the following custom metrics.

### Gauge
- `spansDbSize`: measures total spans byte sizes in MapDB across the current and previous window durations.
- `traceGroupDbSize`: measures total trace group byte sizes in MapDB across the current and previous trace group window durations. 

## Developer Guide
This plugin is compatible with Java 8. See 
- [CONTRIBUTING](https://github.com/opendistro-for-elasticsearch/data-prepper/blob/master/CONTRIBUTING.md) 
- [monitoring](https://github.com/opendistro-for-elasticsearch/data-prepper/blob/master/docs/readme/monitoring.md)