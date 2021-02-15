# Service-Map Stateful Processor

This is a special processor that consumes Opentelemetry traces, stores them in a MapDB data store and evaluate relationships at fixed ```window_duration```. 
The MapDb databases are stored in the ```data/service-map/*``` path.
```
processor:
    service-map-stateful:
```

## Configurations

* window_duration => In seconds. Default is ```180```. 

