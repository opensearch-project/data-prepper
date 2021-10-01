# Log HTTP Source

This is a source plugin that supports HTTP protocol. Currently ONLY support Json UTF-8 codec for incoming request, e.g. 
`[{"key1": "value1"}, {"key2": "value2"}]`.


## Usages
Example `.yaml` configuration:
```
source:
    - http:
```

## Configurations

* port(Optional) => An `int` between 0 and 65535 represents the port source is running on. Default is ```2021```.
* request_timeout(Optional) => An `int` larger than 0 represents request timeout in millis. Default is ```10_000```. 
* thread_count(Optional) => An `int` larger than 0 represents the number of threads to keep in the ScheduledThreadPool. Default is `200`.
* max_connection_count(Optional) => An `int` larger than 0 represents the maximum allowed number of open connections. Default is `500`.
* max_pending_requests(Optional) => An `int` larger than 0 represents the maximum allowed number of tasks in the ScheduledThreadPool work queue. Default is `1024`.

## Metrics

TBD

## Developer Guide
This plugin is compatible with Java 14. See 
- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md) 
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
