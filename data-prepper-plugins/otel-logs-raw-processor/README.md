# OTel Logs String Processor 

This is a processor that serializes a collection of `ExportLogsServiceRequest` sent from [otel-logs-source](../dataPrepper-plugins/otel-logs-source) into a collection of string records.

## Usages
Example `.yaml` configuration
```
processor:
    - otel_logs_raw_processor
```

## Metrics
This plugin uses all common metrics in [AbstractProcessor](https://github.com/opensearch-project/data-prepper/blob/main/data-prepper-api/src/main/java/com/amazon/dataprepper/model/processor/AbstractProcessor.java), and does not currently introduce custom metrics.

## Developer Guide
This plugin is compatible with Java 8. See 
- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md) 
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
