# 2021-05-xx Version 1.0.0

## Highlights
* Now builds using version 1.0+ of the OpenTelemetry tracing specification
* Swapped out the Elasticseach rest client with the OpenSearch rest client
* Additional TraceGroup fields are emitted for enhanced searching and filtering with the Trace Analytics Kibana plugin

## Compatibility
This release is backwards incompatible with data emitted by alpha/beta Data Prepper versions.

If your OpenSearch cluster has ingested data from a previous version of Data Prepper, you will need to delete the data before running this release of Data Prepper by:
1. Stopping any existing instances of Data Prepper
2. Deleting the span and service-map indices. This can be done in the OpenSearch Dashboards UI by navigating to Dev Tools via the left sidebar, then running the following commands:
   1. DELETE /otel-v1-apm-span-*
   2. DELETE /otel-v1-apm-service-map
   3. DELETE /_template/otel-v1-apm-span-index-template
3. Starting new instances of Data Prepper 1.0.0
