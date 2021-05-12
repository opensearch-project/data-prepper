## Version 1.0.0.0-beta1 2021-05-11

Compatible with OpenSearch 1.0.0

### Features
  * Additional TraceGroup fields are emitted for enhanced searching and filtering.
  
### Enhancements
  * Revised data-prepper shutdown behavior to limit data loss.
  
### Bug Fixes
  * Fixed service map multi-thread issue by confining ServiceMapStatefulPrepper to single worker.
  * Fixed Index State Management create and attach policy APIs due to the [breaking change](https://github.com/opendistro-for-elasticsearch/index-management/blob/main/release-notes/opendistro-for-elasticsearch-index-management.release-notes-1.13.0.0.md#breaking-changes).
  
### Maintenance
  * Now builds using [version 1.0+](https://github.com/open-telemetry/opentelemetry-specification/pull/1372) of the OpenTelemetry tracing specification.