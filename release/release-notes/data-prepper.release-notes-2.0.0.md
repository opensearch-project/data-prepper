## 2022-10-10 Version 2.0.0

---

### Breaking Changes

* Replaced the `data-prepper-tar-install.sh` script with `bin/data-prepper`
* Replaced the single Jar file with a directory structure and the `bin/data-prepper` script.
* Data Prepper pipelines no longer support `prepper:`. Use `processor:` instead.
* Data Prepper now requires Java 11 or higher to run.
* Removed properties from the `opensearch` sink: `trace_analytics_raw` and `trace_analytics_service_map`. Use the `index_type` instead.
* Renamed two `grok` metric names. Renamed `grokProcessingMatchSuccess` to `grokProcessingMatch` and `grokProcessingMatchFailure` to `grokProcessingMismatch`.
* Removed `record_type` from `otel_trace_source`.
* Removed `otel_trace_raw_prepper` - use `otel_trace_raw` instead.
* Removed `otel_trace_group_prepper` - use `otel_trace_group` instead.
* Removed `peer-forwarder` processor plugin.
* Incorrect HTTP methods were removed from Data Prepper core HTTP APIs.
* All APIs have been renamed to use the `org.opensearch.dataprepper` package.

### Features
* Support conditional routing in sinks ([#1337](https://github.com/opensearch-project/data-prepper/issues/1337))
* Support Core Peer Forwarder which replaces the peer forwarder processor plugin ([#700](https://github.com/opensearch-project/data-prepper/issues/700))
* Support parsing CSV and TSV values in event fields using CSV processor ([#1081](https://github.com/opensearch-project/data-prepper/issues/1081))
* Support decoding CSV or TSV S3 objects using CSV codec in S3 source  ([#1644](https://github.com/opensearch-project/data-prepper/pull/1644))
* Support processing JSON values in event fields using JSON processor ([#831](https://github.com/opensearch-project/data-prepper/issues/831))
* Support multi node aggregation using Core Peer Forwarder ([#978](https://github.com/opensearch-project/data-prepper/issues/978))
* Use peer forwarding in service_map_stateful processor using Core Peer Forwarder ([#1765](https://github.com/opensearch-project/data-prepper/issues/1765))
* Use peer forwarding in otel_trace_raw processor using Core Peer Forwarder ([#1766](https://github.com/opensearch-project/data-prepper/issues/1766))

### Enhancements
* Updates default configurations for `bounded_buffer` - `buffer_size` is 12,800, `batch_size` is 200.
* Support concurrency in Data Prepper expression ([#1189](https://github.com/opensearch-project/data-prepper/issues/1189))
* Support bulk create option in OpnSearch sink ([#1561](https://github.com/opensearch-project/data-prepper/pull/1561))
* Added new metric to track utilization rate of buffer ([#1817](https://github.com/opensearch-project/data-prepper/issues/1817))
* Support publishing metrics for Data Prepper core ([#1789](https://github.com/opensearch-project/data-prepper/issues/1789))
* Support configurable timeouts for processors and sinks to flush the data downstream before shutdown ([#1742](https://github.com/opensearch-project/data-prepper/issues/1742))
* Moved the fields from S3 JSON objects up to the root level of the object to promote consistency with other S3 codecs ([#1687](https://github.com/opensearch-project/data-prepper/issues/1687))
* Support Duration in Data Prepper server configuration ([#1623](https://github.com/opensearch-project/data-prepper/issues/1623))
* Fixed health check bug when Auth is enabled for HTTP and OTel trace source ([#1600](https://github.com/opensearch-project/data-prepper/issues/1600))
* Enabled HTTP health check for OTel trace source and OTel metrics source ([#1546](https://github.com/opensearch-project/data-prepper/issues/1546))
* Made Java 11 to be the baseline version for Data Prepper ([#1422](https://github.com/opensearch-project/data-prepper/issues/1422))
* Updated HTTP source request timeout to use configured timeout ([#975](https://github.com/opensearch-project/data-prepper/issues/975))
* Distribute Data Prepper Docker image with JDK 17 ([#694](https://github.com/opensearch-project/data-prepper/issues/694))
* Support ACM and S3 for TLS/SSL in HTTP source ([#365](https://github.com/opensearch-project/data-prepper/issues/365))
* Updated HTTP methods supported on core API endpoints ([#313](https://github.com/opensearch-project/data-prepper/issues/313))

### Bug Fixes
* Fix a bug where file sink fails to write to output file with multiple pipeline threads ([#1843](https://github.com/opensearch-project/data-prepper/issues/1843))
* Fixed a bug S3 source poll delay ([#1550](https://github.com/opensearch-project/data-prepper/issues/1550))
* Fixed a bug where Data Prepper stops if there's an error reading S3 object ([#1544](https://github.com/opensearch-project/data-prepper/issues/1544))

### Infrastructure
* Improved Data Prepper assemble task to create a runnable distribution ([#1762](https://github.com/opensearch-project/data-prepper/issues/1762))
* Updated end-to-end tests with core peer forwarder for trace tests ([#1866](https://github.com/opensearch-project/data-prepper/pull/1866))
* Support directory structure for Data Prepper ([#305](https://github.com/opensearch-project/data-prepper/issues/305))

### Maintenance
* Removed support for OTLP protocol as internal data transfer in trace pipeline ([#1272](https://github.com/opensearch-project/data-prepper/issues/1272))
* Update Jackson to 2.13.4 ([#1871](https://github.com/opensearch-project/data-prepper/pull/1871))
* Updated Armeria to 1.19.0 ([#1806](https://github.com/opensearch-project/data-prepper/issues/1806))
* Removed Peer Forwarder processor plugin ([#1874](https://github.com/opensearch-project/data-prepper/pull/1874))
* Removed deprecated type property in DataPrepperPlugin annotation ([#1657](https://github.com/opensearch-project/data-prepper/issues/1657))
* Removed deprecated trace_analytics_raw and trace_analytics_service_map values, index_type replaces them ([#1648](https://github.com/opensearch-project/data-prepper/issues/1648))
* Removed deprecated PluginFactory ([#1584](https://github.com/opensearch-project/data-prepper/issues/1584))
* Removed AWS SDK v1 entirely from S3 source ([#1562](https://github.com/opensearch-project/data-prepper/issues/1562))
* Updated Antlr to 4.10.1 ([#1513](https://github.com/opensearch-project/data-prepper/issues/1513))
* Updated all existing prepper plugins to use only processor. ([#647](https://github.com/opensearch-project/data-prepper/issues/647))
* Removed deprecated prepper plugin type from pipeline configuration. ([#619](https://github.com/opensearch-project/data-prepper/issues/619))
* Updated package naming to `org.opensearch` from `com.amazon` ([#344](https://github.com/opensearch-project/data-prepper/issues/344))