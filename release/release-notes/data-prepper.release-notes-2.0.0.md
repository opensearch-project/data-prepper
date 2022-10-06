## 2022-10-23 Version 2.0.0

---

### Features
* 
* Support Core Peer Forwarder which replaces the peer forwarder processor plugin ([#700](https://github.com/opensearch-project/data-prepper/issues/700))
* Support conditional routing in sinks ([#1337](https://github.com/opensearch-project/data-prepper/issues/1337))
* Support parsing CSV and TSV values in event fields using CSV processor ([#1081](https://github.com/opensearch-project/data-prepper/issues/1081))
* Support processing JSON values in event fields using JSON processor ([#831](https://github.com/opensearch-project/data-prepper/issues/831))
* Support multi node aggregation using Core Peer Forwarder ([#978](https://github.com/opensearch-project/data-prepper/issues/978))
* Support peer forwarding in service_map_stateful processor ([#1765](https://github.com/opensearch-project/data-prepper/issues/1765))
* Support peer forwarding in otel_trace_raw processor ([#1766](https://github.com/opensearch-project/data-prepper/issues/1766))


### Enhancements
* 
* Support configurable timeouts for processors and sinks to flush the ata downstream before shutdown ([#1742](https://github.com/opensearch-project/data-prepper/issues/1742))
* Support for consistent decoding of s3 events ([#1687](https://github.com/opensearch-project/data-prepper/issues/1687))
* Support Duration in Data Prepper server configuration ([#1623](https://github.com/opensearch-project/data-prepper/issues/1623))
* Enabled HTTP health check for OTel trace source and OTel metrics source ([#1546](https://github.com/opensearch-project/data-prepper/issues/1546))
* Made Java 11 to be the baseline version for Data Prepper ([#1422](https://github.com/opensearch-project/data-prepper/issues/1422))
* Updated HTTP source request timeout to use configured timeout ([#975](https://github.com/opensearch-project/data-prepper/issues/975))
* Distribute Data Prepper Docker image with JDK 17 ([#694](https://github.com/opensearch-project/data-prepper/issues/694))
* Support ACM and S3 for TLS/SSL in HTTP source. ([#365](https://github.com/opensearch-project/data-prepper/issues/365))
* Updated HTTP methods supported on core API endpoints ([#313](https://github.com/opensearch-project/data-prepper/issues/313))

### Bug Fixes
* Fixed health check bug when Auth is enabled for HTTP and OTel trace source ([#1600](https://github.com/opensearch-project/data-prepper/issues/1600))
* Fixed a bug S3 source poll delay ([#1550](https://github.com/opensearch-project/data-prepper/issues/1550))
* Fixed a bug where Data Prepper stops if there's an error reading S3 object. ([#1544](https://github.com/opensearch-project/data-prepper/issues/1544))

### Documentation
* 

### Infrastructure
* Improved Data Prepper assemble task to create a runnable distribution ([1762](https://github.com/opensearch-project/data-prepper/pull/1762))

### Maintenance
* 
* Removed Peer Forwarder processor plugin ([1874](https://github.com/opensearch-project/data-prepper/pull/1874))
* Removed deprecated type property in DataPrepperPlugin annotation ([1657](https://github.com/opensearch-project/data-prepper/pull/1657))
* Removed deprecated trace_analytics_raw and trace_analytics_service_map values. index_type replaces them ([1648](https://github.com/opensearch-project/data-prepper/pull/1648))
* Removed deprecated PluginFactory ([#1584](https://github.com/opensearch-project/data-prepper/issues/1584))
* Removed AWS SDK v1 from S3 source ([#1562](https://github.com/opensearch-project/data-prepper/issues/1562))
* Updated Antlr to 4.10.1 ([#1513](https://github.com/opensearch-project/data-prepper/issues/1513))
* Updated all existing prepper plugins to use only processor. ([#647](https://github.com/opensearch-project/data-prepper/issues/647))
* Removed deprecated prepper plugin type from pipeline configuration. ([#619](https://github.com/opensearch-project/data-prepper/issues/619))
* Updated package naming from `com.amazon` to `org.opensearch` ([#344](https://github.com/opensearch-project/data-prepper/issues/344))
* Support directory structure for Data Prepper ([#305](https://github.com/opensearch-project/data-prepper/issues/305))
* 

