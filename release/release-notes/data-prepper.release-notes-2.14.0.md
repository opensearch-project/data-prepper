## 2026-02-25 Version 2.14.0

---

### Breaking Changes

* Report `sinkRequestLatency` and `sqsSinkRequestLatency` metrics with time units. If you were previously using the scale of these metrics is now different. ([#6513](https://github.com/opensearch-project/data-prepper/pull/6513), [#6510](https://github.com/opensearch-project/data-prepper/pull/6510))


### Features

* Add `otel_apm_service_map` for application performance monitoring service map along with deriving remote service and operation ([#6482](https://github.com/opensearch-project/data-prepper/issues/6482), ([#6539](https://github.com/opensearch-project/data-prepper/pull/6539))
* Streaming Lambda response support in the Data Prepper Lambda processor ([#5973](https://github.com/opensearch-project/data-prepper/issues/5973))

### Enhancements

* Support for ARM architectures ([#640](https://github.com/opensearch-project/data-prepper/issues/640))
* Enable cross-region writes in the S3 sink. ([#6323](https://github.com/opensearch-project/data-prepper/pull/6323))
* Support files larger than 2GB in S3/SQS ([#5276](https://github.com/opensearch-project/data-prepper/issues/5276))
* RDS source now handles MySql decimal data types when precision value is 19 or higher ([#6339](https://github.com/opensearch-project/data-prepper/issues/6339))
* Make CloudWatchLogs sink retry indefinitely for retryable errors when no DLQ configured ([#6300](https://github.com/opensearch-project/data-prepper/issues/6300))
* Reduce the Data Prepper tar.gz and Docker image sizes ([#3356](https://github.com/opensearch-project/data-prepper/issues/3356))
* Improve Logging for OpenSearch Source when there are no matching indices. ([#6341](https://github.com/opensearch-project/data-prepper/issues/6341))
* Make shutdown timeout configurable to prevent message loss during scale-down ([#6442](https://github.com/opensearch-project/data-prepper/issues/6442))
* Support compressed files in `file` source ([#5245](https://github.com/opensearch-project/data-prepper/issues/5245))


### Bug Fixes

* Flush remaining data to S3 during shutdown ([#6424](https://github.com/opensearch-project/data-prepper/pull/6424))
* Remove usage of buffer accumulator from Kafka custom consumer ([#6357](https://github.com/opensearch-project/data-prepper/pull/6357))

### Security

* protobuf-4.25.8-cp37-abi3-manylinux2014_x86_64.whl: 1 vulnerabilities (highest severity is: 8.6) ([#6441](https://github.com/opensearch-project/data-prepper/issues/6441))
* urllib3-2.5.0-py3-none-any.whl: 2 vulnerabilities (highest severity is: 7.5) - autoclosed ([#6344](https://github.com/opensearch-project/data-prepper/issues/6344))
* werkzeug-3.0.6-py3-none-any.whl: 2 vulnerabilities (highest severity is: 5.3) ([#6326](https://github.com/opensearch-project/data-prepper/issues/6326))


### Maintenance

* OpenSearch 3 Verification ([#6485](https://github.com/opensearch-project/data-prepper/issues/6485))
* Use a different Gradle plugin for Docker ([#5313](https://github.com/opensearch-project/data-prepper/issues/5313))

