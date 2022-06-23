## 2022-06-23 Version 1.5.0

---

### Features
* Support S3 and SQS as a source of events. ([#251](https://github.com/opensearch-project/data-prepper/issues/251))
* Data Prepper can now report its own metrics with custom tags applied. ([#1415](https://github.com/opensearch-project/data-prepper/issues/1415))
* Support the Embedded Metrics Format (EMF) for reporting Data Prepper's own metrics. ([#1404](https://github.com/opensearch-project/data-prepper/issues/1404))

### Enhancements
* The OpenSearch sink now supports disabling any index management from Data Prepper via the `management-disabled` `index-type`. ([#1051](https://github.com/opensearch-project/data-prepper/issues/1051))
* Add a health check to the HTTP source. ([#1466](https://github.com/opensearch-project/data-prepper/issues/1466))
* Display the port number when starting the HTTP source or OTel trace source. ([#1469](https://github.com/opensearch-project/data-prepper/issues/1469))
* Allow for HTTP decorators in gRPC authentication plugins to give access to the HTTP request. ([#1529](https://github.com/opensearch-project/data-prepper/pull/1529))

### Bug Fixes
* Fix a bug where a null plugin setting throws an exception when attempting to validate that setting. ([#1525](https://github.com/opensearch-project/data-prepper/issues/1525))

### Infrastructure
* Data Prepper supports Docker labels for major version only now. This gets the latest within an entire major version series. ([#1475](https://github.com/opensearch-project/data-prepper/issues/1475))

### Documentation
* Clarified the latest tested version of OpenDistro in the documentation. ([#1494](https://github.com/opensearch-project/data-prepper/issues/1494))

### Maintenance
* Updated Armeria to 1.16.0 ([#924](https://github.com/opensearch-project/data-prepper/issues/924))
* Consolidated all AWS SDK usage on v2 ([#818](https://github.com/opensearch-project/data-prepper/issues/818))
* Fix remaining failing unit tests on Windows ([#970](https://github.com/opensearch-project/data-prepper/issues/970))
* Build on Java 11 using the Java 8 toolchain ([#1406](https://github.com/opensearch-project/data-prepper/issues/1406))
* Support building on JDK 17 ([#1430](https://github.com/opensearch-project/data-prepper/issues/1430))
* Added OpenSearch 2.0.0 to the GitHub Action integration tests ([#1467](https://github.com/opensearch-project/data-prepper/issues/1467))
