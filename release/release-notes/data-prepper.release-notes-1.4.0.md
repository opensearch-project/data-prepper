## 2022-05-13 Version 1.4.0

---

### Features
* Support metrics ingestion ([#1154](https://github.com/opensearch-project/data-prepper/pull/1154), [#1271](https://github.com/opensearch-project/data-prepper/pull/1271))

### Enhancements
* Migrate Trace Analytics plugins to Event Model ([#1216](https://github.com/opensearch-project/data-prepper/pull/1216), [#1223](https://github.com/opensearch-project/data-prepper/pull/1223), [#1224](https://github.com/opensearch-project/data-prepper/pull/1224), [#1220](https://github.com/opensearch-project/data-prepper/pull/1220), [#1237](https://github.com/opensearch-project/data-prepper/pull/1237), [#1241](https://github.com/opensearch-project/data-prepper/pull/1241), [#1239](https://github.com/opensearch-project/data-prepper/pull/1241))
* Support for OpenSearch 2.0 (previously supported versions of OpenSearch and OpenDistro are still supported)

### Infrastructure
* Added needs-documentation label ([#1373](https://github.com/opensearch-project/data-prepper/pull/1373))
* Upload and publish JUnit test reports for some tests ([#1336](https://github.com/opensearch-project/data-prepper/pull/1336))

### Documentation
* Performance tests documentation ([#1309](https://github.com/opensearch-project/data-prepper/pull/1309))
* Added custom metrics in otel-trace-source ([#1246](https://github.com/opensearch-project/data-prepper/pull/1246))

### Maintenance
* Update to use opensearch-java client instead of Rest High Level Client for bulk requests. ([#1381](https://github.com/opensearch-project/data-prepper/pull/1381))
* OpenSearch build files clean-up ([#1315](https://github.com/opensearch-project/data-prepper/pull/1315))
* Set 30 minute timeout to release process GitHub actions ([#1392](https://github.com/opensearch-project/data-prepper/pull/1392))
* Updated Gradle to 7.4.2 ([#1377](https://github.com/opensearch-project/data-prepper/pull/1377))
* Fix file uploads to S3 with Gradle 7 ([#1383](https://github.com/opensearch-project/data-prepper/pull/1383))
* Updated README links ([#1376](https://github.com/opensearch-project/data-prepper/pull/1376))
* Fix link to NOTICE file ([#1268](https://github.com/opensearch-project/data-prepper/pull/1268))
* Run OpenSearch sink integration tests against more versions of OpenDistro ([#1348](https://github.com/opensearch-project/data-prepper/pull/1348))
* Updated Mockito in OpenSearch plugin ([#1339](https://github.com/opensearch-project/data-prepper/pull/1339))
* Removed OpenSearch build-tools gradle plugin from OpenSearch plugin ([#1327](https://github.com/opensearch-project/data-prepper/pull/1327))
* Use complete url for processor READMEs ([#1324](https://github.com/opensearch-project/data-prepper/pull/1324))

### Refactoring
* Made the BulkRetryStrategyTests less reliant on implementation specifics from OpenSearch ([#1346](https://github.com/opensearch-project/data-prepper/pull/1346))
* Decouple OpenSearchSinkIT from OpenSearch Core test cases ([#1325](https://github.com/opensearch-project/data-prepper/pull/1325))
* Use MatcherAssert.assertThat in OpenSearchSinkIT ([#1323](https://github.com/opensearch-project/data-prepper/pull/1323))
