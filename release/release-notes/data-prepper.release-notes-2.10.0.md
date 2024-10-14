## 2024-10-14 Version 2.10.0

---

### Breaking Changes

### Features

* Kafka source: support SASL/SCRAM mechanisms ([#4241](https://github.com/opensearch-project/data-prepper/issues/4241))
* OpenSearch Bulk API Source ([#248](https://github.com/opensearch-project/data-prepper/issues/248))
* Support AWS Kinesis Data Streams as a Source ([#1082](https://github.com/opensearch-project/data-prepper/issues/1082))

### Enhancements

* Support OpenTelemetry logs in S3 source ([#5028](https://github.com/opensearch-project/data-prepper/issues/5028))
* Use HTML in JsonPropertyDescription instead of Markdown ([#4984](https://github.com/opensearch-project/data-prepper/issues/4984))
* Variable drain time when shutting down via shutdown API ([#4966](https://github.com/opensearch-project/data-prepper/issues/4966))
* Make max connections and acquire timeout configurable on S3 sink client ([#4949](https://github.com/opensearch-project/data-prepper/issues/4949))
* Support BigDecimal data type in expressions ([#4817](https://github.com/opensearch-project/data-prepper/issues/4817))
* Caching implementation of EventKeyFactory ([#4843](https://github.com/opensearch-project/data-prepper/pull/4843))

### Bug Fixes

* [BUG] Close Opensearch RestHighLevelClient in OpenSearchClientRefresher on shutdown and initialization failure ([#4770](https://github.com/opensearch-project/data-prepper/issues/4770))

### Security

* CVE-2024-6345  (High) detected in setuptools-68.0.0-py3-none-any.whl ([#4940](https://github.com/opensearch-project/data-prepper/issues/4940))
* CVE-2023-46136 (High) detected in Werkzeug-2.2.3-py3-none-any.whl ([#4938](https://github.com/opensearch-project/data-prepper/issues/4938))
* CVE-2024-34069 (High) detected in Werkzeug-2.2.3-py3-none-any.whl ([#4938](https://github.com/opensearch-project/data-prepper/issues/4938))
* CVE-2024-37891 (Medium) detected in urllib3-2.0.7-py3-none-any.whl ([#4937](https://github.com/opensearch-project/data-prepper/issues/4937))
* CVE-2024-35195 (Medium) detected in requests-2.31.0-py3-none-any.whl ([#4939](https://github.com/opensearch-project/data-prepper/issues/4939))
* CVE-2024-5569  (Low) detected in zipp-3.15.0-py3-none-any.whl ([#4936]https://github.com/opensearch-project/data-prepper/issues/4936))

### Maintenance

* Fixes and improvements for AbstractSinkTest ([#5021](https://github.com/opensearch-project/data-prepper/pull/5021))
* Update the test logging to include the failed assertion ([#4987](https://github.com/opensearch-project/data-prepper/pull/4987))

