## 2025-08-05 Version 2.12.1

---

### Bug Fixes

* Service Map does not rotate with multiple workers ([#5901](https://github.com/opensearch-project/data-prepper/issues/5901))
* Fixes a regression in core where `@SingleThread` annotated processors are only running the last instance. ([#5904](https://github.com/opensearch-project/data-prepper/pull/5904))
* `geoip` S3 download fails to handle existing files during download ([#5898](https://github.com/opensearch-project/data-prepper/issues/5898))
* `geoip` local file fails to handle existing files during download ([#5899](https://github.com/opensearch-project/data-prepper/issues/5899))

### Security

* CVE-2025-46762 - Parquet 1.15.2 ([#5923](https://github.com/opensearch-project/data-prepper/pull/5923))
* CVE-2025-48734 - commons-beanutils 1.11.0 and Checkstyle 10.26.1 ([#5923](https://github.com/opensearch-project/data-prepper/pull/5923))
* CVE-2024-57699 - json-smart 2.5.2 ([#5923](https://github.com/opensearch-project/data-prepper/pull/5923))
* CVE-2025-24970 - Netty 4.1.123 ([#5923](https://github.com/opensearch-project/data-prepper/pull/5923))
* CVE-2025-27817 - Apache Kafka 3.9.1 and Confluent Kafka 7.9.1 ([#5923](https://github.com/opensearch-project/data-prepper/pull/5923))
* CVE-2024-7254 - protobuf-java 3.25.5 ([#5924](https://github.com/opensearch-project/data-prepper/pull/5924))
* CVE-2025-49146 - Postgresql JDBC driver 42.7.1 ([#5936](https://github.com/opensearch-project/data-prepper/pull/5936))
* CVE-2025-23206 - aws-cdk-lib 2.190.0 ([#5925](https://github.com/opensearch-project/data-prepper/pull/5925))
* CVE-2025-5889 - aws-cdk-lib 2.190.0 ([#5925](https://github.com/opensearch-project/data-prepper/pull/5925))
* CVE-2025-47273 - setuptools v78 ([#5926](https://github.com/opensearch-project/data-prepper/pull/5926))


### Maintenance

* Updated the smoke tests scripts to use the end-to-end tests ([#5913](https://github.com/opensearch-project/data-prepper/pull/5913))
