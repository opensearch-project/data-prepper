## 2022-10-27 Version 1.5.2

---

### Security
* Updated Jackson to 2.13.4.2 which fixes CVE-2022-42003 ([#1925](https://github.com/opensearch-project/data-prepper/pull/1925))
* Updated protobuf-java-util to 3.21.7 which fixes CVE-2022-3171 ([#1938](https://github.com/opensearch-project/data-prepper/pull/1938))
* Updated spring-boot to 2.7.3 which fixes CVE-2022-22965 ([#1691](https://github.com/opensearch-project/data-prepper/pull/1691))

### Maintenance
* Added GZIP compression integration tests for S3 source ([#1577](https://github.com/opensearch-project/data-prepper/pull/1577))
* Updated smoke tests to run against OpenSearch 1.3.6 ([#1955](https://github.com/opensearch-project/data-prepper/pull/1955))
* Updated grpcio to 1.50.0 in smoke tests which takes less time to install ([#1954](https://github.com/opensearch-project/data-prepper/pull/1954))
* Updated spring to 5.3.23 ([#1967](https://github.com/opensearch-project/data-prepper/pull/1967))