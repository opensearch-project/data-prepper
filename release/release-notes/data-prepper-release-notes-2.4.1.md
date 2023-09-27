## 2023-08-29 Version 2.4.1

---

### Bug Fixes
* Fix NullPointerException in S3 scan partition supplier. ([#3323](https://github.com/opensearch-project/data-prepper/pull/3323))
* Fix issue caused by InterruptedException in S3 source where source is polling after pipeline shutdown. ([#3345](https://github.com/opensearch-project/data-prepper/pull/3345))
* Update trace analytics sample app to run again. ([#3353](https://github.com/opensearch-project/data-prepper/pull/3353))

### Maintenance
* Improve logging for failed documents in the OpenSearch sink. ([#3389](https://github.com/opensearch-project/data-prepper/pull/3389))
* Update common-codec to 1.16.0 ([#3370](https://github.com/opensearch-project/data-prepper/pull/3370))
* Update hibernate-validator to 8.0.1.Final. ([#3369](https://github.com/opensearch-project/data-prepper/pull/3369))
* Update trace analytics sample app to run using the latest Spring Boot 3.1.3. ([#3346](https://github.com/opensearch-project/data-prepper/pull/3346))
* Update Gradle to 8.3. ([#3300](https://github.com/opensearch-project/data-prepper/pull/3300))
* Update grpcio from 1.50.0 to 1.53.0. ([#3315](https://github.com/opensearch-project/data-prepper/pull/3315))
* Update certifi from 2022.12.7 to 2023.7.22. ([#3314](https://github.com/opensearch-project/data-prepper/pull/3314))
* Update Bouncy Castle to 1.76. ([#3307](https://github.com/opensearch-project/data-prepper/pull/3307))
* Reduce sleep times in BlockingBufferTests to speed up unit tests. ([#3287](https://github.com/opensearch-project/data-prepper/pull/3287))
* Update checkstyle dependency to 10.12.3. ([#3286](https://github.com/opensearch-project/data-prepper/pull/3286))
* Remove Maxmind license keys from test URLs. ([#3285](https://github.com/opensearch-project/data-prepper/pull/3285))
* Remove unnecessary dependencies in the S3 sink and Parquet codecs. ([#3283](https://github.com/opensearch-project/data-prepper/pull/3283))

### Security
* Update armeria to 1.25.2 to fix CVE-2023-32732, CVE-2023-38493. ([#3366](https://github.com/opensearch-project/data-prepper/pull/3366))
* Fix CVE fixes CVE-2022-36944, WS-2023-0116, CVE-2021-39194, CVE-2023-3635, CVE-2023-36479, CVE-2023-40167. ([#3392](https://github.com/opensearch-project/data-prepper/pull/3392))
* Update commons-compress to 1.24.0 to fix CVE-2023-42503. ([#3388](https://github.com/opensearch-project/data-prepper/pull/3388))
