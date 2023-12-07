## 2023-12-07 Version 2.6.1

---

### Enhancements
* Add aggregate metrics for ddb source export and stream ([#3728](https://github.com/opensearch-project/data-prepper/pull/3728))

### Bug Fixes
* Update and upsert bulk actions do not include changes from document_root_key, exclude_keys, etc. ([#3745](https://github.com/opensearch-project/data-prepper/issues/3745))
* S3 source processes SQS notification when S3 folder is created ([#3727](https://github.com/opensearch-project/data-prepper/issues/3727))

### Security
* Fix CVE-2023-6378 and CVE-2023-6481 by updating logback to 1.4.14 ([#3729](https://github.com/opensearch-project/data-prepper/issues/3729), [#3817](https://github.com/opensearch-project/data-prepper/issues/3817))
* Require nimbus-jose-jwt 9.37.1 to fix CVE-2021-31684 and CVE-2023-1370 ([#3731](https://github.com/opensearch-project/data-prepper/pull/3731))
* Updates example analytics-service to Spring Boot 3.1.6 fixing CVE-2023-34055 ([#3732](https://github.com/opensearch-project/data-prepper/pull/3732))
