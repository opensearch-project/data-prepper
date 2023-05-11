## 2023-05-09 Version 2.2.1

---

### Enhancements
* Log full errors when OpenSearch sink fails to start ([#2565](https://github.com/opensearch-project/data-prepper/issues/2565))
* Increase backoff delays in S3 source ([#2568](https://github.com/opensearch-project/data-prepper/issues/2568))
* Add object filter patterns for Core Peer Forwarder ([#2576](https://github.com/opensearch-project/data-prepper/issues/2576))
* Improve logging when OpenSearch sink fails to push ([#2597](https://github.com/opensearch-project/data-prepper/issues/2597))
* Update OpenSearch sink retry mechanism ([#2643](https://github.com/opensearch-project/data-prepper/issues/2643))

### Bug Fixes
* Fix NullPointerException in OpenSearch sink for bulkRetryCountMap ([#2600](https://github.com/opensearch-project/data-prepper/issues/2600))

### Security
* Fix CVE-2022-1471 ([#2632](https://github.com/opensearch-project/data-prepper/pull/2632))
* Fix CVE-2023-20863, CVE-2022-45143 ([#2632](https://github.com/opensearch-project/data-prepper/pull/2632))
* Fix CVE-2022-41854, CVE-2022-1471 ([#2651](https://github.com/opensearch-project/data-prepper/pull/2651))

### Maintenance
* Update kotlin-stdlib, kotlin-stdlib-common to 1.8.20 ([#2434](https://github.com/opensearch-project/data-prepper/pull/2434))
* Update log4j-jpl to 2.20.0 ([#2430](https://github.com/opensearch-project/data-prepper/pull/2430))
* Update byte-buddy-agent to 1.14.3 ([#2430](https://github.com/opensearch-project/data-prepper/pull/2430))
* Cleanup OpenSearch sink, OTel trace processor gradle files and use Gradle catalog ([#2569](https://github.com/opensearch-project/data-prepper/pull/2569))
* Fix End-to-End acknowledgement integration test ([#2551](https://github.com/opensearch-project/data-prepper/pull/2569))
