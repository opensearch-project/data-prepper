## 2024-02-19 Version 2.6.2

---

### Enhancements
* Add 4xx aggregate metric and shard progress metric for dynamodb source ([#3913](https://github.com/opensearch-project/data-prepper/pull/3913))


### Bug Fixes
* S3 Scan has potential to filter out objects with the same timestamp ([#4123](https://github.com/opensearch-project/data-prepper/issues/4123))
* Kafka buffer attempts to create a topic when disabled ([#4111](https://github.com/opensearch-project/data-prepper/issues/4111))
* Grok processor match requests continue after timeout ([#4026](https://github.com/opensearch-project/data-prepper/issues/4026))
* Serialization error during peer-forwarding ([#3981](https://github.com/opensearch-project/data-prepper/issues/3981))
* BlockingBuffer.bufferUsage metric does not include records in-flight ([#3936](https://github.com/opensearch-project/data-prepper/issues/3936))
* Null Pointer Exception in Key Value Processor ([#3928](https://github.com/opensearch-project/data-prepper/issues/3928))
* Incomplete route set leads to duplicates when E2E ack is enabled. ([#3866](https://github.com/opensearch-project/data-prepper/issues/3866))
* Data Prepper is losing connections from S3 pool ([#3809](https://github.com/opensearch-project/data-prepper/issues/3809))
* Key value processor will throw NPE if source key does not exist in the Event ([#3496](https://github.com/opensearch-project/data-prepper/issues/3496))
* Exception in substitute string processor shuts down processor work but not pipeline ([#2956](https://github.com/opensearch-project/data-prepper/issues/2956))
* Add 4xx aggregate metric and shard progress metric for dynamodb source ([#3921](https://github.com/opensearch-project/data-prepper/pull/3921))

### Security
* Fix GHSA-6g3j-p5g6-992f from OpenSearch jar ([#3837](https://github.com/opensearch-project/data-prepper/issues/3837))
* Fix CVE-2023-41329 (Medium) detected in wiremock-3.0.1.jar ([#3954](https://github.com/opensearch-project/data-prepper/issues/3954))
* Fix CVE-2023-51074 (Medium) detected in json-path-2.8.0.jar ([#3919](https://github.com/opensearch-project/data-prepper/issues/3919))
* Fix CVE-2023-50572 (Medium) detected in jline-3.9.0.jar, jline-3.22.0.jar ([#3871](https://github.com/opensearch-project/data-prepper/issues/3871))
* Require Mozilla Rhino 1.7.12 to fix SNYK-JAVA-ORGMOZILLA-1314295. ([#3839](https://github.com/opensearch-project/data-prepper/pull/3839))
