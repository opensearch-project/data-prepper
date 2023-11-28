## 2023-11-28 Version 2.6.0

---

### Features
* Support DynamoDB as a source. ([#2932](https://github.com/opensearch-project/data-prepper/issues/2932))
* Use Kafka as a buffer ([#3322](https://github.com/opensearch-project/data-prepper/issues/3322))
* Support dynamically changing the visibility timeout for S3 Source with SQS queue ([#2485](https://github.com/opensearch-project/data-prepper/issues/2485))
* Create or update Amazon OpenSearch Serverless network policy ([#3577](https://github.com/opensearch-project/data-prepper/issues/3577))
* Sink level metric for end to end latency ([#3494](https://github.com/opensearch-project/data-prepper/issues/3494))


### Enhancements
* Use Amazon Linux as base Docker image ([#3505](https://github.com/opensearch-project/data-prepper/issues/3505))
* Allow the Kafka buffer (and others that do not require the heap) to bypass the heap circuit breaker ([#3616](https://github.com/opensearch-project/data-prepper/issues/3616))
* Improve gRPC request exception logging ([#3621](https://github.com/opensearch-project/data-prepper/issues/3621))
* Configure the delay in the random string source ([#3601](https://github.com/opensearch-project/data-prepper/issues/3601))
* Add `distribution_version` flag to `opensearch` source ([#3636](https://github.com/opensearch-project/data-prepper/pull/3636))


### Bug Fixes
* Data Prepper is writing empty DLQ objects ([#3644](https://github.com/opensearch-project/data-prepper/issues/3644))
* Bulk Operation Retry Strategy should print cause of error ([#3504](https://github.com/opensearch-project/data-prepper/issues/3504))
* ISM index rollover actions fail because of missing setting for otel-v1-apm-span-* indices ([#3506](https://github.com/opensearch-project/data-prepper/issues/3506))
* AWS `opensearch` source error: `ElasticsearchVersionInfo.buildFlavor` ([#3640](https://github.com/opensearch-project/data-prepper/issues/3640))
* No permissions for writing to Amazon OpenSearch Serverless collection only shows errors after `max_retries` limit is reached ([#3508](https://github.com/opensearch-project/data-prepper/issues/3508))
* Bulk Operation Retry Strategy should print cause of error ([#3504](https://github.com/opensearch-project/data-prepper/issues/3504))
* NullPointer exception in `DefaultKafkaClusterConfigSupplier` get API ([#3528](https://github.com/opensearch-project/data-prepper/issues/3528))
* Check if failedDeleteCount is positive before logging an SQS error ([#3686](https://github.com/opensearch-project/data-prepper/issues/3686))
* Docker image jre-jammy contains Berkeley DB ([#3543](https://github.com/opensearch-project/data-prepper/issues/3543))
* Race condition in DefaultEventHandle ([#3617](https://github.com/opensearch-project/data-prepper/issues/3617))


### Security
* CVE-2023-44981 (Critical) detected in multiple libraries ([#3491](https://github.com/opensearch-project/data-prepper/issues/3491))
* CVE-2023-36478 (High) detected in http2-hpack-11.0.12.jar, jetty-http-11.0.12.jar ([#3490](https://github.com/opensearch-project/data-prepper/issues/3490))
* CVE-2023-4586 (High) detected in netty-handler-4.1.100.Final.jar ([#3443](https://github.com/opensearch-project/data-prepper/issues/3443))
* CVE-2023-5072 (High) detected in json-20230618.jar ([#3522](https://github.com/opensearch-project/data-prepper/issues/3522))
* CVE-2023-39410 (High) detected in avro-1.11.0.jar ([#3430](https://github.com/opensearch-project/data-prepper/issues/3430))
* CVE-2023-4043 (High) detected in parsson-1.1.2.jar ([#3588](https://github.com/opensearch-project/data-prepper/issues/3588))
* CVE-2023-46122 (High) detected in io_2.13-1.9.1.jar ([#3547](https://github.com/opensearch-project/data-prepper/issues/3547))
* CVE-2023-46136 (High) detected in Werkzeug-2.2.3-py3-none-any.whl ([#3552](https://github.com/opensearch-project/data-prepper/issues/3552))
* CVE-2023-26048 (Medium) detected in jetty-server-11.0.12.jar ([#2533](https://github.com/opensearch-project/data-prepper/issues/2533))
* CVE-2023-26049 (Medium) detected in jetty-http-11.0.12.jar, jetty-server-11.0.12.jar ([#2532](https://github.com/opensearch-project/data-prepper/issues/2532))
* CVE-2023-40167 (Medium) detected in jetty-http-11.0.12.jar ([#3359](https://github.com/opensearch-project/data-prepper/issues/3359))
* CVE-2023-36479 (Medium) detected in jetty-servlets-11.0.12.jar ([#3367](https://github.com/opensearch-project/data-prepper/issues/3367))
* WS-2023-0236 (Low) detected in jetty-xml-11.0.12.jar ([#3072](https://github.com/opensearch-project/data-prepper/issues/3072))


### Maintenance
* Update to the Gradle 8.x version which supports Java 21. Gradle 8.3 is supporting up to Java 20. ([#3330](https://github.com/opensearch-project/data-prepper/issues/3330))
* Start building Data Prepper on Java 21 ([#3329](https://github.com/opensearch-project/data-prepper/issues/3329))
* Integration tests to validate data going to OpenSearch ([#3678](https://github.com/opensearch-project/data-prepper/issues/3678))
* Unit tests fail on Windows machine ([#3459](https://github.com/opensearch-project/data-prepper/issues/3459))
* Fix disabled E2E ack integration tests in PipelinesWithAcksIT.java ([#3472](https://github.com/opensearch-project/data-prepper/issues/3472))
* Remove the `@Deprecated` from `Record` ([#3536](https://github.com/opensearch-project/data-prepper/issues/3536))
* Remove all unnecessary projects in the 2.6 branch ([#3605](https://github.com/opensearch-project/data-prepper/issues/3605))
* Update end-to-end tests to run from the released Docker image ([#3566](https://github.com/opensearch-project/data-prepper/issues/3566))

