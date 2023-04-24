## 2023-04-20 Version 2.2.0

---

### Features
* Support End-to-End Acknowledgments for S3 source ([#2394](https://github.com/opensearch-project/data-prepper/pull/2394), [#2426](https://github.com/opensearch-project/data-prepper/pull/2426), [#2436](https://github.com/opensearch-project/data-prepper/pull/2436), [#2458](https://github.com/opensearch-project/data-prepper/pull/2458), [#2465](https://github.com/opensearch-project/data-prepper/pull/2465))
* Support S3 DLQ for OpenSearch sink ([#2298](https://github.com/opensearch-project/data-prepper/pull/2298))
* Support Amazon Opensearch Serverless Sink ([#2169](https://github.com/opensearch-project/data-prepper/pull/2169))
* Support S3 Select in `s3` source ([#1971](https://github.com/opensearch-project/data-prepper/pull/1971))
* Support new `list_to_map` processor ([#2410](https://github.com/opensearch-project/data-prepper/pull/2410))
* Support Snappy compression in `s3` source ([#2420](https://github.com/opensearch-project/data-prepper/pull/2420))
* Add append aggregation action for aggregate processor ([#2230](https://github.com/opensearch-project/data-prepper/pull/2230))
* Support built-in grok patterns for common use-cases ([#2514](https://github.com/opensearch-project/data-prepper/issues/2514))

### Enhancements
* Improve logging by adding error details to logs ([#2391](https://github.com/opensearch-project/data-prepper/issues/2391), [#2417](https://github.com/opensearch-project/data-prepper/issues/2417), [#2555](https://github.com/opensearch-project/data-prepper/issues/2555), [#2448](https://github.com/opensearch-project/data-prepper/issues/2448), [#2438](https://github.com/opensearch-project/data-prepper/issues/2438))
* Add EventFactory framework to create events ([#2378](https://github.com/opensearch-project/data-prepper/issues/2378), [#2399](https://github.com/opensearch-project/data-prepper/issues/2399) [#2426](https://github.com/opensearch-project/data-prepper/issues/2426))
* Support routes as an alias to route component ([#2550](https://github.com/opensearch-project/data-prepper/issues/2550))
* Update Java serialization allowlist to have specific classes for JsonNode ([#2376](https://github.com/opensearch-project/data-prepper/issues/2376))
* Support configurable retries in OpenSearch sink ([#2339](https://github.com/opensearch-project/data-prepper/issues/2339))
* Use AwsSdk2Transport with OpenSearch Java client ([#1881](https://github.com/opensearch-project/data-prepper/issues/1881))
* Add sqsMessagesDeleteFailed to track SQS delete failures ([#2450](https://github.com/opensearch-project/data-prepper/issues/2450))
* Retry TimeoutException caused by flushing data to buffer in S3 source ([#2470](https://github.com/opensearch-project/data-prepper/issues/2470))
* Add IAM role validation to S3 source and OpenSearch sink ([#2472](https://github.com/opensearch-project/data-prepper/issues/2472))
* Allow deprecated name for plugins ([#2504](https://github.com/opensearch-project/data-prepper/issues/2504))
* Make shutdown behaviour configurable ([#2524](https://github.com/opensearch-project/data-prepper/issues/2524))
* Add support for document_root_key to configure the key to write to OpenSearch ([#2511](https://github.com/opensearch-project/data-prepper/issues/2511))
* Support format option in `add_entries` processor  ([#2464](https://github.com/opensearch-project/data-prepper/issues/2464))
* Support isolated service map extraction in service-map ([#628](https://github.com/opensearch-project/data-prepper/issues/628))

### Bug Fixes
* Fix missing data points in EMFLoggingMeterRegistry ([#2469](https://github.com/opensearch-project/data-prepper/issues/2469))
* Fix Trace analytics sample app requirement dependencies ([#2477](https://github.com/opensearch-project/data-prepper/pull/2477))
* Shutdown log_generator properly ([#2494](https://github.com/opensearch-project/data-prepper/pull/2494))
* Fix CSV codec drops data without metrics ([#2512](https://github.com/opensearch-project/data-prepper/pull/2512))
* Fix bug where Java process doesn't get killed when Data Prepper terminates ([#2441](https://github.com/opensearch-project/data-prepper/pull/2441))

### Security
* Fix CVE-2023-20863, CVE-2023-1370, WS-2019-0379 ([#2546](https://github.com/opensearch-project/data-prepper/pull/2546))

### Maintenance
* Update package name for otel_logs_source from com.amazon to org.opensearch ([#2543](https://github.com/opensearch-project/data-prepper/pull/2543))
* Updated the following plugin names, otel_trace_raw to otel_traces, otel_metrics_raw_processor to otel_metrics, service_map_stateful to service_map ([#2526](https://github.com/opensearch-project/data-prepper/pull/2526))
* Move backoff and retry logic to BufferAccumulator ([#2481](https://github.com/opensearch-project/data-prepper/pull/2481))
* Remove unnecessary calls in file sink. ([#2475](https://github.com/opensearch-project/data-prepper/pull/2475))
* Clean up un-used Peer Forwarder processor gradle file ([#2386](https://github.com/opensearch-project/data-prepper/pull/2386))
* Fix Java doc warning ([#2396](https://github.com/opensearch-project/data-prepper/pull/2396))
* Use Gradle version catalogs for dependency management ([#2382](https://github.com/opensearch-project/data-prepper/pull/2382))
* Update developer guide links to Documentation ([#2367](https://github.com/opensearch-project/data-prepper/pull/2367))
* Update log4j-bom to 2.20.0 ([#2334](https://github.com/opensearch-project/data-prepper/pull/2334))
* Update commons-io to 2.11.0 ([#2335](https://github.com/opensearch-project/data-prepper/pull/2335), [#2336](https://github.com/opensearch-project/data-prepper/pull/2336))
* Update byte-buddy to 1.14.2 ([#2407](https://github.com/opensearch-project/data-prepper/pull/2407))
* Update randomcutforest to 3.6.0 ([#2507](https://github.com/opensearch-project/data-prepper/pull/2507))
* Update micrometer-bom to 1.10.5 ([#2433](https://github.com/opensearch-project/data-prepper/pull/2433))
* Replace Grok debugger in documentation ([#2425](https://github.com/opensearch-project/data-prepper/pull/2425))
* Test against OpenSearch 2.6.0 version ([#2455](https://github.com/opensearch-project/data-prepper/issues/2455))
