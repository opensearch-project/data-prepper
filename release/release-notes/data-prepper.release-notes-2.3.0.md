## 2023-06-06 Version 2.3.0

---

### Features
* Supports S3 as a sink. ([#1048](https://github.com/opensearch-project/data-prepper/issues/1048), [#2732](https://github.com/opensearch-project/data-prepper/issues/2732))
* Tagging events. ([#629](https://github.com/opensearch-project/data-prepper/issues/629))
* Adds a function for getting the length of a string using Data Prepper Expressions. ([#2639](https://github.com/opensearch-project/data-prepper/issues/2639))
* Adds a function to determine if an IP address is in a CIDR block. ([#2625](https://github.com/opensearch-project/data-prepper/issues/2625))
* Support for expressions to extract string or numerical values in Data Prepper Expressions. ([#2686](https://github.com/opensearch-project/data-prepper/issues/2686), [#2685](https://github.com/opensearch-project/data-prepper/issues/2685))
* Read Parquet files using a codec for the S3 source and other codec-compatible sources. ([#2445](https://github.com/opensearch-project/data-prepper/issues/2445))
* Read Avro files using a codec for the S3 source and other codec-compatible sources. ([#2527](https://github.com/opensearch-project/data-prepper/issues/2527))
* Adds an obfuscate processor to help obfuscate data. ([#1952](https://github.com/opensearch-project/data-prepper/issues/1952))
* Support parsing User-Agent headers with a `user_agent` processor. ([#2618](https://github.com/opensearch-project/data-prepper/issues/2618))
* Support expressions in `add_entries` processor. ([#2672](https://github.com/opensearch-project/data-prepper/issues/2672))
* Support adding event data from event metadata in `add_entries` processor. ([#2687](https://github.com/opensearch-project/data-prepper/issues/2687))
* Adds a tail sampling processor. ([#2572](https://github.com/opensearch-project/data-prepper/issues/2572))
* Adds when conditions for many difference processors. ([#2613](https://github.com/opensearch-project/data-prepper/issues/2613))


### Enhancements
* Support OpenSearch composable index templates. ([#1275](https://github.com/opensearch-project/data-prepper/issues/1275))
* Adds input codec plugin concept to Data Prepper. ([#1532](https://github.com/opensearch-project/data-prepper/issues/1532))
* Improves error messages on authorization failures with OpenSearch clusters. ([#2657](https://github.com/opensearch-project/data-prepper/issues/2657), [#2655](https://github.com/opensearch-project/data-prepper/issues/2655))
* Clearer metrics when S3 source does not detect events in S3 objects. ([#2699](https://github.com/opensearch-project/data-prepper/issues/2699))
* Allow override the Log4j configuration file when running Data Prepper. ([#2720](https://github.com/opensearch-project/data-prepper/issues/2720))
* Pipeline creation succeeds even when sinks are not ready. ([#2656](https://github.com/opensearch-project/data-prepper/issues/2656))
* Support compression in OTel sources. ([#1176](https://github.com/opensearch-project/data-prepper/issues/1176))
* Support gzip compression in HTTP source. ([#1399](https://github.com/opensearch-project/data-prepper/issues/1399))
* Support functions in Data Prepper Expression language. ([#2626](https://github.com/opensearch-project/data-prepper/issues/2626))
* Support SNS to SQS messages in S3 source. ([#2604](https://github.com/opensearch-project/data-prepper/issues/2604))
* Support shared AWS credentials across pipeline components. ([#2751](https://github.com/opensearch-project/data-prepper/issues/2751), [#2764](https://github.com/opensearch-project/data-prepper/issues/2764))
* Initial extension plugin support. ([#2636](https://github.com/opensearch-project/data-prepper/issues/2636), [#2637](https://github.com/opensearch-project/data-prepper/issues/2637))


### Bug Fixes
* Improves how the `opensearch` sink retries requests to OpenSearch with a maximum wait time. ([#2641](https://github.com/opensearch-project/data-prepper/issues/2641))
* Improves how the `opensearch` sink initializes by continuing to retry failure requests. ([#2769](https://github.com/opensearch-project/data-prepper/issues/2769))
* Fixes an exception when writing to OpenSearch. ([#2654](https://github.com/opensearch-project/data-prepper/issues/2654))
* Improves error logs for document failures in `opensearch` sink. ([#2597](https://github.com/opensearch-project/data-prepper/issues/2597))
* Fixes issues with S3 key prefixes in the `opensearch` S3 DLQ. ([#2581](https://github.com/opensearch-project/data-prepper/issues/2581))
* Updates Random Cut Forest dependency to reduce noise. ([#2783](https://github.com/opensearch-project/data-prepper/issues/2783))
* Fixes to writing to the Data Prepper buffer in the S3 source. ([#2701](https://github.com/opensearch-project/data-prepper/pull/2701))
* Fixes S3 source backoff which was too aggressive in the requests it made. ([#2568](https://github.com/opensearch-project/data-prepper/pull/2568))


### Security
* CVE-2021-22096 ([#2671](https://github.com/opensearch-project/data-prepper/issues/2671))
* CVE-2021-22060 ([#2670](https://github.com/opensearch-project/data-prepper/issues/2670))


### Maintenance
* DataPrepper ExpressionEvaluator should not be type specific. ([#2703](https://github.com/opensearch-project/data-prepper/issues/2703))
* Change JsonStringBuilder in JacksonEvent to be non static for ease-of-use. ([#2665](https://github.com/opensearch-project/data-prepper/issues/2665))
