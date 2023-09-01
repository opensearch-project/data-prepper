## 2023-08-23 Version 2.4.0

---

### Features
* Support Kafka as source. ([#254](https://github.com/opensearch-project/data-prepper/issues/254))
* Support source coordination. ([#2412](https://github.com/opensearch-project/data-prepper/issues/2412))
* Support S3 scan capability in S3 source. ([#1970](https://github.com/opensearch-project/data-prepper/issues/1970))
* Support ElasticSearch 6.8 in OpenSearch sink. ([#3003](https://github.com/opensearch-project/data-prepper/issues/3003))
* Support custom index template in OpenSearch sink with ElasticSearch 6.8. ([#3060](https://github.com/opensearch-project/data-prepper/issues/3060))
* Support filtering data in sink using include_keys and exclude_keys. ([#2975](https://github.com/opensearch-project/data-prepper/issues/2975))

### Enhancements
* Support generic sink codec structure for sinks. ([#2403](https://github.com/opensearch-project/data-prepper/issues/2403))
* Support expressions in OpenSearch index and document ID. ([#2864](https://github.com/opensearch-project/data-prepper/issues/2864))
* Support defining bucket ownership. ([#2012](https://github.com/opensearch-project/data-prepper/issues/2012))
* Add exemplars to metrics generated in aggregate processor. ([#3164](https://github.com/opensearch-project/data-prepper/issues/3164))
* Add cardinality key support in Anomaly detector processor using identification_keys. ([#3073](https://github.com/opensearch-project/data-prepper/issues/3073))
* Support allow_duplicate_values in key-value processor. ([#889](https://github.com/opensearch-project/data-prepper/issues/889))
* Support remove_brackets in key-value processor. ([#892](https://github.com/opensearch-project/data-prepper/issues/892))
* Support exclude_keys in key-value processor. ([#890](https://github.com/opensearch-project/data-prepper/issues/890))
* Support default_keys in key-value processor. ([#891](https://github.com/opensearch-project/data-prepper/issues/891))
* Add GZip compression in S3 sink. ([#3130](https://github.com/opensearch-project/data-prepper/issues/3130))
* Add Snappy compression in S3 sink. ([#3154](https://github.com/opensearch-project/data-prepper/issues/3154))
* Update metric for ECF instance from counter to gauge and fix flaky tests caused by RCF variance. ([#3145](https://github.com/opensearch-project/data-prepper/issues/3145))
* Support s3:// prefix in pipeline where s3 bucket names are used. ([#3143](https://github.com/opensearch-project/data-prepper/issues/3143))
* Update circuit breaker configuration log message. ([#3175](https://github.com/opensearch-project/data-prepper/pull/3175))
* Deprecate document_id_field in support of document_id. ([#3074](https://github.com/opensearch-project/data-prepper/pull/3074))

### Bug Fixes
* Fix race condition in sources using End-to-End Acknowledgments. ([#3038](https://github.com/opensearch-project/data-prepper/issues/3038))
* Fix DLQ deserialization with create action. ([#3040](https://github.com/opensearch-project/data-prepper/issues/3040))
* Fix IllegalArgumentException in csv processor when key does not exist. ([#3053](https://github.com/opensearch-project/data-prepper/issues/3053))
* Handle RequestTimeoutException in push based sources correctly. ([#3063](https://github.com/opensearch-project/data-prepper/issues/3063))
* Fix S3 sink writing to closed stream. ([#3160](https://github.com/opensearch-project/data-prepper/issues/3160))
* Fix timestamp used in S3 sink to 24-hour format. ([#3171](https://github.com/opensearch-project/data-prepper/issues/3171))
* Fix stale buffer data not being written to S3 sink. ([#3187](https://github.com/opensearch-project/data-prepper/pull/3187))
* Fix IllegalArgumentException in convert entry processor. ([#3135](https://github.com/opensearch-project/data-prepper/issues/3135))
* Fix ClassCastException in parse_json processor with OTel logs source. ([#3184](https://github.com/opensearch-project/data-prepper/issues/3184))
* Fix UnexpectedTypeException in S3 select using NotBlank annotation. ([#3208](https://github.com/opensearch-project/data-prepper/pull/3208))

### Maintenance
* Update existing release workflow to trigger Data Prepper release jenkins job. ([#2122](https://github.com/opensearch-project/data-prepper/issues/2122))
* Reduce test time which reduces build time. ([#3019](https://github.com/opensearch-project/data-prepper/pull/3019), [#3020](https://github.com/opensearch-project/data-prepper/pull/3020), [#3021](https://github.com/opensearch-project/data-prepper/pull/3021))
* Updated GitHub actions to use Data Prepper in job titles. ([#3104](https://github.com/opensearch-project/data-prepper/pull/3104))
* Add Apache commons-lang3 to gradle catalog. ([#3120](https://github.com/opensearch-project/data-prepper/pull/3120))
* Add integration test coverage for ODFE 0.10.0 OpenSearch sink. ([#3131](https://github.com/opensearch-project/data-prepper/pull/3131))
* Updated documentation process in developer guide. ([#2772](https://github.com/opensearch-project/data-prepper/pull/2772))
* Fix flaky conditional routing test. ([#3139](https://github.com/opensearch-project/data-prepper/issues/3139))
* Fix flaky unit tests. ([#3150](https://github.com/opensearch-project/data-prepper/pull/3150))
* Add integration test for S3 sink. ([#3179](https://github.com/opensearch-project/data-prepper/pull/3179))
* Update Data Prepper tar.gz to include JDK 17.0.8+7. ([#3136](https://github.com/opensearch-project/data-prepper/pull/3136))
* Update S3 sink to speed up unit test time. ([#3203](https://github.com/opensearch-project/data-prepper/pull/3203))