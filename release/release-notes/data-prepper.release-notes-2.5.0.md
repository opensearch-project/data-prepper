## 2023-10-09 Version 2.5.0

---

### Features
* Support OpenSearch as source. ([#1985](https://github.com/opensearch-project/data-prepper/issues/1985))
* Support translate processor. ([#1914](https://github.com/opensearch-project/data-prepper/issues/1914))
* Support dissect processor. ([#3362](https://github.com/opensearch-project/data-prepper/issues/3362))
* Support AWS secrets in pipeline and Data Prepper config. ([#2780](https://github.com/opensearch-project/data-prepper/issues/2780))

### Enhancements
* Support update, upsert, delete bulk actions in OpenSearch sink. ([#3109](https://github.com/opensearch-project/data-prepper/issues/3109))
* Support inline index templates in OpenSearch sink. ([#3365](https://github.com/opensearch-project/data-prepper/issues/3365))
* Add retry to Kafka consumer in source. ([#3399](https://github.com/opensearch-project/data-prepper/pull/3399))
* Support OpenTelemetry SeverityText for logs. ([#3280](https://github.com/opensearch-project/data-prepper/issues/3280))
* Merging PipelineDataflowModel instead of pipeline YAML files. ([#3289]https://github.com/opensearch-project/data-prepper/issues/3289) 
* Support recursive feature in KeyValue processor. ([#888](https://github.com/opensearch-project/data-prepper/issues/888))

### Bug Fixes
* Fix NullPointerException in S3 scan when bucket kay has null value. ([#3316](https://github.com/opensearch-project/data-prepper/issues/3316))
* Fix a bug where S3 source does not stop on pipeline shutdown. ([#3341](https://github.com/opensearch-project/data-prepper/issues/3341))
* Fix exemplar list in Histogram and Count aggregations. ([#3364](https://github.com/opensearch-project/data-prepper/pull/3364))

### Security
* Fix CVE-2023-39410. ([#3430](https://github.com/opensearch-project/data-prepper/issues/3430))

### Maintenance
* Build with Gradle 8. ([#3287](https://github.com/opensearch-project/data-prepper/issues/3267))  
* Remove sleep from Kafka source timeout test. ([#3263](https://github.com/opensearch-project/data-prepper/issues/3263))
* Enable Gatling HTTPS support and path configuration. ([#3308](https://github.com/opensearch-project/data-prepper/issues/3308))
* Support Gatling tests using AWS sigV4 signing. ([#3311](https://github.com/opensearch-project/data-prepper/issues/3311))
* Support local ARM image build. ([#3352](https://github.com/opensearch-project/data-prepper/issues/3352))