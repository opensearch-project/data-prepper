## 2022-03-22 Version 1.3.0

---

### Important Deprecation
* We have updated the pipeline definition to support `processor:` as replacement of `prepper:` which has been deprecated and will be fully removed in 2.0. ([#655](https://github.com/opensearch-project/data-prepper/pull/655), [#667](https://github.com/opensearch-project/data-prepper/pull/667))

### Features
* AggregateProcessor for generic stateful aggregation ([README](https://github.com/opensearch-project/data-prepper/blob/1.3/data-prepper-plugins/aggregate-processor/README.md)) ([#839](https://github.com/opensearch-project/data-prepper/pull/839), [#850](https://github.com/opensearch-project/data-prepper/pull/850), [#931](https://github.com/opensearch-project/data-prepper/pull/931), [#969](https://github.com/opensearch-project/data-prepper/pull/969), [#1022](https://github.com/opensearch-project/data-prepper/pull/1022), [#1046](https://github.com/opensearch-project/data-prepper/pull/1046))
* DateProcessor to extract dates from fields in events ([README](https://github.com/opensearch-project/data-prepper/blob/1.3/data-prepper-plugins/date-processor/README.md)) ([#971](https://github.com/opensearch-project/data-prepper/pull/971), [#1014](https://github.com/opensearch-project/data-prepper/pull/1014))
* Processors to support mutate, alter, and delete fields from Events ([README](https://github.com/opensearch-project/data-prepper/blob/1.3/data-prepper-plugins/mutate-event-processors/README.md)) ([#1002](https://github.com/opensearch-project/data-prepper/pull/1002))
* KeyValueProcessor to support parsing messages with key-value strings such as queries and properties ([README](https://github.com/opensearch-project/data-prepper/blob/1.3/data-prepper-plugins/key-value-processor/README.md)) ([#872](https://github.com/opensearch-project/data-prepper/pull/872))
* DropProcessor to filter out (remove/drop) entire events based off confitional expression ([README](https://github.com/opensearch-project/data-prepper/tree/1.3/data-prepper-plugins/drop-events-processor)) ([#801](https://github.com/opensearch-project/data-prepper/pull/801), [#1174](https://github.com/opensearch-project/data-prepper/pull/1174))

### Enhancements
* Add dependency Injection support for Data Prepper Core and Plugins. ([#815](https://github.com/opensearch-project/data-prepper/pull/815), [#846](https://github.com/opensearch-project/data-prepper/pull/846), [#1140](https://github.com/opensearch-project/data-prepper/pull/1140))
* Add Data Prepper expression evaluator ([#1024](https://github.com/opensearch-project/data-prepper/pull/1024), [#1027](https://github.com/opensearch-project/data-prepper/pull/1027), [#1090](https://github.com/opensearch-project/data-prepper/pull/1090), [#1065](https://github.com/opensearch-project/data-prepper/pull/1065), [#1153](https://github.com/opensearch-project/data-prepper/pull/1153), [#1155](https://github.com/opensearch-project/data-prepper/pull/1155), [#1157](https://github.com/opensearch-project/data-prepper/pull/1157), [#1169](https://github.com/opensearch-project/data-prepper/pull/1169), [#1177](https://github.com/opensearch-project/data-prepper/pull/1177), [#1178](https://github.com/opensearch-project/data-prepper/pull/1178))
* Support for nested syntax in LogStashConfigConverter ([#1088](https://github.com/opensearch-project/data-prepper/pull/1088))
* Support default values for attributes in LogstashConfigConverter mapping files ([#1095](https://github.com/opensearch-project/data-prepper/pull/1095))
* Support converting index with a date-time pattern in LogstashConfigConverter ([#1045](https://github.com/opensearch-project/data-prepper/pull/1045), [#1095](https://github.com/opensearch-project/data-prepper/pull/1095))
* Support creation from raw text string in JacksonEvent builder ([#770](https://github.com/opensearch-project/data-prepper/pull/770), [#1074](https://github.com/opensearch-project/data-prepper/pull/1074))
* Support loading plugins from multiple packages ([#948](https://github.com/opensearch-project/data-prepper/pull/948))
* Support of date and time patterns in opensearch sink index names ([#788](https://github.com/opensearch-project/data-prepper/pull/788), [#833](https://github.com/opensearch-project/data-prepper/pull/833))
* Support for passing a PipelineDescription in @DataPrepperPluginConstructor ([#825](https://github.com/opensearch-project/data-prepper/pull/825))
* Validate Plugin Configurations using JSR-303 ([#826](https://github.com/opensearch-project/data-prepper/pull/826))
* Support Logstash configuration conversion for OpenSearch Logstash output ([#756](https://github.com/opensearch-project/data-prepper/pull/756))
* Support negation of boolean attribute values in Logstash configuration converter while mapping plugins ([#756](https://github.com/opensearch-project/data-prepper/pull/756))

### Bug Fixes
* Allow stdout and file sink to output generic object type ([#1192](https://github.com/opensearch-project/data-prepper/pull/1192))
* Fixed issue where Spring was unable to find the PrometheusMeterRegistry Bean ([#1019](https://github.com/opensearch-project/data-prepper/pull/1019))
* Use BlockingTaskExecutor in OtelTraceSource ([#745](https://github.com/opensearch-project/data-prepper/pull/745))

### Infrastructure
* Upload Maven artifacts as part of Release build ([#1181](https://github.com/opensearch-project/data-prepper/pull/1181))
* Updated the release build to push the Docker image and upload archives ([#1151](https://github.com/opensearch-project/data-prepper/pull/1151))
* Update Gradle project to produce only tar.gz archives ([#1132](https://github.com/opensearch-project/data-prepper/pull/1132))
* Add simple integration tests for AggregateProcessor ([#1046](https://github.com/opensearch-project/data-prepper/pull/1046))
* Assemble the data-prepper-core uber-jar using Zip64 ([#820](https://github.com/opensearch-project/data-prepper/pull/820))

### Documentation
* Example on log ingestion from Kubernetes containers ([#729](https://github.com/opensearch-project/data-prepper/pull/729))
* Update copyright headers in release scripts ([#933](https://github.com/opensearch-project/data-prepper/pull/933))
* Update copyright headers in data-prepper subprojects ([#776](https://github.com/opensearch-project/data-prepper/pull/776), [#928](https://github.com/opensearch-project/data-prepper/pull/928))
* Example on Data-Prepper ECS Firelens integration ([#704](https://github.com/opensearch-project/data-prepper/pull/704))

### Maintenance
* Delete outdated Kibana trace analytics example ([#1135](https://github.com/opensearch-project/data-prepper/pull/1135))
* Update Open Distro usages to OpenSearch in scripts ([#1086](https://github.com/opensearch-project/data-prepper/pull/1086))
* Upgrade docker-compose.yml files from ODFE to OpenSearch ([#847](https://github.com/opensearch-project/data-prepper/pull/847))
* Update the ADOT example to OpenSearch ([#703](https://github.com/opensearch-project/data-prepper/pull/703))
* Migrate demo sample application to use opensearch and opensearch dashboards instead of ODFE and kibana ([#666](https://github.com/opensearch-project/data-prepper/pull/666))
* Update log ingestion example to use latest data-prepper docker image ([#752](https://github.com/opensearch-project/data-prepper/pull/752))

### Refactoring
* Deprecating Record and RecordMetaData ([#954](https://github.com/opensearch-project/data-prepper/pull/954))
* FileSink to use Event model ([#750](https://github.com/opensearch-project/data-prepper/pull/750))
* StringPrepper to use Event model ([#753](https://github.com/opensearch-project/data-prepper/pull/753))
* StdInSource, RandomStringSource to use Event model ([#625](https://github.com/opensearch-project/data-prepper/pull/625))