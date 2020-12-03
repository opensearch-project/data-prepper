## 2020-12-10 Version 0.1.0 (Current)

This is the first official release of Open Distro for Elasticsearch Data Prepper

Data Prepper is a server side data collector with abilities to filter, enrich, transform, normalize and aggregate data for downstream analytics and visualization. The broader vision for Data Prepper is to enable an end-to-end data analysis life cycle from gathering raw logs to facilitating sophisticated and actionable interactive ad-hoc analyses on the data.

### Documentation
* [Overview](https://github.com/opendistro-for-elasticsearch/Data-Prepper/commit/e51238b2cea55bba138466f146fc19622331e59c): Overview of Data Prepper
* [Trace Overview](https://github.com/opendistro-for-elasticsearch/Data-Prepper/commit/e51238b2cea55bba138466f146fc19622331e59c): Trace overview

### Features
*The current release does not have an accurate list of commits/changes to individual features as they were added. We intend to track and maintain accurate list of commits referencing to feature/enhancement/test/documentation from subsequent releases. Below are the major features for this release*

1. Support multiple pipeline executions in parallel
2. Elasticsearch service as sink plugin
3. OtelTraceSource supporting OTLP/grpc and OTLP/Http as source plugin
4. OtelTraceRawProcessor as processor plugin
5. ServiceMap processor for stateful processing as processor plugin
6. Default BlockingBuffer as buffer plugin
7. Default plugins like file as source/sink, stdin/stdout, string case converter
8. data-prepper-benchmarks subproject for ability to run basic benchmarks
9. Examples - including Jaeger hotrod application, trace analytics application, zipkin sleuth webmvc
10. Docker distribution capability
11. Linux & MacOS archive distribution capability


#### Core Features
* Feature [#1](https://github.com/opendistro-for-elasticsearch/Data-Prepper/pull/1): Basic Data Prepper
  * Add data-prepper-core, data-prepper-api and data-prepper-plugins sub-projects
  * Add Data Prepper API interfaces for Source, Buffer, Processor and Sink
  * Add basic Pipeline and ProcessWorker
  * Add YML parser for configuration
  * Add custom deserializer for configuration
  * Add Data Prepper PluginType annotation
  * Implement default plugins for source, buffer, processor and sink
  * Add plugin repository
  * Add ProcessorState interface
  * Add Pipeline execute
  * Add ElasticsearchSink as sink plugin
  * Add OtelTraceSource as source plugin
* Feature [#3](https://github.com/opendistro-for-elasticsearch/Data-Prepper/pull/3): Segregate Data Prepper plugins into separate projects
* Feature [#16](https://github.com/opendistro-for-elasticsearch/Data-Prepper/pull/16/files): Add Lmdb plugin
* Feature [#26](https://github.com/opendistro-for-elasticsearch/Data-Prepper/pull/26): Add Blocking buffer as default buffer plugin
* Feature [#28](https://github.com/opendistro-for-elasticsearch/Data-Prepper/pull/28): Add ServiceMap stateful processor
* Feature [#47](https://github.com/opendistro-for-elasticsearch/Data-Prepper/pull/47): Capability to support multiple pipelines
* Feature [#53](https://github.com/opendistro-for-elasticsearch/Data-Prepper/pull/53): Add hashId for relationships for ServiceMap processor
* Feature [#71](https://github.com/opendistro-for-elasticsearch/Data-Prepper/pull/71): Add support for multiple sinks and processors
* Feature [#100](https://github.com/opendistro-for-elasticsearch/Data-Prepper/pull/100): Add docker distribution capabilities

#### Examples & Testing
* Example [#79](https://github.com/opendistro-for-elasticsearch/Data-Prepper/pull/79): Add Jaeger hot rod example application
* Example [#81](https://github.com/opendistro-for-elasticsearch/Data-Prepper/pull/98): Add Kibana trace analytics Docker
* Example [#181](https://github.com/opendistro-for-elasticsearch/Data-Prepper/pull/181): Add trace analytics sample application

#### Documentation
* RFC [#22](https://github.com/opendistro-for-elasticsearch/Data-Prepper/pull/22): Add RFC first cut
* Documentation [#157](https://github.com/opendistro-for-elasticsearch/Data-Prepper/pull/157): Add Overview documentation
#### Enhancements
* Enhancement [#18](https://github.com/opendistro-for-elasticsearch/Data-Prepper/pull/18): Add file config for DLQ
* Enhancement [#23](https://github.com/opendistro-for-elasticsearch/Data-Prepper/pull/23): Expose bulk batch settings for Elasticsearch sink plugin
* Enhancement [#27](https://github.com/opendistro-for-elasticsearch/Data-Prepper/pull/27): Make synchronous bulk requests to Elasticsearch service
* Enhancement [#42](https://github.com/opendistro-for-elasticsearch/Data-Prepper/pull/42): Add retry capabilities to Elasticsearch sink plugin


  
  
