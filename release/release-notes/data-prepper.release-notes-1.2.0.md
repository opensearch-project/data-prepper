## 2021-12-15 Version 1.2.0

---

### Features
* Grok Prepper for processing unstructured data with grok pattern matching. ([#302](https://github.com/opensearch-project/data-prepper/pull/302)), ([#324](https://github.com/opensearch-project/data-prepper/pull/324)), ([#377](https://github.com/opensearch-project/data-prepper/pull/377)), ([#449](https://github.com/opensearch-project/data-prepper/pull/449)), ([#510](https://github.com/opensearch-project/data-prepper/pull/510)), ([#548](https://github.com/opensearch-project/data-prepper/pull/548)), ([549](https://github.com/opensearch-project/data-prepper/pull/589)), ([#548](https://github.com/opensearch-project/data-prepper/pull/548)), & ([#586](https://github.com/opensearch-project/data-prepper/pull/586))
* HTTP Source plugin for receiving log data ([#309](https://github.com/opensearch-project/data-prepper/pull/309)), ([#325](https://github.com/opensearch-project/data-prepper/pull/325)), ([#359](https://github.com/opensearch-project/data-prepper/pull/359)), ([#380](https://github.com/opensearch-project/data-prepper/pull/380)), & ([#415](https://github.com/opensearch-project/data-prepper/pull/415))
* Logstash config support. Users can now run Data Prepper with a logstash.conf file. ([#581](https://github.com/opensearch-project/data-prepper/pull/581)), ([#568](https://github.com/opensearch-project/data-prepper/pull/568)), ([#579](https://github.com/opensearch-project/data-prepper/pull/579)), ([#580](https://github.com/opensearch-project/data-prepper/pull/580)), ([#582](https://github.com/opensearch-project/data-prepper/pull/582)), ([#587](https://github.com/opensearch-project/data-prepper/pull/587)), ([#636](https://github.com/opensearch-project/data-prepper/pull/636)), ([#473](https://github.com/opensearch-project/data-prepper/pull/473)), ([#535](https://github.com/opensearch-project/data-prepper/pull/535)), ([#552](https://github.com/opensearch-project/data-prepper/pull/552)), ([#577](https://github.com/opensearch-project/data-prepper/pull/577)), ([#588](https://github.com/opensearch-project/data-prepper/pull/588)),  ([#597](https://github.com/opensearch-project/data-prepper/pull/597)), ([#616](https://github.com/opensearch-project/data-prepper/pull/616)), ([#447](https://github.com/opensearch-project/data-prepper/pull/447)), ([#506](https://github.com/opensearch-project/data-prepper/pull/506)), ([#559](https://github.com/opensearch-project/data-prepper/pull/559)), ([#575](https://github.com/opensearch-project/data-prepper/pull/575)), ([#584](https://github.com/opensearch-project/data-prepper/pull/584)), ([#591](https://github.com/opensearch-project/data-prepper/pull/591)), & ([#617](https://github.com/opensearch-project/data-prepper/pull/617))

### Enhancements
* PluginSettings now supports generic List and Map data types ([#302](https://github.com/opensearch-project/data-prepper/pull/302))
* A disabled SSL warning was added to HTTP and Otel Trace Source plugins and Data Prepper core APIs. A warning will appear in Data Prepper logs when SSL is disabled. ([#537](https://github.com/opensearch-project/data-prepper/pull/537)) & ([#603](https://github.com/opensearch-project/data-prepper/pull/603))
* HTTP & Otel Trace Source support configurable basic authentication via plugins ([#570](https://github.com/opensearch-project/data-prepper/pull/570)), ([#545](https://github.com/opensearch-project/data-prepper/pull/545))
* File Source file type is now configurable and supports parsing JSON files. The default remains plain text.  ([#601](https://github.com/opensearch-project/data-prepper/pull/601)),
* Buffer Interface now supports batch writing via a writeAll method. BlockingBuffer now supports the writeAll method. ([#320](https://github.com/opensearch-project/data-prepper/pull/320))
* Plugin framework now supports a DataPrepperPluginConstructor annotation for indicating a plugin constructor. ([$481](https://github.com/opensearch-project/data-prepper/pull/481))
* Data Prepper core APIs now support basic HTTP Authentication, Docker image's core API's are now secure by default ([#558](https://github.com/opensearch-project/data-prepper/pull/558)), ([#561](https://github.com/opensearch-project/data-prepper/pull/561))
* OpenSearch sink now supports forwarding requests through an HTTP Proxy ([#479](https://github.com/opensearch-project/data-prepper/pull/479)
* OpenSearch sink now supports an optional index-type parameter.  ([#480](https://github.com/opensearch-project/data-prepper/pull/480))) & ([#433](https://github.com/opensearch-project/data-prepper/pull/433))
* OpenSearch sink now emits a new metric for bulkRequestSizeBytes. ([#572](https://github.com/opensearch-project/data-prepper/pull/572))

### Bug Fixes
* Fixed Github Actions for ODFE integration tests ([#393](https://github.com/opensearch-project/data-prepper/pull/393))

### Infrastructure
* Using Armeria client builder to help mitigate flaky end-to-end tests ([#375](https://github.com/opensearch-project/data-prepper/pull/375))
* Syncing OpenSearch version to help mitigate flaky end-to-end tests ([403](https://github.com/opensearch-project/data-prepper/pull/403))
* Refactoring existing end-to-end tests out of data prepper core into a new e2e-test module ([#512](https://github.com/opensearch-project/data-prepper/pull/512))
* Added basic grok end-to-end tests and created necessary CI workflow ([#536](https://github.com/opensearch-project/data-prepper/pull/536))
* Code coverage comment bot was added to the GitHub workflow ([#549](https://github.com/opensearch-project/data-prepper/pull/549))
* Added DCO check for GitHub workflow ([#360](https://github.com/opensearch-project/data-prepper/pull/360))
* Code checkstyle integration ([#378](https://github.com/opensearch-project/data-prepper/pull/378))
* Improved Issue template ([#397](https://github.com/opensearch-project/data-prepper/pull/397))
* Supporting Maven publication of the Data Prepper API ([#596](https://github.com/opensearch-project/data-prepper/pull/596)), ([#634](https://github.com/opensearch-project/data-prepper/pull/634)) & ([#635](https://github.com/opensearch-project/data-prepper/pull/635))
* Added support for generating THIRD PARTY licenses ([#621](https://github.com/opensearch-project/data-prepper/pull/621)) & ([#631](https://github.com/opensearch-project/data-prepper/pull/631))

### Documentation
* New getting started, developer, getting started trace analytics and pipeline setup guide ([#346](https://github.com/opensearch-project/data-prepper/pull/346))
* Added new log ingestion guide showcasing new HTTP and Grok Prepper Plugins ([#573](https://github.com/opensearch-project/data-prepper/pull/573))
* New guide for migrating to OpenSearch Data Prepper from Open Distro Data Prepper ([#470](https://github.com/opensearch-project/data-prepper/pull/470))
* Added Project Resource to documentation ([#482](https://github.com/opensearch-project/data-prepper/pull/482))
* Added Coding Guidance to the Developer Guid ([#560](https://github.com/opensearch-project/data-prepper/pull/560))
* Added instructions to build and run the Docker image locally ([#564](https://github.com/opensearch-project/data-prepper/pull/564))
* Updated copyright headers for root project, api and core ([#569](https://github.com/opensearch-project/data-prepper/pull/569))
* Update documentation to use OpenSearch Dashboards ([#658](https://github.com/opensearch-project/data-prepper/pull/658))
* Improving OpenSearch sink documentation ([#553](https://github.com/opensearch-project/data-prepper/pull/553)), ([#562](https://github.com/opensearch-project/data-prepper/pull/562)), & ([#563](https://github.com/opensearch-project/data-prepper/pull/563))

### Maintenance
* Updated version to 1.2 ([#416](https://github.com/opensearch-project/data-prepper/pull/416))
* The OpenSearch REST client and the build plugins used by the OpenSearch plugin are now at 1.1.0 ([#384](https://github.com/opensearch-project/data-prepper/pull/384))
* Use OpenSearch instead of Elasticsearch in builds ([#438](https://github.com/opensearch-project/data-prepper/pull/438))
* Use Netty 4.1.68 which fixes CVE-2021-37136 and CVE-2021-37137 ([#661](https://github.com/opensearch-project/data-prepper/pull/661))
* Uses Log4j 2.16.0 which fixes CVE-2021-44228 and CVE-2021-45046 ([#742](https://github.com/opensearch-project/data-prepper/pull/742))

### Refactoring
* Created a new internal data model, Events, to capture data as it flows through the pipeline. This was introduced to eliminate the excessive de/serialization of the current implementation. Currently, integrated with only log ingestion and sample plugins ([#](https://github.com/opensearch-project/data-prepper/pull/412)), ([#435](https://github.com/opensearch-project/data-prepper/pull/435)), ([#463](https://github.com/opensearch-project/data-prepper/pull/463)), ([#468](https://github.com/opensearch-project/data-prepper/pull/468)), ([#477](https://github.com/opensearch-project/data-prepper/pull/477)), & ([#539](https://github.com/opensearch-project/data-prepper/pull/539))
* The StdOutSink supports Objects instead of Strings as part of the migration to support the new event model ([#599](https://github.com/opensearch-project/data-prepper/pull/599))
* The FileSource uses Objects to support the new event model ([#](https://github.com/opensearch-project/data-prepper/pull/601))
* Small refactoring of PeerForwarder to improve readability of the code ([#626](https://github.com/opensearch-project/data-prepper/pull/626))
* Plugin class redesign leveraging new plugin framework ([#363](https://github.com/opensearch-project/data-prepper/pull/363)), ([#451](https://github.com/opensearch-project/data-prepper/pull/451)), ([#478](https://github.com/opensearch-project/data-prepper/pull/478))
* Consistent usage of OpenSearch, OpenSearch Dashboards and Amazon OpenSearch. ([#637](https://github.com/opensearch-project/data-prepper/pull/637))
* Refactoring OpenSearch sink to improve extendability and maintainability. ([#361](https://github.com/opensearch-project/data-prepper/pull/361)) & ([#414](https://github.com/opensearch-project/data-prepper/pull/414))
* Performance Improvements for Event model and Prepper threading ([#728](https://github.com/opensearch-project/data-prepper/pull/728)), ([#708](https://github.com/opensearch-project/data-prepper/pull/708))