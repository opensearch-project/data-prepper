## 2024-05-16 Version 2.8.0

---

### Features
* Support Full load and CDC from AWS DocumentDB [#4534] (https://github.com/opensearch-project/data-prepper/issues/4534)
* Support conditional expression to evaluate based on the data type for a given field ([#4478](https://github.com/opensearch-project/data-prepper/issues/4478) [#4523](https://github.com/opensearch-project/data-prepper/pull/4523), [#4500](https://github.com/opensearch-project/data-prepper/pull/4500)))
* Allow using event fields in s3 sink object_key [#3310] (https://github.com/opensearch-project/data-prepper/issues/3310)
* Support ndjson with a codec [#2700] (https://github.com/opensearch-project/data-prepper/issues/2700)
* Support S3 bucket ownership validation on the S3 sink ([#4468](https://github.com/opensearch-project/data-prepper/issues/4468))
* Support encoding JSON ([#832](https://github.com/opensearch-project/data-prepper/issues/832) [#4514](https://github.com/opensearch-project/data-prepper/pull/4514))
* Support for Event Json input and output codecs ([#4436](https://github.com/opensearch-project/data-prepper/pull/4436))
* Add support for dynamic bucket and default bucket in S3 sink ([#4402](https://github.com/opensearch-project/data-prepper/pull/4402))
* Add support to export/full load MongoDB/DocumentDB collection with `_id` field of different data type ([#4503](https://github.com/opensearch-project/data-prepper/pull/4503))


### Enhancements
* HTTP data chunking support for kafka buffer ([#4475](https://github.com/opensearch-project/data-prepper/pull/4475))
* ENH: automatic credential refresh in kafka source ([#4258](https://github.com/opensearch-project/data-prepper/pull/4258))
* Add creation and aggregation of dynamic S3 groups based on events ([#4346](https://github.com/opensearch-project/data-prepper/pull/4346))
* Truncate Processor: Add support to truncate all fields in an event ([#4317](https://github.com/opensearch-project/data-prepper/pull/4317))
* Provide validations of AWS accountIds ([#4398](https://github.com/opensearch-project/data-prepper/issues/4398))
* Better metrics on OpenSearch document errors ([#4344](https://github.com/opensearch-project/data-prepper/issues/4344))
* Better metrics for OpenSearch duplicate documents ([#4343](https://github.com/opensearch-project/data-prepper/issues/4343))
* Address route and subpipeline for pipeline tranformation ([#4528](https://github.com/opensearch-project/data-prepper/pull/4528))
* Add support for BigDecimal in ConvertType processor ([#4316](https://github.com/opensearch-project/data-prepper/issues/4316))
* Checkpoint records at an interval for TPS case when AckSet is enabled  ([#4526](https://github.com/opensearch-project/data-prepper/pull/4526))
* Write stream events that timeout to write to internal buffer in separate thread ([#4524](https://github.com/opensearch-project/data-prepper/pull/4524))
* Key value processor enhancements ([#4521](https://github.com/opensearch-project/data-prepper/pull/4521))
* Add bucket owner support to s3 sink ([#4504](https://github.com/opensearch-project/data-prepper/pull/4504))
* Initial work to support core data types in Data Prepper ([#4496](https://github.com/opensearch-project/data-prepper/pull/4496))
* Changing logging level for config transformation and fixing rule ([#4466](https://github.com/opensearch-project/data-prepper/pull/4466))
* Add folder-based partitioning for s3 scan source ([#4455](https://github.com/opensearch-project/data-prepper/pull/4455))
* Pipeline Configuration Transformation ([#4446](https://github.com/opensearch-project/data-prepper/pull/4446))
* Added support for multiple workers in S3 Scan Source ([#4439](https://github.com/opensearch-project/data-prepper/pull/4439))
* Bootstrap the RuleEngine package ([#4442](https://github.com/opensearch-project/data-prepper/pull/4442))
* Make s3 partition size configurable and add unit test for S3 partition creator classes ([#4437](https://github.com/opensearch-project/data-prepper/pull/4437))
* Remove creating S3 prefix path partition upfront ([#4432](https://github.com/opensearch-project/data-prepper/pull/4432))
* Change s3 sink client to async client ([#4425](https://github.com/opensearch-project/data-prepper/pull/4425))
* Create new codec for each s3 group in s3 sink ([#4410](https://github.com/opensearch-project/data-prepper/pull/4410))
* Validate the AWS account Id in the S3 source using a new annotation ([#4400](https://github.com/opensearch-project/data-prepper/pull/4400))
* Add server connections metric to http and otel sources ([#4393](https://github.com/opensearch-project/data-prepper/pull/4393))
* Log the User-Agent when Data Prepper shuts down from POST /shutdown ([#4390](https://github.com/opensearch-project/data-prepper/pull/4390))
* Add aggregate_threshold with maximum_size to s3 sink ([#4385](https://github.com/opensearch-project/data-prepper/pull/4385))
* Refactor PipelinesDataFlowModelParser to take in an InputStream instead of a file path ([#4289](https://github.com/opensearch-project/data-prepper/pull/4289))
* Add support to use old ddb stream image for REMOVE events ([#4275](https://github.com/opensearch-project/data-prepper/pull/4275))


### Bug Fixes
* Fix count aggregation exemplar data ([#4341](https://github.com/opensearch-project/data-prepper/pull/4341))
* Revert HTTP data chunking changes for kafka buffer done in PR 4266 ([#4329](https://github.com/opensearch-project/data-prepper/pull/4329))
* Fix Router performance issue ([#4327](https://github.com/opensearch-project/data-prepper/pull/4327))
* Do not require field_split_characters to not be empty for key_value processor ([#4358](https://github.com/opensearch-project/data-prepper/pull/4358))
* Do not write empty lists of DlqObject to the DLQ ([#4403](https://github.com/opensearch-project/data-prepper/pull/4403))
* Fix transient test failure for subpipelines ([#4479](https://github.com/opensearch-project/data-prepper/pull/4479))
* Fix JacksonEvent to propagate ExternalOriginalTime if its set at the time of construction ([#4489](https://github.com/opensearch-project/data-prepper/pull/4489))
* FIX: null certificate value should be valid in opensearch connection ([#4494](https://github.com/opensearch-project/data-prepper/pull/4494))
* [BUG]Incorrect Behavior of Obfuscate Processor with Predefined Pattern "%{CREDIT_CARD_NUMBER}" ([#4340](https://github.com/opensearch-project/data-prepper/issues/4340))
* [BUG] Empty DLQ entries when version conflicts occur ([#4301](https://github.com/opensearch-project/data-prepper/issues/4301))
* [BUG] otel sources should show a more clear exception when receiving data that cannot be processed based on the configured compression type ([#4022](https://github.com/opensearch-project/data-prepper/issues/4022))
* [BUG] : unable to set field_delimiter_regex ([#2946](https://github.com/opensearch-project/data-prepper/issues/2946))
* Fix aggregate processor local mode ([#4529](https://github.com/opensearch-project/data-prepper/pull/4529))
* Add `long` as a target type for `convert_entry_type` processor ([#4120](https://github.com/opensearch-project/data-prepper/issues/4120))
* Fix write json basic test ([#4527](https://github.com/opensearch-project/data-prepper/pull/4527))
* Fix depth field in template ([#4509](https://github.com/opensearch-project/data-prepper/pull/4509))
* Fix for S3PartitionCreatorScheduler ConcurrentModification Exception ([#4473](https://github.com/opensearch-project/data-prepper/pull/4473))
* Fix acknowledgements in DynamoDB ([#4419](https://github.com/opensearch-project/data-prepper/pull/4419))
* Fix DocumentDB source S3PathPrefix null or empty ([#4472](https://github.com/opensearch-project/data-prepper/pull/4472))
* Fix an issue that exception messages are masked ([#4416](https://github.com/opensearch-project/data-prepper/pull/4416))
* Fix bug where using upsert or update without routing parameter caused… ([#4397](https://github.com/opensearch-project/data-prepper/pull/4397))
* Fix bug in s3 sink dynamic bucket and catch invalid bucket message ([#4413](https://github.com/opensearch-project/data-prepper/pull/4413))
* Fix flaky PipelineConfigurationFileReaderTest ([#4386](https://github.com/opensearch-project/data-prepper/pull/4386))
* Aggregate Processor: local mode should work when there is no when condition ([#4380](https://github.com/opensearch-project/data-prepper/pull/4380))


### Security
* CVE-2024-22201 on http2-common 9.4.51 version - autoclosed ([#4452](https://github.com/opensearch-project/data-prepper/issues/4452))
* CVE-2023-22102 (High) detected in mysql-connector-j-8.0.33.jar - autoclosed ([#3920](https://github.com/opensearch-project/data-prepper/issues/3920))


### Maintenance
* Gradle 8.7 ([#4417](https://github.com/opensearch-project/data-prepper/pull/4417))
* Adds a Gradle convention plugin for Maven publication ([#4421](https://github.com/opensearch-project/data-prepper/pull/4421))
* MAINT: allow latest schema version if not specified in confluent schema ([#4453](https://github.com/opensearch-project/data-prepper/pull/4453))
* Publish expression and logstash-configuration to Maven ([#4474](https://github.com/opensearch-project/data-prepper/pull/4474))
* Create unit test report as html ([#4384](https://github.com/opensearch-project/data-prepper/pull/4384))
* Update Stream Ack Manager unit test and code refactor ([#4383](https://github.com/opensearch-project/data-prepper/pull/4383))
* Grpc exception handler: Modified to return BADREQUEST for some internal errors ([#4387](https://github.com/opensearch-project/data-prepper/pull/4387))
* Remove unexpected event handle message ([#4388](https://github.com/opensearch-project/data-prepper/pull/4388))
* Bump parquet version to 1.14.0. ([#4520](https://github.com/opensearch-project/data-prepper/pull/4520))
* Clear system property to disable s3 scan when stream worker exits, set s3 sink threshold to 15 seconds for docdb streams ([#4522](https://github.com/opensearch-project/data-prepper/pull/4522))
* ExportPartitionWorkerTest testProcessPartitionSuccess(String) failure ([#4298](https://github.com/opensearch-project/data-prepper/issues/4298))
* MAINT: inject external origination timestamp ([#4507](https://github.com/opensearch-project/data-prepper/pull/4507))
* Updates Ameria to 1.28.2 ([#4440](https://github.com/opensearch-project/data-prepper/pull/4440))
* MAINT: use authentication block in opensearch sink ([#4438](https://github.com/opensearch-project/data-prepper/pull/4438))
* MAINT: use authentication for basic credentials in opensearch source ([#4435](https://github.com/opensearch-project/data-prepper/pull/4435))
* MAINT: deprecate certificate_content with certificate_key ([#4434](https://github.com/opensearch-project/data-prepper/pull/4434))
* MAINT: deprecate plaintext with plain under sasl in kafka ([#4433](https://github.com/opensearch-project/data-prepper/pull/4433))
* MAINT: deprecate pipeline_configurations with extension ([#4428](https://github.com/opensearch-project/data-prepper/pull/4428))
* Maint/renaming kafka source plugin setting ([#4429](https://github.com/opensearch-project/data-prepper/pull/4429))