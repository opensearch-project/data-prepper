## 2024-03-27 Version 2.7.0

---

### Features

* Add a GeoIP processor. ([#253](https://github.com/opensearch-project/data-prepper/issues/253), [#3941](https://github.com/opensearch-project/data-prepper/issues/3941), [#3942](https://github.com/opensearch-project/data-prepper/issues/3942))
* Flatten json processor ([#4128](https://github.com/opensearch-project/data-prepper/issues/4128))
* Add select_entries processor ([#4147](https://github.com/opensearch-project/data-prepper/pull/4147))
* Decompress processor ([#4016](https://github.com/opensearch-project/data-prepper/issues/4016))
* Support parsing of XML fields in Events ([#4165](https://github.com/opensearch-project/data-prepper/issues/4165), [#4024](https://github.com/opensearch-project/data-prepper/issues/4024))
* Processor for parsing Amazon Ion documents ([#3730](https://github.com/opensearch-project/data-prepper/issues/3730))
* Append values to lists in an event ([#4129](https://github.com/opensearch-project/data-prepper/issues/4129))
* MapToList processor ([#3935](https://github.com/opensearch-project/data-prepper/issues/3935))
* Date processor to convert from epoch_second, epoch_milli, or epoch_nano ([#2929](https://github.com/opensearch-project/data-prepper/issues/2929), [#4076](https://github.com/opensearch-project/data-prepper/issues/4076))
* Support reading of old image for delete events on DynamoDB source ([#4261](https://github.com/opensearch-project/data-prepper/issues/4261))
* Add string truncate processor to the family of mutate string processor ([#3925](https://github.com/opensearch-project/data-prepper/issues/3925))
* Add join function ([#4075](https://github.com/opensearch-project/data-prepper/pull/4075))


### Enhancements

* Support format expressions for routing in the opensearch sink ([#3833](https://github.com/opensearch-project/data-prepper/issues/3833))
* Allow . and @ characters to be part of json pointer in expressions ([#4130](https://github.com/opensearch-project/data-prepper/pull/4130))
* Support maximum request length configurations in the HTTP and OTel sources ([#3931](https://github.com/opensearch-project/data-prepper/issues/3931))
* Provide a config option to do node local aggregation ([#4306](https://github.com/opensearch-project/data-prepper/pull/4306))
* Allow peer forwarder to skip sending events to remote peer ([#3996](https://github.com/opensearch-project/data-prepper/issues/3996))
* Include encrypted data key in Kafka buffer message. ([#3655](https://github.com/opensearch-project/data-prepper/issues/3655))
* Support larger message sizes in Kafka Buffer ([#3916](https://github.com/opensearch-project/data-prepper/pull/3916))
* Modify S3 Source to allow multiple SQS workers ([#4239](https://github.com/opensearch-project/data-prepper/pull/4239))
* Add support for tracking performance of individual Events in the grok processor ([#4196](https://github.com/opensearch-project/data-prepper/issues/4196))
* Support codec on the file source to help with testing ([#4018](https://github.com/opensearch-project/data-prepper/issues/4018))
* Provide a delay processor to put a delay in the processor for debugging and testing ([#3938](https://github.com/opensearch-project/data-prepper/issues/3938))
* Support ByteCount in plugin parser ([#3191](https://github.com/opensearch-project/data-prepper/issues/3191))
* Add Buffer Latency Metric ([#4237](https://github.com/opensearch-project/data-prepper/pull/4237))
* Adds an append mode to the file sink ([#3687](https://github.com/opensearch-project/data-prepper/issues/3687))


### Bug Fixes

* Attempting to evaluate if a key is null throws an Exception if the value is a List<String> for conditional expressions ([#4109](https://github.com/opensearch-project/data-prepper/issues/4109))
* Data Prepper process threads stop when processors throw exceptions ([#4103](https://github.com/opensearch-project/data-prepper/issues/4103))
* Upsert action requires existing document in OpenSearch ([#4036](https://github.com/opensearch-project/data-prepper/issues/4036))
* Many Grok failures do not tag events ([#4031](https://github.com/opensearch-project/data-prepper/issues/4031))
* Using update, upsert, or delete actions without specifying document_id crashes the pipeline with NPE ([#3988](https://github.com/opensearch-project/data-prepper/issues/3988))
* OpenSearch Sink upsert action fails to create new document if it doesn't exist already ([#3934](https://github.com/opensearch-project/data-prepper/issues/3934))
* DynamoDb source global state not found for export ([#3579](https://github.com/opensearch-project/data-prepper/issues/3579))
* Missing Configuration details in Kafka documentation ([#3157](https://github.com/opensearch-project/data-prepper/issues/3157))
* File Source fails to process large files. ([#707](https://github.com/opensearch-project/data-prepper/issues/707))
* Add key_value_when conditional to key_value processor ([#4246](https://github.com/opensearch-project/data-prepper/pull/4246))
* Adds Kafka producer metrics for buffer usage ([#4139](https://github.com/opensearch-project/data-prepper/pull/4139))
* Throw a more useful error when the S3 source is unable to determine bucket ownership ([#4021](https://github.com/opensearch-project/data-prepper/pull/4021))
* Add sts_header_overrides to s3 dlq configuration ([#3845](https://github.com/opensearch-project/data-prepper/pull/3845))
* Delay reading from the Kafka buffer as long as the circuit breaker is open ([#4135](https://github.com/opensearch-project/data-prepper/pull/4135))
* Use timer for sink latency metrics ([#4174](https://github.com/opensearch-project/data-prepper/pull/4174))
* Fix bug where process worker would shut down if a processor drops all events ([#4262](https://github.com/opensearch-project/data-prepper/pull/4262))
* Send acknowledgements to source when events are forwarded to remote peer ([#4305](https://github.com/opensearch-project/data-prepper/pull/4305))
* Injecting timestamp in index name that is not a suffix throws IllegalArgumentException ([#3957](https://github.com/opensearch-project/data-prepper/issues/3957))


### Security

* Fixes CVE-2024-29133 ([#4314](https://github.com/opensearch-project/data-prepper/issues/4314))
* Fixes CVE-2024-29131 ([#4313](https://github.com/opensearch-project/data-prepper/issues/4313))
* Fixes CVE-2023-52428 ([#4296](https://github.com/opensearch-project/data-prepper/issues/4296))
* Fixes CVE-2024-23944 ([#4290](https://github.com/opensearch-project/data-prepper/issues/4290))
* Fixes CVE-2023-51775 ([#4282](https://github.com/opensearch-project/data-prepper/issues/4282))
* Fixes CVE-2024-22201 ([#4186](https://github.com/opensearch-project/data-prepper/issues/4186))
* Fixes CVE-2024-25710 ([#4164](https://github.com/opensearch-project/data-prepper/issues/4164))
* Fixes CVE-2024-26308 ([#4163](https://github.com/opensearch-project/data-prepper/issues/4163))
* Fixes CVE-2024-21634 ([#3926](https://github.com/opensearch-project/data-prepper/issues/3926))
* Fixes CVE-2023-50570 ([#3870](https://github.com/opensearch-project/data-prepper/issues/3870))
* Fixes CVE-2023-3635 ([#3068](https://github.com/opensearch-project/data-prepper/issues/3068))


### Maintenance

* Create Kafka buffer integration tests for KMS ([#3980](https://github.com/opensearch-project/data-prepper/issues/3980), [#4040](https://github.com/opensearch-project/data-prepper/issues/4040))
* Fixes Dependabot updates are not configured for all projects ([#3301](https://github.com/opensearch-project/data-prepper/issues/3301))

