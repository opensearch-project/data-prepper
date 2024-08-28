## 2024-08-28 Version 2.9.0

---


### Features

* Support sets and set operations in Data Prepper expressions ([#3854](https://github.com/opensearch-project/data-prepper/issues/3854))
* Add startsWith expression function ([#4840](https://github.com/opensearch-project/data-prepper/pull/4840))
* Support default route option for Events that match no other route ([#4615](https://github.com/opensearch-project/data-prepper/issues/4615))
* Delete input for processors which expand the event ([#3968](https://github.com/opensearch-project/data-prepper/issues/3968))
* Dynamic Rule Detection ([#4600](https://github.com/opensearch-project/data-prepper/issues/4600))
* Kafka Source should support message headers ([#4565](https://github.com/opensearch-project/data-prepper/issues/4565))
* Aggregate processor : add option to allow raw events ([#4598](https://github.com/opensearch-project/data-prepper/pull/4598))
* Add support for start and end times in count and histogram aggregate actions ([#4614](https://github.com/opensearch-project/data-prepper/pull/4614))
* Add an option to count unique values of specified key(s) to CountAggregateAction ([#4644](https://github.com/opensearch-project/data-prepper/issues/4644))
* Flatten processor: option for keys wihout brackets ([#4616](https://github.com/opensearch-project/data-prepper/issues/4616))
* Modify Key Value processor to support string literal grouping ([#4599](https://github.com/opensearch-project/data-prepper/pull/4599))
* Make AWS credential management available in data-prepper-config.yaml ([#2570](https://github.com/opensearch-project/data-prepper/issues/2570))

### Enhancements

* Support enhanced configuration of the Kafka source and buffer loggers ([#4126](https://github.com/opensearch-project/data-prepper/issues/4126))
* Update the rename_keys and delete_entries processors to use EventKey ([#4636](https://github.com/opensearch-project/data-prepper/pull/4636))
* Update the mutate string processors to use the EventKey. ([#4649](https://github.com/opensearch-project/data-prepper/pull/4649))
* OpenSearch Sink add support for sending pipeline parameter in BulkRequest ([#4609](https://github.com/opensearch-project/data-prepper/pull/4609))
* Add support for Kafka headers and timestamp in the Kafka Source ([#4566](https://github.com/opensearch-project/data-prepper/pull/4566))


### Bug Fixes

* [BUG] Visibility duplication protection fails when using S3 source for large files and receiving 10 messages from SQS queue ([#4812](https://github.com/opensearch-project/data-prepper/issues/4812))
* [BUG] ChangeVisibilityTimeout call failure during pipeline shutdown. ([#4575](https://github.com/opensearch-project/data-prepper/issues/4575))
* [BUG] Service-map relationship should be created regardless of missing traceGroupName ([#4821](https://github.com/opensearch-project/data-prepper/issues/4821))
* [BUG] Unable to create stateful processors with multiple workers. ([#4660](https://github.com/opensearch-project/data-prepper/issues/4660))
* [BUG] Routes: regex doesn't work ([#4763](https://github.com/opensearch-project/data-prepper/issues/4763))
* [BUG] Grok plugin CLOUDFRONT_ACCESS_LOG pattern does not compile ([#4604](https://github.com/opensearch-project/data-prepper/issues/4604))
* [BUG] The user_agent processor throws exceptions with multiple threads. ([#4618](https://github.com/opensearch-project/data-prepper/issues/4618))
* [BUG] DynamoDB source export converts Numbers ending in 0 to scientific notation ([#3840](https://github.com/opensearch-project/data-prepper/issues/3840))
* Fix null document in DLQ object ([#4814](https://github.com/opensearch-project/data-prepper/pull/4814))
* Fix KeyValue Processor value grouping bug ([#4606](https://github.com/opensearch-project/data-prepper/pull/4606))


### Security

* CVE-2024-6345 (High) detected in setuptools-68.0.0-py3-none-any.whl ([#4738](https://github.com/opensearch-project/data-prepper/issues/4738))
* CVE-2024-39689 (High) detected in certifi-2023.7.22-py3-none-any.whl ([#4715](https://github.com/opensearch-project/data-prepper/issues/4715))
* CVE-2024-5569 (Low) detected in zipp-3.15.0-py3-none-any.whl ([#4714](https://github.com/opensearch-project/data-prepper/issues/4714))
* CVE-2024-3651 (High) detected in idna-3.3-py3-none-any.whl ([#4713](https://github.com/opensearch-project/data-prepper/issues/4713))
* CVE-2024-35195 (Medium) detected in requests-2.31.0-py3-none-any.whl ([#4562](https://github.com/opensearch-project/data-prepper/issues/4562))
* CVE-2024-37891 (Medium) detected in urllib3-2.0.7-py3-none-any.whl ([#4641](https://github.com/opensearch-project/data-prepper/issues/4641))


### Maintenance

* Removes Zookeeper from Data Prepper ([#4707](https://github.com/opensearch-project/data-prepper/pull/4707))
* Tools to generate User Agent strings in the performance-test project ([#4620](https://github.com/opensearch-project/data-prepper/pull/4620))
