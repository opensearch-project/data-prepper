## 2026-07-01 Version 2.16.0

---

### Breaking Changes

* Default to point-in-time for the OpenSearch source on Amazon OpenSearch Serverless ([#6335](https://github.com/opensearch-project/data-prepper/issues/6335))

### Features

* Add experimental pull-based ingestion to write to an existing OpenSearch index through Kafka ([#6835](https://github.com/opensearch-project/data-prepper/issues/6835))
* Continuously tail files with the `file` source, including offset tracking, rotation detection, and glob patterns ([#6782](https://github.com/opensearch-project/data-prepper/issues/6782))
* Add `filter_list` processor to keep only the array elements matching a condition ([#6610](https://github.com/opensearch-project/data-prepper/issues/6610))
* Scrape metrics from Prometheus endpoints with the pull-based Prometheus source ([#1997](https://github.com/opensearch-project/data-prepper/issues/1997))
* Support writing metrics to OpenSearch TSDB indices with `index_type: tsdb` in the OpenSearch sink ([#6644](https://github.com/opensearch-project/data-prepper/issues/6644))
* Convert OpenTelemetry traces into span events with a new codec ([#6650](https://github.com/opensearch-project/data-prepper/issues/6650))

### Enhancements

* Create the log group and log stream automatically in the CloudWatch Logs sink ([#6861](https://github.com/opensearch-project/data-prepper/issues/6861))
* Attach Entity attributes to requests in the CloudWatch Logs sink ([#6860](https://github.com/opensearch-project/data-prepper/issues/6860))
* Read the Confluence and Jira bearer token from a secrets manager ([#6844](https://github.com/opensearch-project/data-prepper/issues/6844))
* Support legacy MD5 checksum validation for S3-compatible storage ([#6780](https://github.com/opensearch-project/data-prepper/issues/6780))
* Support log signals, a configurable SigV4 signing service, and additional headers in the OTLP sink ([#6763](https://github.com/opensearch-project/data-prepper/issues/6763))
* Add a source-layer shuffle to the Iceberg source for correct and scalable CDC processing ([#6666](https://github.com/opensearch-project/data-prepper/issues/6666))
* Connect to an OpenSearch instance behind a reverse proxy in the OpenSearch sink ([#6654](https://github.com/opensearch-project/data-prepper/issues/6654))
* Support Confluence and Jira Data Center by allowing local addresses ([#6496](https://github.com/opensearch-project/data-prepper/issues/6496))
* Filter S3 objects by prefix and suffix for both SQS and scan in the S3 source ([#6386](https://github.com/opensearch-project/data-prepper/issues/6386))
* Support path-style access in the S3 source ([#6340](https://github.com/opensearch-project/data-prepper/issues/6340))
* Discover indexes with a single scan in the OpenSearch source ([#6169](https://github.com/opensearch-project/data-prepper/issues/6169))
* Support named credentials in the AWS extension ([#4637](https://github.com/opensearch-project/data-prepper/issues/4637))
* Support conditional script updates of documents in the OpenSearch sink ([#3563](https://github.com/opensearch-project/data-prepper/issues/3563))
* Support client certificate authentication for OpenSearch ([#633](https://github.com/opensearch-project/data-prepper/issues/633))
* Split array fields into separate events with the `split_event` processor ([#5707](https://github.com/opensearch-project/data-prepper/issues/5707))

### Bug Fixes

* Fix the `file` source re-reading a file indefinitely when using a codec in non-tail mode ([#6934](https://github.com/opensearch-project/data-prepper/issues/6934))
* Prevent the CloudWatch Logs sink uploader thread from silently terminating on unchecked errors ([#6887](https://github.com/opensearch-project/data-prepper/issues/6887))
* Safely handle non-String `PluginConfigVariable` values in the Confluence and Jira OAuth2 configuration ([#6874](https://github.com/opensearch-project/data-prepper/issues/6874))
* Resolve `derived.environment` from resource attributes in the `otel_apm_service_map` processor instead of always returning `generic:default` ([#6786](https://github.com/opensearch-project/data-prepper/issues/6786))
* Fix cardinality explosion and Prometheus compatibility in `otel_apm_service_map` metrics ([#6710](https://github.com/opensearch-project/data-prepper/issues/6710))
* Fix the Iceberg source initial-load completion detection race between leader and worker ([#6686](https://github.com/opensearch-project/data-prepper/issues/6686))
* Continue reading from the OpenSearch source when some documents fail to load, logging and counting the failures ([#6337](https://github.com/opensearch-project/data-prepper/issues/6337))
* Accept escaped JSON pointer syntax in processor keys such as `rename_keys` ([#5121](https://github.com/opensearch-project/data-prepper/issues/5121))
* Fix `delete_source` removing the parsed field when writing to root in the `parse_json` processor ([#6443](https://github.com/opensearch-project/data-prepper/pull/6443))

### Security

* CVE-2026-6322, CVE-2026-6321, CVE-2026-45149 - aws-cdk-lib 2.253.1 ([#6913](https://github.com/opensearch-project/data-prepper/pull/6913))

### Maintenance

* Update the release process for OpenSearch project organization changes ([#6912](https://github.com/opensearch-project/data-prepper/issues/6912))
* Support automatic plugin loading in Data Prepper core ([#4838](https://github.com/opensearch-project/data-prepper/issues/4838))
* Support experimental features within Data Prepper plugins ([#6811](https://github.com/opensearch-project/data-prepper/pull/6811))
* Fix the flaky DefaultAcknowledgementSetManagerTests ([#6719](https://github.com/opensearch-project/data-prepper/issues/6719))
* Fix the KafkaSourceJsonTypeIT ClassCastException on the kafka_headers cast ([#6865](https://github.com/opensearch-project/data-prepper/issues/6865))
