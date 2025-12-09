## 2025-11-24 Version 2.13.0

---

### Features
* Support Prometheus as a sink (remote-write model) ([#3028](https://github.com/opensearch-project/data-prepper/issues/3028))
* Support cross-region access for S3 buckets for both SQS and scan pipelines ([#4470](https://github.com/opensearch-project/data-prepper/issues/4470), [#4811](https://github.com/opensearch-project/data-prepper/issues/4811))
* Support for sending logs to OpenSearch sata streams ([#2037](https://github.com/opensearch-project/data-prepper/issues/2037))


### Enhancements
* Delete entries processor enhancements using regular expressions ([#6087](https://github.com/opensearch-project/data-prepper/issues/6087))
* DataPrepper S3-SQS source config should support `data_selection` similar to S3 Scan configs ([#6149](https://github.com/opensearch-project/data-prepper/issues/6149))
* Support customer STS header overrides for default configuration ([#6078](https://github.com/opensearch-project/data-prepper/issues/6078))
* Improve performance of Data Prepper expressions ([#6020](https://github.com/opensearch-project/data-prepper/issues/6020))
* Support `jsonl` file extension in S3 sink ([#5993](https://github.com/opensearch-project/data-prepper/issues/5993))
* Support adding nested keys in each element for `iterate_on` with add_entries ([#5910](https://github.com/opensearch-project/data-prepper/issues/5910))
* Support for HTTP headers in the CloudWatch Logs sink ([#5905](https://github.com/opensearch-project/data-prepper/issues/5905))
* Support alternative extension implementations ([#5792](https://github.com/opensearch-project/data-prepper/issues/5792))
* Rename keys processor can normalize keys ([#5757](https://github.com/opensearch-project/data-prepper/issues/5757))
* Support for Kafka consumer config: `isolation.level` ([#5896](https://github.com/opensearch-project/data-prepper/issues/5896))
* Allow extensions to depend upon other extensions ([#2825](https://github.com/opensearch-project/data-prepper/issues/2825))
* Add EMF config that enables adding extra properties to the EMF record ([#6259](https://github.com/opensearch-project/data-prepper/pull/6259))
* Add TokenPaginationCrawler for SAAS plugins ([#6007](https://github.com/opensearch-project/data-prepper/issues/6007))


### Bug Fixes
* S3 sink without path_prefix provided is failing to write ([#6090](https://github.com/opensearch-project/data-prepper/issues/6090))
* Lambda Processor ClassCastException with Aggregate Processor ([#6054](https://github.com/opensearch-project/data-prepper/issues/6054))
* LocalDBService fails to handle existing files during download ([#5899](https://github.com/opensearch-project/data-prepper/issues/5899))
* Creates and deletes in the same bulk request can result in 404 ([#5521](https://github.com/opensearch-project/data-prepper/issues/5521))
* Events are missing from S3 sink even with end-to-end acknowledgements ([#5413](https://github.com/opensearch-project/data-prepper/issues/5413))
* Data Prepper Expression string starting in `/` is considered escaped Json Pointer ([#1188](https://github.com/opensearch-project/data-prepper/issues/1188))
* Checkpoint acknowledgments for DynamoDB pipelines ([#4764](https://github.com/opensearch-project/data-prepper/issues/4764))


### Security
* aws-cdk-lib-2.190.0.tgz: 1 vulnerabilities (highest severity is: 3.1) - autoclosed ([#5928](https://github.com/opensearch-project/data-prepper/issues/5928))
* CVE-2024-7254 in com.google.protobuf:protobuf-java (caused by com.linecorp.armeria:armeria-grpc) ([#5890](https://github.com/opensearch-project/data-prepper/issues/5890))
* aws-cdk-lib-2.88.0.tgz: 1 vulnerabilities (highest severity is: 3.1) - autoclosed ([#5780](https://github.com/opensearch-project/data-prepper/issues/5780))
* aws-cdk-lib-2.100.0.tgz: 2 vulnerabilities (highest severity is: 3.9) - autoclosed ([#5779](https://github.com/opensearch-project/data-prepper/issues/5779))


### Maintenance
* Initial work for Data Prepper plugin test framework with standard, simple tests ([#5908](https://github.com/opensearch-project/data-prepper/issues/5908))
* Publish Maven artifacts as snapshots ([#5796](https://github.com/opensearch-project/data-prepper/issues/5796))
* Use end-to-end tests for smoke tests ([#3524](https://github.com/opensearch-project/data-prepper/issues/3524))
