## 2025-04-24 Version 2.11.0

---

### Breaking Changes

* The Docker task no longer runs as root. This may break existing Docker compose configurations. For example, mapping the `.aws` directory to the `/root/.aws` directory should map it to `/.aws` instead.

### Features

* Support AWS Aurora/RDS PostgreSQL as source ([#5309](https://github.com/opensearch-project/data-prepper/issues/5309))
* Support SQS as a Source ([#1049](https://github.com/opensearch-project/data-prepper/issues/1049))
* Jira Connector - to seamlessly sync all the ticket details to OpenSearch ([#4754](https://github.com/opensearch-project/data-prepper/issues/4754))
* AWS Lambda as Processor and Sink ([#4699](https://github.com/opensearch-project/data-prepper/issues/4699))
* Support AWS Aurora/RDS MySQL as source ([#4561](https://github.com/opensearch-project/data-prepper/issues/4561))
* AWS Lambda as Sink ([#4170](https://github.com/opensearch-project/data-prepper/issues/4170))
* Integrate OpenSearch Ml-Commons into Data Prepper ([#5509](https://github.com/opensearch-project/data-prepper/issues/5509))

### Enhancements

* Update OTel Protobuf specification ([#5434](https://github.com/opensearch-project/data-prepper/pull/5434))
* Data Prepper support for dynamic renaming of keys ([#4849](https://github.com/opensearch-project/data-prepper/issues/4849))
* S3 DLQ should pass expected bucket owner to PutObject request ([#5498](https://github.com/opensearch-project/data-prepper/issues/5498))
* AWS Secrets Manager Plugin does not support sts_header_overrides ([#5475](https://github.com/opensearch-project/data-prepper/issues/5475))
* Support configurable stream read constraints max length in the JSON input codec ([#5466](https://github.com/opensearch-project/data-prepper/issues/5466))
* Support reading S3 object metadata only ([#5433](https://github.com/opensearch-project/data-prepper/issues/5433))
* Kafka local AWS Glue registry support ([#5377](https://github.com/opensearch-project/data-prepper/issues/5377))
* Address Scale Items for Lambda Processor and Sink ([#5031](https://github.com/opensearch-project/data-prepper/issues/5031))
* Add support to skip remote peer forwarding based on configuration ([#5127](https://github.com/opensearch-project/data-prepper/pull/5127))
* Add index_types for OTel logs and metrics #3148 ([#3929](https://github.com/opensearch-project/data-prepper/pull/3929))
* Send RetryInfo on OTel timeouts to improve or clarify backpressure on OTel gRPC sources ([#4294](https://github.com/opensearch-project/data-prepper/pull/4294), [#4119](https://github.com/opensearch-project/data-prepper/issues/4119))
* Enhance Lambda processor to retry based on certain class of exception ([#5340](https://github.com/opensearch-project/data-prepper/issues/5340))
* Support multiple delete_when condition in delete_entries processor ([#5315](https://github.com/opensearch-project/data-prepper/issues/5315))
* Add additional index_types ([#3148](https://github.com/opensearch-project/data-prepper/issues/3148))
* Add experimental feature concept ([#2695](https://github.com/opensearch-project/data-prepper/issues/2695))

### Bug Fixes

* NullPointerException on S3 Delete Event Due to Null Object Size ([#5448](https://github.com/opensearch-project/data-prepper/issues/5448))
* Index Template with flat_object type field fails during deserialization ([#5425](https://github.com/opensearch-project/data-prepper/issues/5425))
* DynamoDB source with acknowledgements expires frequently ([#5412](https://github.com/opensearch-project/data-prepper/issues/5412))
* Kinesis source doesn't pass the given polling retrieval config to underlying KCL ([#5269](https://github.com/opensearch-project/data-prepper/issues/5269))
* UTF-8 Character Encoding Issues in opensearchproject/data-prepper container ([#5238](https://github.com/opensearch-project/data-prepper/issues/5238))
* Validate that routes configured in the sink exist on startup of Data Prepper ([#5106](https://github.com/opensearch-project/data-prepper/issues/5106))
* Escaping of "/" in json pointers ([#5101](https://github.com/opensearch-project/data-prepper/issues/5101))
* DynamoDB Source doesn't support parsing data with Control Characters ([#5027](https://github.com/opensearch-project/data-prepper/issues/5027))

### Security

* [docker] Run as non root ([#5311](https://github.com/opensearch-project/data-prepper/issues/5311))
* werkzeug-3.0.3-py3-none-any.whl: 2 vulnerabilities (highest severity is: 7.5) ([#5122](https://github.com/opensearch-project/data-prepper/issues/5122))

### Maintenance

* Migrate existing plugins to use POJO configuration classes. ([#5246](https://github.com/opensearch-project/data-prepper/issues/5246))
* Support examples in documentation ([#5077](https://github.com/opensearch-project/data-prepper/issues/5077))
* End-to-end log test failing when index name has DateTime pattern suffix in OpenSearch Sink Config ([#984](https://github.com/opensearch-project/data-prepper/issues/984))

