## 2026-04-06 Version 2.15.0

---

### Breaking Changes


### Features

* Support Prometheus Remote Write v1 as an experimental source ([#6533](https://github.com/opensearch-project/data-prepper/issues/6533))
* Add experimental Iceberg CDC source plugin for capturing row-level changes from Apache Iceberg tables ([#6552](https://github.com/opensearch-project/data-prepper/issues/6552))
* Add experimental S3 Enrich processor to merge enrichment data from S3 into pipeline events ([#5992](https://github.com/opensearch-project/data-prepper/pull/5992))
* Add substring expression functions: `substringAfter`, `substringBefore`, `substringAfterLast`, `substringBeforeLast` ([#6612](https://github.com/opensearch-project/data-prepper/issues/6612))
* Add `generateUuid()` expression function for the add_entries processor ([#6653](https://github.com/opensearch-project/data-prepper/pull/6653))
* Support function composition in expressions ([#6322](https://github.com/opensearch-project/data-prepper/issues/6322))

### Enhancements

* Support server-side encryption with KMS and DSSE-KMS in the S3 sink ([#6528](https://github.com/opensearch-project/data-prepper/issues/6528))
* Support open source Prometheus in the prometheus sink ([#6594](https://github.com/opensearch-project/data-prepper/issues/6594))
* Support getting event size via expressions ([#6278](https://github.com/opensearch-project/data-prepper/issues/6278))
* Mark the SQS sink as generally available ([#6661](https://github.com/opensearch-project/data-prepper/pull/6661))
* Rename `otel_*` source plugins to `otlp_*` for consistency while continuing to allow old names ([#6530](https://github.com/opensearch-project/data-prepper/pull/6530))

### Bug Fixes

* Fix Prometheus sink NullPointerException when unit or aggregationTemporality is null ([#6683](https://github.com/opensearch-project/data-prepper/issues/6683))
* Fix error syntax in logs-otel-v1 index template ([#6646](https://github.com/opensearch-project/data-prepper/issues/6646))
* Fix default dlq_pipeline not receiving failed events from the OpenSearch sink ([#6643](https://github.com/opensearch-project/data-prepper/issues/6643))
* Fix invalid document version events still included in OpenSearch bulk requests ([#6601](https://github.com/opensearch-project/data-prepper/issues/6601))
* Fix KMS encryption plugin cache using incorrect key type for lookups ([#6636](https://github.com/opensearch-project/data-prepper/pull/6636))
* Fix empty plugin configurations failing after Jackson upgrade ([#6598](https://github.com/opensearch-project/data-prepper/pull/6598))

### Security

* CVE-2026-25645 - requests 2.33.0 ([#6677](https://github.com/opensearch-project/data-prepper/pull/6677))
* CVE-2026-33750, CVE-2026-33532 - aws-cdk-lib 2.247.0 ([#6715](https://github.com/opensearch-project/data-prepper/pull/6715))

### Maintenance

