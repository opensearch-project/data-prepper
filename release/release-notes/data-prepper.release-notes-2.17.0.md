## 2026-11-05 Version 2.17.0

---

### Breaking Changes

* Change the `rss` source configuration to a `feeds` map and a new event schema as part of its rewrite; existing `rss` pipelines must update their configuration ([#7002](https://github.com/opensearch-project/data-prepper/issues/7002))
* Stop acknowledging undelivered events in the CloudWatch Logs sink; failed batches now retry at most `max_retries` times even without a DLQ and are negatively acknowledged when retries run out ([#7211](https://github.com/opensearch-project/data-prepper/issues/7211), [#7212](https://github.com/opensearch-project/data-prepper/pull/7212))
* Stop copying `gen_ai.*` fields and token totals onto non-GenAI root spans in the `otel_traces` processor; custom queries or dashboards that read those fields on root spans will see different data, and setting `genai_root_propagation: all_roots` restores the previous behavior ([#7210](https://github.com/opensearch-project/data-prepper/pull/7210))

### Features

* Ingest data from existing Splunk forwarders and HTTP Event Collector (HEC) clients without any client changes using the new `splunk_hec` source, which supports the standard HEC endpoints, multiple tokens with per-token defaults, indexer acknowledgments, and backpressure ([#6823](https://github.com/opensearch-project/data-prepper/issues/6823))
* Ingest multiple RSS and Atom feeds with the rewritten `rss` source, which deduplicates items, retries failed feeds independently with exponential backoff, and supports per-feed polling frequency and HTTP basic authentication ([#7002](https://github.com/opensearch-project/data-prepper/issues/7002))
* Scale Data Prepper to multiple nodes using a relational database you already run, such as PostgreSQL or MySQL, with the new `jdbc` source coordination store ([#6740](https://github.com/opensearch-project/data-prepper/issues/6740))
* Add an opt-in plugin framework backed by an embedded Apache Felix OSGi runtime ([#6760](https://github.com/opensearch-project/data-prepper/issues/6760))

### Enhancements

* Improve load balancing for horizontally scaled OpenTelemetry sources with new gRPC `max_connection_age` and `connection_drain_duration` settings that make clients periodically reconnect and discover new replicas ([#6745](https://github.com/opensearch-project/data-prepper/issues/6745))
* Support Amazon OpenSearch Service domains optimized for observability with the `trace-analytics-plain-raw-v2` index type in the OpenSearch sink ([#7188](https://github.com/opensearch-project/data-prepper/pull/7188))
* Support string values up to 64 MB in events, up from the previous 20 MB limit ([#7145](https://github.com/opensearch-project/data-prepper/issues/7145))
* Support `azure_federated` OAuth authentication for Azure Event Hubs in the Kafka source ([#7011](https://github.com/opensearch-project/data-prepper/pull/7011))
* Support Map, List, and Array types in the `length()` expression function ([#7030](https://github.com/opensearch-project/data-prepper/issues/7030))
* Attach a per-instance `instance_label` in the Prometheus sink so multiple Data Prepper nodes can safely write the same series ([#7023](https://github.com/opensearch-project/data-prepper/issues/7023))
* Write to Amazon OpenSearch Serverless time series collections by automatically setting the timestamp field in the OpenSearch sink ([#7221](https://github.com/opensearch-project/data-prepper/issues/7221))
* Support a dynamic per-event entity in the CloudWatch Logs sink ([#7032](https://github.com/opensearch-project/data-prepper/issues/7032))
* Add a `sum` action to the `aggregate` processor to sum a numeric field per group ([#7024](https://github.com/opensearch-project/data-prepper/issues/7024))
* Support custom rate limits by group in the `aggregate` processor `rate_limiter` action ([#7096](https://github.com/opensearch-project/data-prepper/issues/7096))
* Support `metadata_max_age`, `acknowledgments_timeout`, and `acknowledgments_expiry_reset` options in the Kafka source ([#7073](https://github.com/opensearch-project/data-prepper/issues/7073))
* Support `connections_max_idle` in the Kafka source ([#7016](https://github.com/opensearch-project/data-prepper/issues/7016))
* Automatically detect and skip pre-compression JSON metadata headers when decompressing files ([#7021](https://github.com/opensearch-project/data-prepper/issues/7021))
* Make the DynamoDB source stream shard discovery interval configurable ([#7000](https://github.com/opensearch-project/data-prepper/issues/7000))
* Make the acknowledgements shutdown timeout configurable in `data-prepper-config.yaml` ([#7127](https://github.com/opensearch-project/data-prepper/pull/7127))

### Bug Fixes

* Fix the Jira source failing to crawl after Atlassian removed the `/rest/api/3/search` endpoint ([#7104](https://github.com/opensearch-project/data-prepper/issues/7104), [#7143](https://github.com/opensearch-project/data-prepper/issues/7143))
* Fix the `otel_trace_source` dropping the entire OTLP request when a span has duplicate attribute keys ([#7151](https://github.com/opensearch-project/data-prepper/issues/7151))
* Fix the RDS source dropping UPDATE events that set a column to NULL on a cascading foreign key parent table ([#7129](https://github.com/opensearch-project/data-prepper/issues/7129))
* Fix the Kafka source `azure_federated` authentication stopping after the first OAuth token refresh ([#7039](https://github.com/opensearch-project/data-prepper/issues/7039))
* Fix the pull-based ingestion routing hash to match OpenSearch shard routing ([#6993](https://github.com/opensearch-project/data-prepper/issues/6993))
* Log the OpenSearch sink first-write-wins notice once per data stream instead of once per document ([#6972](https://github.com/opensearch-project/data-prepper/issues/6972))
* Parse top-level JSON arrays in the `parse_json` and `parse_ion` processors ([#5705](https://github.com/opensearch-project/data-prepper/issues/5705))
* Fix handling of JSON pointer escape sequences when setting event keys ([#6150](https://github.com/opensearch-project/data-prepper/issues/6150))
* Perform per-field conflict resolution with `overwrite_if_destination_exists` when the `key_value` processor writes to a destination ([#7091](https://github.com/opensearch-project/data-prepper/issues/7091))

### Security

* CVE-2026-59890 - setuptools 83 ([#7009](https://github.com/opensearch-project/data-prepper/pull/7009))
* CVE-2026-102598 - werkzeug 3.1.9 ([#7176](https://github.com/opensearch-project/data-prepper/pull/7176))

### Maintenance

* Make the `dissect` processor thread-safe ([#5546](https://github.com/opensearch-project/data-prepper/issues/5546))
