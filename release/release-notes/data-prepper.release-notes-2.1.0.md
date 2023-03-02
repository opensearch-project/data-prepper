## 2023-03-02 Version 2.1.0

---

### Features
* Support OpenTelementry Logs ([#1372](https://github.com/opensearch-project/data-prepper/pull/1372))
* New type conversion processor ([#2010](https://github.com/opensearch-project/data-prepper/issues/2010))
* New anomaly detection processor ([#2040](https://github.com/opensearch-project/data-prepper/issues/2040))
* Support histogram action in aggregate processor ([#2078](https://github.com/opensearch-project/data-prepper/pull/2078))
* Support percent sampler action in aggregate processor ([#2094](https://github.com/opensearch-project/data-prepper/issues/2094))
* Support rate limiter action in aggregate processor ([#2088](https://github.com/opensearch-project/data-prepper/issues/2088))
* Provide a circuit breaking capability ([#2150](https://github.com/opensearch-project/data-prepper/issues/2150))

### Enhancements
* Support custom headers when assuming AWS IAM roles ([#1888](https://github.com/opensearch-project/data-prepper/issues/1888))
* Provide additional metrics for S3 source ([#2025](https://github.com/opensearch-project/data-prepper/issues/2025))
* Support "when" condition in aggregate processor actions ([#2021](https://github.com/opensearch-project/data-prepper/issues/2021))
* Support dynamic index name in OpenSearch sink ([#1459](https://github.com/opensearch-project/data-prepper/issues/1459))
* Support latest OTEL metrics features ([#1310](https://github.com/opensearch-project/data-prepper/issues/1310))
* Support loading files from S3 in OpenSearch sink ([#2120](https://github.com/opensearch-project/data-prepper/issues/2120))
* Support metric tags based on metric name pattern ([#2229](https://github.com/opensearch-project/data-prepper/issues/2229))
* Support retry when sink fails to initialize ([#936](https://github.com/opensearch-project/data-prepper/issues/936))
* Add more metrics for OpenSearch sink ([#2168](https://github.com/opensearch-project/data-prepper/issues/2168))
* Validate file content from S3 for OpenSearch sink ([#2178](https://github.com/opensearch-project/data-prepper/issues/2178))
* Add more configurable values to OTEL trace raw processor ([#2165](https://github.com/opensearch-project/data-prepper/issues/2165))
* Add more metrics for OTEL trace raw processor ([#2166](https://github.com/opensearch-project/data-prepper/issues/2166))
* Provide logging markers to filter sensitive data and events ([#1990](https://github.com/opensearch-project/data-prepper/issues/1990))
* Support sensitive data masking in logs ([#2187](https://github.com/opensearch-project/data-prepper/issues/2187))
* Emit Metrics for ACM and AWS CloudMap related calls ([#2087](https://github.com/opensearch-project/data-prepper/issues/2087))
* Add buffer overflow metric in AbstractBuffer ([#2167](https://github.com/opensearch-project/data-prepper/issues/2167))
* Support configurable read batch delay for peer-forwarder ([#2158](https://github.com/opensearch-project/data-prepper/issues/2158))
* Improve ReceiveBuffer Performance ([#2142](https://github.com/opensearch-project/data-prepper/issues/2142))
* Support OTEL scope logs ([#2115](https://github.com/opensearch-project/data-prepper/issues/2115))
* Implement batching for peer forwarder request documents ([#2197](https://github.com/opensearch-project/data-prepper/pull/2197))
* Add more metrics to OTEL metrics source ([#2283](https://github.com/opensearch-project/data-prepper/pull/2283))
* Support a version property in pipeline YAML configurations ([#2263](https://github.com/opensearch-project/data-prepper/issues/2263))
* Change the path prefix for OTEL endpoints ([#2257](https://github.com/opensearch-project/data-prepper/issues/2257))
* Change the path for HTTP source ([#2258](https://github.com/opensearch-project/data-prepper/issues/2258))
* Improve peer-forwarding serialization ([#2242](https://github.com/opensearch-project/data-prepper/issues/2242))
* Add null support to Data Prepper Expressions ([#1136](https://github.com/opensearch-project/data-prepper/issues/1136))
* Support for complex document ID and routing ID ([#1966](https://github.com/opensearch-project/data-prepper/pull/1966))
* Add PluginMetrics in the Auth Plugin for HTTP, OTEL and Metrics Source ([#2023](https://github.com/opensearch-project/data-prepper/pull/2023))
* Support Long type in expressions ([#2265](https://github.com/opensearch-project/data-prepper/pull/2265))
* Add backoff for SQS in S3 source to reduce logging ([#2326](https://github.com/opensearch-project/data-prepper/pull/2326))
* Add several performance improvement changes ([#2190](https://github.com/opensearch-project/data-prepper/pull/2190),
[#2228](https://github.com/opensearch-project/data-prepper/pull/2228),
[#2238](https://github.com/opensearch-project/data-prepper/pull/2238),
[#2239](https://github.com/opensearch-project/data-prepper/pull/2239),
[#2241](https://github.com/opensearch-project/data-prepper/pull/2241),
[#2246](https://github.com/opensearch-project/data-prepper/pull/2246),
[#2250](https://github.com/opensearch-project/data-prepper/pull/2250),
[#2290](https://github.com/opensearch-project/data-prepper/pull/2290))

### Bug Fixes
* Fix logging by updating related packages for SLF4J 2.0 ([#2113](https://github.com/opensearch-project/data-prepper/pull/2113))
* Fix null pointer exception in remote peer forwarding ([#2124](https://github.com/opensearch-project/data-prepper/pull/2124))
* Fix Data Prepper to not terminate on invalid open telemetry metric/trace data ([#2176](https://github.com/opensearch-project/data-prepper/pull/2176))
* Fix Armeria exception when under pressure ([#2274](https://github.com/opensearch-project/data-prepper/pull/2274))
* Fix release build issue ([#2345](https://github.com/opensearch-project/data-prepper/pull/2345))

### Security
* Fix CVE-2022-3509, CVE-2022-3510 ([#2079](https://github.com/opensearch-project/data-prepper/pull/2079))
* Fix CVE-2022-41881, CVE-2021-21290 and CVE-2022-41915 ([#2093](https://github.com/opensearch-project/data-prepper/pull/2093))
* Fix CVE-2020-8908, CVE-2018-10237 ([#2254](https://github.com/opensearch-project/data-prepper/pull/2254))
* Update OpenTelemetry dependencies to address CVE-2023-25577 and CVE-2023-23934 ([#2288](https://github.com/opensearch-project/data-prepper/pull/2288))
* Use secure Java serialization with Java serialization ([#2310](https://github.com/opensearch-project/data-prepper/issues/2310))

### Maintenance
* Test against multiple OTEL version ([#1963](https://github.com/opensearch-project/data-prepper/issues/1963))
* Document trace_peer_forwarder in README ([#2141](https://github.com/opensearch-project/data-prepper/issues/2141))
* Use consistent AWS pipeline configurations ([#2184](https://github.com/opensearch-project/data-prepper/issues/2184))
* Use snake case for all configurations ([#2203](https://github.com/opensearch-project/data-prepper/issues/2203))
