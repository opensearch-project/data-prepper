## 2021-xx-xx Version 1.1.0

---

**Note:** These commits were originally contributed to the Open Distro for Elasticsearch Data Prepper [repo](https://github.com/opendistro-for-elasticsearch/data-prepper), they were migrated to this repo in [#191](https://github.com/opensearch-project/data-prepper/pull/191). This set of release notes links back to the original ODFE pull requests for posterity.

### Features
* AWS Certificate Manager integration for both the OTel Trace Source and Peer Forwader plugins. Allows private keys and certificates to be fetched from ACM when establishing connections using TLS. ([#705](https://github.com/opendistro-for-elasticsearch/data-prepper/pull/705), [#726](https://github.com/opendistro-for-elasticsearch/data-prepper/pull/726))
* AWS S3 integration for both the OTel Trace Source and Peer Forwader plugins. Allows private keys and certificates to be fetched from S3 when establishing connections using TLS. ([#711](https://github.com/opendistro-for-elasticsearch/data-prepper/pull/711), [#726](https://github.com/opendistro-for-elasticsearch/data-prepper/pull/726))
* AWS Cloud Map integration. Cloud Map can be used as a peer discovery provider when operating a Data Prepper cluster. ([#734](https://github.com/opendistro-for-elasticsearch/data-prepper/pull/734))

### Enhancements
* The PeerForwarder target port is now configurable. ([#742](https://github.com/opendistro-for-elasticsearch/data-prepper/pull/742))
* The AWS IAM role to be assumed by the OpenSearch sink is now configurable. ([#736](https://github.com/opendistro-for-elasticsearch/data-prepper/pull/736))
* Multiple Micrometer metric registries can now be configured. The default remains Prometheus, however an AWS CloudWatch registry provider has been added. ([#585](https://github.com/opendistro-for-elasticsearch/data-prepper/pull/585))
* The OTel Trace Source's gRPC server can be configured to accept 'application/json' requests in addition to gRPC requests. ([#607](https://github.com/opendistro-for-elasticsearch/data-prepper/pull/607))

### Bug Fixes
* Increased number of threads that write to sinks (previously hardcoded to 1). ([#681](https://github.com/opendistro-for-elasticsearch/data-prepper/pull/681))
* Increased hash ring vNode count to better distribute requests made by the Peer Forwarder plugin. ([#806](https://github.com/opendistro-for-elasticsearch/data-prepper/pull/806))
* Fixed the OTel Trace Source's health check service to actually be configurable, previously it was always enabled regardless of config settings. ([#733](https://github.com/opendistro-for-elasticsearch/data-prepper/pull/733))
* Provided a better exception message when an OpenSearch index alias name is already taken by a concrete index. ([#588](https://github.com/opendistro-for-elasticsearch/data-prepper/pull/588))


### Refactoring
* Created a dedicated threadpool to enable async peer requests. Previously requests were synchronous and performed poorly in clusters with high peer counts. ([#777](https://github.com/opendistro-for-elasticsearch/data-prepper/pull/777))
* Changed MapDB usage from disk to memory to improve performance. A 10x speed improvement was observed. ([#671](https://github.com/opendistro-for-elasticsearch/data-prepper/pull/671))
* Fixed a race condition in the Service Map Stateful Prepper which could cause threads to stop responding. ([#671](https://github.com/opendistro-for-elasticsearch/data-prepper/pull/671))
* Improved GC profile, re-used Java objects to limit the number of survivor objects in JVM heap. ([#671](https://github.com/opendistro-for-elasticsearch/data-prepper/pull/671))
* Added an index template version check on startup to ensure old versions of Data Prepper won't overwrite newer index templates. ([#608](https://github.com/opendistro-for-elasticsearch/data-prepper/pull/608))
* Misc refactoring of OTel Trace Raw Prepper, removed the DelayQueue in favor of a cache to improve performance. ([#681](https://github.com/opendistro-for-elasticsearch/data-prepper/pull/681))

