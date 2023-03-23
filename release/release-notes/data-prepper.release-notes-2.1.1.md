## 2023-03-16 Version 2.1.1

---

### Bug Fixes
* Removed traceState from Link as it is not required ([#2363](https://github.com/opensearch-project/data-prepper/pull/2363))
* Fix IllegalArgumentException in PluginMetrics when null value for pipeline name is passed from OTel sources ([#2369](https://github.com/opensearch-project/data-prepper/pull/2369))

### Maintenance
* Removed constraints on Netty  and use version supplied by dependencies ([#2031](https://github.com/opensearch-project/data-prepper/pull/2031))
