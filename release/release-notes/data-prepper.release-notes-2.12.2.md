## 2025-10-14 Version 2.12.2

---

### Security

* Require full TLS trust in OpenSearch plugins by default unless insecure is configured. Fixes CVE-2025-62371 / [GHSA-43ff-rr26-8hx4](https://github.com/opensearch-project/data-prepper/security/advisories/GHSA-43ff-rr26-8hx4). ([#6171](https://github.com/opensearch-project/data-prepper/pull/6171))
* Use standard TLS when downloading the geoop database from an HTTP URL. Fixes [GHSA-3xgr-h5hq-7299](https://github.com/opensearch-project/data-prepper/security/advisories/GHSA-3xgr-h5hq-7299). ([#6167](https://github.com/opensearch-project/data-prepper/pull/6167))
* Use the TLS protocol identifier. Fixes [GHSA-28gg-8qqj-fhh5](https://github.com/opensearch-project/data-prepper/security/advisories/GHSA-28gg-8qqj-fhh5). ([#6166](https://github.com/opensearch-project/data-prepper/pull/6166))
* Updated Netty to 4.1.125 to resolve CVE-2025-55163, CVE-2025-58057, CVE-2025-58056 ([#6085](https://github.com/opensearch-project/data-prepper/pull/6085), [#5998](https://github.com/opensearch-project/data-prepper/pull/5998))
* Updated commons-lang to 3.18.0 to resolve CVE-2025-48924 ([#6085](https://github.com/opensearch-project/data-prepper/pull/6085))
* Updated BouncyCastle to 1.81 to resolve CVE-2025-8916 ([#6085](https://github.com/opensearch-project/data-prepper/pull/6085))

### Maintenance

* Update the bundled JDK to 17.0.16. ([#6172](https://github.com/opensearch-project/data-prepper/pull/6172))
* Require the smoke tests to pass before attempting to promote. ([#6086](https://github.com/opensearch-project/data-prepper/pull/6086))
