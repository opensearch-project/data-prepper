## 2023-07-12 Version 2.3.2

---

### Bug Fixes
* Updated the release date ([#2912](https://github.com/opensearch-project/data-prepper/pull/2912))
* Fix addTags API in EventMetadata ([#2996](https://github.com/opensearch-project/data-prepper/pull/2996))
* Fix DLQ writer writing empty list ([#2998](https://github.com/opensearch-project/data-prepper/pull/2998))
* Fix SqsWorker error messages ([#3002](https://github.com/opensearch-project/data-prepper/pull/3002))
* Fix S3 errors around end of file behavior. ([#3006](https://github.com/opensearch-project/data-prepper/pull/3006))
* Retry s3 reads on socket exceptions. ([#3008](https://github.com/opensearch-project/data-prepper/pull/3008))
* Fix race condition in SqsWorker when acknowledgements are enabled ([#3010](https://github.com/opensearch-project/data-prepper/pull/3010))
* Remove validation that made keys starting or ending with . - or _ invalid ([#3007](https://github.com/opensearch-project/data-prepper/pull/3007))

### Security
* Fix CVE-2023-35165, CVE-2023-34455, CVE-2023-34453, CVE-2023-34454, CVE-2023-2976 ([#2952](https://github.com/opensearch-project/data-prepper/pull/2952))
* Fix bucket ownership validation. This was a regression introduced in Data Prepper 2.3. ([#3011](https://github.com/opensearch-project/data-prepper/pull/3011))