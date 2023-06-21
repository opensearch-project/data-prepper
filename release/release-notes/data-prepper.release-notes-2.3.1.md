## 2023-06-20 Version 2.3.1

---

### Enhancements
* Add support for external ID when making STS AssumeRole call ([#2862](https://github.com/opensearch-project/data-prepper/pull/2862))
* Remove sensitive from the error log for index name format failure in OpenSearch sink ([#2894](https://github.com/opensearch-project/data-prepper/pull/2894))

### Bug Fixes
* Fix errors when SQS notifications are from AWS EventBridge ([#2861](https://github.com/opensearch-project/data-prepper/pull/2861), [#2789](https://github.com/opensearch-project/data-prepper/pull/2789), [#2788](https://github.com/opensearch-project/data-prepper/pull/2788))
* Fix concurrentModification in CredentialsCache ([#2876](https://github.com/opensearch-project/data-prepper/pull/2876))
* Fix suppressed exception and added logs when incorrect compression is configures in S3 source ([#2879](https://github.com/opensearch-project/data-prepper/pull/2879), [#2896](https://github.com/opensearch-project/data-prepper/pull/2896))
* Fix S3 sink metrics names as they are conflicting with S3 source ([#2887](https://github.com/opensearch-project/data-prepper/pull/2887))
* Fix silent dropping data when index has null keys ([#2885](https://github.com/opensearch-project/data-prepper/pull/2885))

### Security
* Fix CVE-2017-1000487, CVE-2022-4244, CVE-2022-4245 ([#2848](https://github.com/opensearch-project/data-prepper/pull/2848))
