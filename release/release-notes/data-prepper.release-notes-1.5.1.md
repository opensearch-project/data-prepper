## 2022-07-08 Version 1.5.1

---

### Bug Fixes
* Fix a bug where the S3 Source failed to load all records for concatenated gzip files. This was found in some Application Load Balancer log files. [#1572](https://github.com/opensearch-project/data-prepper/pull/1572)
* The S3 Source continues to run even if errors occur reading from S3. Previously, the polling thread would stop. [#1566](https://github.com/opensearch-project/data-prepper/issues/1566)
* S3 Source poll delay will no longer sleep if some messages are received from the SQS queue. [#1567](https://github.com/opensearch-project/data-prepper/issues/1567)
