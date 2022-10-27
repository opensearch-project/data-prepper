## 2022-10-24 Version 2.0.1

---

### Bug Fixes

* Sending events to multiple pipelines would result in unexpected behavior because the event was mutated in all pipelines. Duplicate all events sent through the `pipeline` sink. [#1886](https://github.com/opensearch-project/data-prepper/issues/1886)
* Support read S3 objects when keys have spaces. [#1923](https://github.com/opensearch-project/data-prepper/issues/1923).
* Delete `s3:TestEvent` objects in the S3 source to avoid those events getting stuck. [#1924](https://github.com/opensearch-project/data-prepper/issues/1924)
* Correctly parse duration values of `0s` or `0ms`. [#1910](https://github.com/opensearch-project/data-prepper/pull/1910)

### Security

* Updates protobuf-java-util to 3.23.7 due to CVE-2022-3171. [#1945](https://github.com/opensearch-project/data-prepper/pull/1945)
* Updates Jackson to 2.13.4.2 due to CVE-2022-42003. [#1933](https://github.com/opensearch-project/data-prepper/pull/1933)

### Maintenance

* Support multiple threads in the Data Prepper core server. [#1951](https://github.com/opensearch-project/data-prepper/pull/1951)
* Run smoke tests against OpenSearch 1.3.6. [#1962](https://github.com/opensearch-project/data-prepper/pull/1962)
* Use Python grpcio 1.50.0 in smoke tests to reduce time to run. [#1959](https://github.com/opensearch-project/data-prepper/pull/1959)
* Reduce test flakiness in `RandomStringSourceTests`. [#1964](https://github.com/opensearch-project/data-prepper/pull/1964)
* Reduce smoke test timeout to 8 minutes from 30 minutes. [#1965](https://github.com/opensearch-project/data-prepper/pull/1965)
