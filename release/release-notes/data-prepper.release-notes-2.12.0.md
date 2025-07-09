## 2025-06-26 Version 2.12.0

---

### Breaking Changes


### Features

* OTel telemetry unified source ([#5596](https://github.com/opensearch-project/data-prepper/issues/5596))
* Add SQS sink to Data Prepper ([#5634](https://github.com/opensearch-project/data-prepper/issues/5634))
* KDS cross account stream ([#5687](https://github.com/opensearch-project/data-prepper/pull/5687))
* Add otlp sink (for AWS X-Ray) ([#5663](https://github.com/opensearch-project/data-prepper/issues/5663))
* Adds the Modulus operator to Data Prepper expressions ([#5685](https://github.com/opensearch-project/data-prepper/issues/5685))
* Add API tokens as an Authc/z method for OpenSearch Sink ([#5549](https://github.com/opensearch-project/data-prepper/issues/5549))
* Add support for convering keys to lowercase/uppercase in RenameKeyProcessor ([#5810](https://github.com/opensearch-project/data-prepper/pull/5810))
* Add multi-line csv support ([#5784](https://github.com/opensearch-project/data-prepper/pull/5784))
* Add auto conversion option to convert_type processor ([#5782](https://github.com/opensearch-project/data-prepper/pull/5782))
* Add detect format processor ([#5774](https://github.com/opensearch-project/data-prepper/pull/5774))
* Add `getEventType()` expression function ([#5686](https://github.com/opensearch-project/data-prepper/pull/5686))

### Enhancements

* Allow plugins to access the default pipeline role ([#4958](https://github.com/opensearch-project/data-prepper/issues/4958))
* Allow disabling metrics ([#5431](https://github.com/opensearch-project/data-prepper/issues/5431))
* Support enabling specific experimental plugins ([#5675](https://github.com/opensearch-project/data-prepper/issues/5675))
* Make opensearch source scroll timeout configurable and increase default value ([#5679](https://github.com/opensearch-project/data-prepper/issues/5679))


### Bug Fixes

* [BUG] OpenTelemetry Spans are indexed using the span id causing collisions ([#5370](https://github.com/opensearch-project/data-prepper/issues/5370))
* [BUG] _default route no longer seems to exist in 2.11.0 ([#5763](https://github.com/opensearch-project/data-prepper/issues/5763))
* Fix kafka source with glue registry ([#5765](https://github.com/opensearch-project/data-prepper/pull/5765))

### Security

* protobuf-3.19.5-cp39-cp39-manylinux_2_17_x86_64.manylinux2014_x86_64.whl: 1 vulnerabilities (highest severity is: 7.5) ([#5802](https://github.com/opensearch-project/data-prepper/issues/5802))
* urllib3-2.2.2-py3-none-any.whl: 2 vulnerabilities (highest severity is: 5.3) ([#5801](https://github.com/opensearch-project/data-prepper/issues/5801))
* protobuf-3.20.3-cp39-cp39-manylinux_2_5_x86_64.manylinux1_x86_64.whl: 1 vulnerabilities (highest severity is: 7.5) ([#5800](https://github.com/opensearch-project/data-prepper/issues/5800))
* urllib3-1.26.19-py2.py3-none-any.whl: 2 vulnerabilities (highest severity is: 5.3) ([#5799](https://github.com/opensearch-project/data-prepper/issues/5799))


### Maintenance

* Refactor maven downloading logic to be dynamic ([#5826](https://github.com/opensearch-project/data-prepper/pull/5826))
