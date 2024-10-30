<img src="https://raw.githubusercontent.com/opensearch-project/data-prepper/main/docs/images/DataPrepper_auto.svg" height="64px" alt="OpenSearch Data Prepper">

[![codecov](https://codecov.io/gh/opensearch-project/data-prepper/branch/main/graph/badge.svg?token=IS7GOIY622)](https://codecov.io/gh/opensearch-project/data-prepper)
# OpenSearch Data Prepper

We envision OpenSearch Data Prepper as an open source data collector for observability data (trace, logs, metrics) that can filter, enrich, transform, normalize, and aggregate data for downstream analysis and visualization. It will support stateful processing across multiple instances of data pipelines for observability use cases such as distributed tracing and multi-line log events (e.g. stack traces, aggregations, and log-to-metric transformations). Currently OpenSearch Data Prepper supports processing of distributed trace data and log ingestion with plans to support metric data in the future. 

Please read the [Overview](docs/overview.md) to understand what OpenSearch Data Prepper is and how it works.

## Getting Started

Our [Getting Started](docs/getting_started.md) guide is the best starting point for anybody who wants to run OpenSearch Data Prepper.

Please read the [Trace Analytics](docs/trace_analytics.md) guide or [Log Analytics](docs/log_analytics.md) to get started with using OpenSearch Data Prepper for trace or log analytics use cases.

## Project Resources

* [Downloads](https://opensearch.org/downloads.html)
* [Documentation](https://opensearch.org/docs/latest/clients/data-prepper/index/)
* [Configuration Reference](https://opensearch.org/docs/latest/clients/data-prepper/data-prepper-reference/)
* Need help? Try the [OpenSearch Data Prepper category](https://discuss.opendistrocommunity.dev/c/data-prepper/61) in the OpenSearch forums
* [Project Principles](https://opensearch.org/#principles)

## Contribute

We invite developers from the larger OpenSearch community to contribute and help improve test coverage and give us feedback on where improvements can be made in design, code and documentation. You can look at [contribution guide](CONTRIBUTING.md) for more information on how to contribute.

If you are looking to contribute code, or just to build from source, please see our [Developer Guide](docs/developer_guide.md).

## Code of Conduct

This project has adopted an [Open Source Code of Conduct](CODE_OF_CONDUCT.md).

## Security Issue Notifications

If you discover a potential security issue in this project, please refer to the [security policy](https://github.com/opensearch-project/data-prepper/security/policy).

## License

This library is licensed under the [Apache 2.0 License](LICENSE)

## Copyright

Copyright OpenSearch Contributors. See [NOTICE](NOTICE) for details.
