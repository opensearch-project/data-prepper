
# Data Prepper

We envision Data Prepper as an open source data collector for observability data (trace, logs, metrics) that can filter, enrich, transform, normalize, and aggregate data for downstream analysis and visualization. It will support stateful processing across multiple instances of data pipelines for observability use cases such as distributed tracing and multi-line log events (e.g. stack traces, aggregations, and log-to-metric transformations). Currently Data Prepper supports processing of distributed trace data and will support processing of logs and metric data in the future. 

## Table of Contents

- [Overview](docs/readme/overview.md)
- Trace Analytics
  - [Overview](docs/readme/trace_overview.md)
  - [Trace Analytics Setup](docs/readme/trace_setup.md)
  - [Scaling and Tuning](docs/readme/trace_tuning.md)
- Project Details
  - [Setup](docs/readme/project_setup.md)
  - [Error Handling](docs/readme/error_handling.md)
  - [Logging](docs/readme/logs.md)
  - [Monitoring](docs/readme/monitoring.md)
  - [Contribute](#Contribute)
  - [Code of Conduct](#Code-of-Conduct)
  - [Security Issue Notifications](#Security-Issue-Notifications)
  - [License](#License)



## Contribute

We invite developers from the larger OpenSearch community to contribute and help improve test coverage and give us feedback on where improvements can be made in design, code and documentation. You can look at  [contribution guide](CONTRIBUTING.md) for more information on how to contribute.

## Code of Conduct

This project has adopted an [Open Source Code of Conduct](CODE_OF_CONDUCT.md).

## Security Issue Notifications

If you discover a potential security issue in this project, please refer to the [security policy](https://github.com/opensearch-project/data-prepper/security/policy).

## License

This library is licensed under the Apache 2.0 License. [Refer](LICENSE)

## Copyright

Copyright OpenSearch Contributors. See [NOTICE](NOTICE.txt) for details.