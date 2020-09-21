# simple-ingest-transformation-utility-pipeline

## Overview

Simple Ingest Transformation Utility Pipeline (SITUP) is an open source, lightweight ingestion service that provides customers with abilities to filter, enrich, transform, normalize and aggregate data for analytics.

*Please note that SITUP is under active development.*

This project contains the following top level components:

* [situp-api](situp-api/): The SITUP API, contains the interfaces for all the SITUP components.
* [situp-core](situp-core/): The core implementation of SITUP.
* [situp-plugins](situp-plugins/): The home for SITUP plugins.
  * [common](situp-plugins/common): Common plugins for all components *viz. source, buffer, processor, and sink*
  * [apmtracesource](situp-plugins/apmtracesource/): The HTTP source plugin for APM Trace.
  * [elasticsearch](situp-plugins/elasticsearch/): The Elasticsearch sink plugin that publishes records to elasticsearch cluster via REST client.
  * [lmdb-processor-state](situp-plugins/lmdb-processor-state/): TODO

We would love to hear from the larger community: please provide feedback proactively.

## Design RFC
[RFC](docs/dev/trace-analytics-rfc.md)

## Contribute

We invite developers from the larger Open Distro community to contribute and help improve test coverage and give us feedback on where improvements can be made in design, code and documentation. You can look at  [contribution guide](CONTRIBUTING.md) for more information on how to contribute.

## Code of Conduct

This project has adopted an [Open Source Code of Conduct](CODE_OF_CONDUCT.md).

## Security Issue Notifications

If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public GitHub issue.

## License

This library is licensed under the Apache 2.0 License. [Refer](LICENSE)
