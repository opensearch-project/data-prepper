## Jaeger Hot R.O.D. on EC2
This directory contains a script to setup install and run the Jaeger Hot R.O.D. sample application
using [Docker](https://docs.docker.com/) and [Docker Compose](https://docs.docker.com/compose/).

The script downloads/installs Docker and Docker Compose, writes necessary application configuration files, then creates the following containers:
1. [Jaeger HotROD](https://github.com/jaegertracing/jaeger/tree/master/examples/hotrod) - the example application to generate trace data
2. [Jaeger Agent](https://www.jaegertracing.io/docs/1.21/architecture/#agent) - a network daemon which batches trace spans and sends to the collector
3. [OpenTelemetry Collector](https://github.com/open-telemetry/opentelemetry-collector) - a vendor-agnostic implementation on how to receive, process and export telemetry data
