# Logstash Migration Guide

This guide describes running Data Prepper with a Logstash configuration.

As mentioned in the [Getting Started](getting_started.md) guide, to configure Data Prepper, you require the following 
two files:
* `data-prepper-config.yaml`
* `pipelines.yaml`

Assuming you have a Logstash configuration `logstash.conf` file you can use that instead of `pipelines.yaml` to configure Data Prepper.

### Running Data Prepper with Logstash configuration

1. To install Data Prepper's docker image, please visit the _Installation_ section in the [Getting Started](getting_started.md) guide.


2. Run the Docker image pulled in Step 1 by supplying `logstash.conf` and `data-prepper-config.yaml`

```
docker run --name data-prepper -p 4900:4900 -v ${PWD}/logstash.conf:/usr/share/data-prepper/pipelines.conf -v ${PWD}/data-prepper-config.yaml:/usr/share/data-prepper/data-prepper-config.yaml opensearchproject/opensearch-data-prepper:latest pipelines.conf
```

The following output in your terminal indicates Data Prepper is running correctly:

```
INFO  com.amazon.dataprepper.pipeline.ProcessWorker - log-pipeline Worker: No records received from buffer
```