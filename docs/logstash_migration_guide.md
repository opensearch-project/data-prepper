# Logstash Migration Guide

This guide describes running Data Prepper with a Logstash configuration.

As mentioned in the [Getting Started](getting_started.md) guide, you need to configure Data Prepper with a pipeline using a `pipelines.yaml` file.

If you have a Logstash configuration `logstash.conf` file you can use that instead of `pipelines.yaml` to configure Data Prepper.

### Supported Plugins

As of Data Prepper 1.2 release, the following plugins from the Logstash configuration are supported:
* HTTP Input plugin
* Grok Filter plugin
* Elasticsearch Output plugin
* Amazon Elasticsearch Output plugin

### Limitations
* Apart from the Supported plugins, all other plugins from the Logstash configuration will throw an `Exception` and fail to execute.
* Conditionals in the Logstash configuration are not supported as of Data Prepper 1.2 release.

### Running Data Prepper with Logstash configuration

1. To install Data Prepper's docker image, please visit the _Installation_ section in the [Getting Started](getting_started.md) guide.


2. Run the Docker image pulled in Step 1 by supplying your `logstash.conf`

```
docker run --name data-prepper -p 4900:4900 -v ${PWD}/logstash.conf:/usr/share/data-prepper/pipelines.conf opensearchproject/data-prepper:latest pipelines.conf
```


The `logstash.conf` gets converted to `logstash.yaml` by mapping the Plugins and attributes in the Logstash configuration to the corresponding ones in Data Prepper.
You can find the converted `logstash.yaml` in the same directory where you placed `logstash.conf`.


The following output in your terminal indicates Data Prepper is running correctly:

```
INFO  org.opensearch.dataprepper.pipeline.ProcessWorker - log-pipeline Worker: No records received from buffer
```