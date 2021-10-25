# Getting Started with Data Prepper

If you are migrating from Open Distro Data Prepper,
visit the [Migrating from Open Distro](migrating_from_opendistro.md) page.
Otherwise, please continue.

## Installation

There are two ways to install Data Prepper for running.

1. Run the Docker image
2. Build from source

The easiest way to use Data Prepper is by running the Docker image. We suggest
you use this approach if you have [Docker](https://www.docker.com) available.

You can pull the Docker image:

```
docker pull opensearchproject/data-prepper:latest
```

If you have special requirements that require you build from source, or if you
want to contribute, please see the [Developer Guide](developer_guide.md).

## Configure Data Prepper

You must configure Data Prepper with a pipeline before running it.

You will configure two files:

* `data-prepper-config.yaml`
* `pipelines.yaml`

Depending on what you want to do, we have a few different guides to configuring Data Prepper.

* [Trace Analytics](trace_analytics.md) - Learn how to setup Data Prepper for trace observability
* [Simple Pipeline](simple_pipelines.md) - Learn the basics of Data Prepper pipelines with some simple configurations.

## Running

The remainder of this page shows examples for running from the Docker image. If you
built from source, you will need to make some modifications to the example commands.

However you configure your pipeline, you will run Data Prepper the same way. You run the Docker
image and supply both the `pipelines.yaml` and `data-prepper-config.yaml` files.

```
docker run --name data-prepper -p 4900:4900 -v ${PWD}/pipelines.yaml:/usr/share/data-prepper/pipelines.yaml -v ${PWD}/data-prepper-config.yaml:/usr/share/data-prepper/data-prepper-config.yaml opensearchproject/data-prepper:latest
```

Once Data Prepper is running, it will process data until it is shutdown. Once you are done, shut it down with

```
curl -X POST http://localhost:4900/shutdown
```

### Additional Configurations

Optionally add `"-Dlog4j.configurationFile=config/log4j2.properties"` to the command if you would like to pass a custom log4j2 properties file. If no properties file is provided, Data Prepper will default to the log4j2.properties file in the *shared-config* directory.

## Next Steps

All Data Prepper instances expose a few APIs. The [API documentation](core_apis.md) outlines these APIs and
how to configure the server.

Trace Analytics is an important Data Prepper use case. If you haven't yet configure it,
please visit the [Trace Analytics documentation](trace_analytics.md).

To monitor Data Prepper, please read the [Monitoring](monitoring.md) page.

## Other Examples

We have other several Docker [examples](https://github.com/opensearch-project/data-prepper/tree/main/examples/)
that allow you to run Data Prepper in different scenarios.
