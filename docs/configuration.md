# Configuration
A Data Prepper instance requires 2 configuration files to run, and allows an optional 3rd Log4j 2 configuration file (see [Logging](logs.md)).

1. A YAML file which describes the data pipelines to run (including sources, processors, and sinks)
2. A YAML file containing Data Prepper server settings, primarily for interacting with the exposed Data Prepper server APIs
3. An optional Log4j 2 configuration file (can be JSON, YAML, XML, or .properties)

For Data Prepper before version 2.0, the `.jar` file expects the pipeline configuration file path followed by the server configuration file path. Example:
```
java -jar data-prepper-core-$VERSION.jar pipelines.yaml data-prepper-config.yaml
```

Optionally add `"-Dlog4j.configurationFile=config/log4j2.properties"` to the command if you would like to pass a custom Log4j 2 configuration file. If no properties file is provided, Data Prepper will default to the log4j2.properties file in the shared-config directory.


For Data Prepper 2.0 or above, Data Prepper is launched through `data-prepper` script with no additional command line arguments needed:
```
bin/data-prepper
```

Configuration files are read from specific subdirectories in the application's home directory:
1. `pipelines/`: for pipelines configurations; pipelines configurations can be written in one and more yaml files
2. `config/data-prepper-config.yaml`: for Data Prepper server configurations

You can continue to supply your own pipeline configuration file path followed by the server configuration file path, but the support for this method will be dropped in a future release. Example:
```
bin/data-prepper pipelines.yaml data-prepper-config.yaml
```

Additionally, Log4j 2 configuration file is read from `config/log4j2.properties` in the application's home directory.

## Pipeline Configuration

Example Pipeline configuration file (pipelines.yaml):

```yaml
entry-pipeline:
  workers: 4
  delay: "100"
  source:
    otel_trace_source:
      ssl: false
  sink:
    - pipeline:
        name: "raw-pipeline"
    - pipeline:
        name: "service-map-pipeline"
raw-pipeline:
  workers: 4
  source:
    pipeline:
      name: "entry-pipeline"
  processor:
    - otel_trace_raw:
  sink:
    - stdout:
service-map-pipeline:
  workers: 4
  delay: "100"
  source:
    pipeline:
      name: "entry-pipeline"
  processor:
    - service_map_stateful:
  sink:
    - stdout:
```
This sample pipeline creates a source to receive trace data and outputs transformed data to stdout. 

* `delay`(Optional): An `int` representing the maximum duration in milliseconds to retrieve records from the buffer. If the buffer's specified batch_size has not been reached before this duration is exceeded, a partial batch is used. If this value is set to 0, all available records up to the batch size will be immediately returned. If the buffer is empty, the buffer will block for up to 5 seconds to wait for records. Default value is `3000`.
* `workers`(Optional): An `int` representing the number of ProcessWorker threads for the pipeline.  Default value is `1`.

## Server Configuration
Data Prepper allows the following properties to be configured:

* `ssl`: boolean indicating TLS should be used for server APIs. Defaults to `true`
* `keyStoreFilePath`: string path to a .jks or .p12 keystore file. Required if `ssl` is `true`
* `keyStorePassword` string password for keystore. Optional, defaults to empty string
* `privateKeyPassword` string password for private key within keystore. Optional, defaults to empty string
* `serverPort`: integer port number to use for server APIs. Defaults to `4900`
* `metricRegistries`: list of metrics registries for publishing the generated metrics. Defaults to Prometheus; Prometheus and CloudWatch are currently supported.
* `metricTags`: map of metric tag key-value pairs applied as common metric tags to meter registries. Defaults to empty map. The maximum number of pairs is limited to 3. Note that `serviceName` is a reserved tag key with `DataPrepper` as default tag value. Its value could also be set through the environment variable `DATAPREPPER_SERVICE_NAME`. If `serviceName` is defined in `metricTags`, the value will overwrite those set through the above mechanism.

Example Data Prepper configuration file (data-prepper-config.yaml) with SSL enabled:

```yaml
ssl: true
keyStoreFilePath: "/usr/share/data-prepper/keystore.p12"
keyStorePassword: "password"
privateKeyPassword: "password"
serverPort: 4900
metricRegistries: [Prometheus]
metricTags:
  customKey: customValue
```

The Data Prepper Docker image runs with SSL enabled using a default self-signed certificate. 
For more robust security, you should generate your own private key and certificate. 
You can generate the certificate using existing tools such as OpenSSL. 
If you'd like a short primer, you can mimic the [steps used to create the default certificate](https://github.com/opensearch-project/data-prepper/tree/main/release/docker/config/README.md), and change them to suite your needs. 
Please note that for PKCS12 files (.p12), you should use the same password for the keystore and private key.

To run the Data Prepper Docker image with the default `data-prepper-config.yaml`, the command should look like this:

For Data Prepper 2.0 or above:
```
docker run \
 --name data-prepper-test \
 -p 4900:4900 \
 --expose 21890 \
 -v /full/path/to/pipelines.yaml:/usr/share/data-prepper/pipelines/pipelines.yaml \
 data-prepper/data-prepper:latest
```

For Data Prepper before 2.0:
```
docker run \
 --name data-prepper-test \
 -p 4900:4900 \
 --expose 21890 \
 -v /full/path/to/pipelines.yaml:/usr/share/data-prepper/pipelines.yaml \
 data-prepper/data-prepper:latest
```

To disable SSL, create a `data-prepper-config.yaml` with the following configuration.

```yaml
ssl: false
```

In order to pass your own `data-prepper-config.yaml`, mount it as a volume in the Docker image by adding the argument below to `docker run`. Note that the config must be mounted to proper path inside the container:

For Data Prepper 2.0 or above:
```
-v /full/path/to/data-prepper-config.yaml:/usr/share/data-prepper/config/data-prepper-config.yaml
```

For Data Prepper before 2.0:
```
-v /full/path/to/data-prepper-config.yaml:/usr/share/data-prepper/data-prepper-config.yaml
```

If your `data-prepper-config.yaml` has SSL enabled, and you are using your own keystore, it will need to be mounted as a Docker volume as well. Note that the mount path should correspond with
the `keyStoreFilePath` field from your `data-prepper-config.yaml`. It is recommended to mount to `/usr/share/data-prepper/config/data-prepper-config.yaml` (for Data Prepper 2.0 or above) or `/usr/share/data-prepper/data-prepper-config.yaml` (for Data Prepper before 2.0) to ensure that the path exists in the Docker image.
To do so, add the argument below to the `docker run` command.

```
 -v /full/path/to/keystore.p12:/usr/share/data-prepper/keystore.p12
```

## Circuit Breakers

Data Prepper supports circuit breakers which will interrupt adding objects
to the buffer when certain conditions are met.

### Heap

Heap circuit breaker: When the JVM heap usage reaches a configurable size stop accepting requests to buffers.

Configuration

```yaml
circuit_breakers:
  heap:
    usage: 6.5gb
    reset: 2s
```

* `usage` - float - The absolute value of JVM memory which will trip the circuit breaker. This can be defined with bytes (`b`), kilobytes (`kb`), megabytes (`mb`), or gigabytes (`gb`).
* `reset` - Duration - The time between when the circuit is tripped and the next attempt to validate will occur. Defaults to 1s.
* `check_interval` - Duration - The time between checks of the heap usage. Defaults to 500ms.

## Deprecated Pipeline Configuration Support
Starting in Data Prepper 1.3.0, Prepper plugins were renamed to Processors. The use of the prepper or processor name in pipeline configuration files is still supported. However, the use of both processor and prepper in the same configuration file is **not** supported.

Starting in Data Prepper 2.0, The use of the prepper name in pipeline configuration files is no longer supported.

An example of deprecated prepper pipeline configuration file (pipelines.yaml):
```yaml
grok-pipeline:
  source:
    http:
  prepper:
    - grok:
        match:
          log: [ "%{COMMONAPACHELOG}" ]
  sink:
    - stdout:
```
To continue to use the same configuration in Data Prepper 2.0 or above, rename `prepper` to `processor`:
```yaml
grok-pipeline:
  source:
    http:
  processor:
    - grok:
        match:
          log: [ "%{COMMONAPACHELOG}" ]
  sink:
    - stdout:
```
