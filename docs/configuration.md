# Configuration
A Data Prepper instance requires 2 configuration files to run, and allows an optional 3rd Log4j 2 configuration file (see [Logging](logs.md)).

1. A YAML file which describes the data pipelines to run (including sources, preppers, and sinks)
2. A YAML file containing Data Prepper server settings, primarily for interacting with the exposed Data Prepper server APIs
3. An optional Log4j 2 configuration file (can be JSON, YAML, XML, or .properties)

The resulting `.jar` file expects the pipeline configuration file path followed by the server configuration file path. Example:
```
java -jar data-prepper-core-$VERSION.jar pipelines.yaml data-prepper-config.yaml
```

Optionally add `"-Dlog4j.configurationFile=config/log4j2.properties"` to the command if you would like to pass a custom Log4j 2 configuration file. If no properties file is provided, Data Prepper will default to the log4j2.properties file in the shared-config directory.

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
  prepper:
    - otel_trace_raw_prepper:
  sink:
    - stdout:
service-map-pipeline:
  workers: 4
  delay: "100"
  source:
    pipeline:
      name: "entry-pipeline"
  prepper:
    - service_map_stateful:
  sink:
    - stdout:
```
This sample pipeline creates a source to receive trace data and outputs transformed data to stdout. 


## Server Configuration
Data Prepper allows the following properties to be configured:

* `ssl`: boolean indicating TLS should be used for server APIs. Defaults to `true`
* `keyStoreFilePath`: string path to a .jks or .p12 keystore file. Required if `ssl` is `true`
* `keyStorePassword` string password for keystore. Optional, defaults to empty string
* `privateKeyPassword` string password for private key within keystore. Optional, defaults to empty string
* `serverPort`: integer port number to use for server APIs. Defaults to `4900`
* `metricRegistries`: list of metrics registries for publishing the generated metrics. Defaults to Prometheus; Prometheus and CloudWatch are currently supported.

Example Data Prepper configuration file (data-prepper-config.yaml) with SSL enabled:

```yaml
ssl: true
keyStoreFilePath: "/usr/share/data-prepper/keystore.p12"
keyStorePassword: "password"
privateKeyPassword: "password"
serverPort: 4900
metricRegistries: [Prometheus]
```

The Data Prepper Docker image runs with SSL enabled using a default self-signed certificate. 
For more robust security, you should generate your own private key and certificate. 
You can generate the certificate using existing tools such as OpenSSL. 
If you'd like a short primer, you can mimic the [steps used to create the default certificate](https://github.com/opensearch-project/data-prepper/tree/main/release/docker/config/README.md), and change them to suite your needs. 
Please note that for PKCS12 files (.p12), you should use the same password for the keystore and private key.

To run the Data Prepper Docker image with the default `data-prepper-config.yaml`, the command should look like this.

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

In order to pass your own `data-prepper-config.yaml`, mount it as a volume in the Docker image by adding the argument below to `docker run`. Note that the config must be mounted to `/usr/share/data-prepper/data-prepper-config.yaml`

```
v /full/path/to/data-prepper-config.yaml:/usr/share/data-prepper/data-prepper-config.yaml
```

If your `data-prepper-config.yaml` has SSL enabled, and you are using your own keystore, it will need to be mounted as a Docker volume as well. Note that the mount path should correspond with
the `keyStoreFilePath` field from your `data-prepper-config.yaml`. It is recommended to mount to `/usr/share/data-prepper/data-prepper-config.yaml` to ensure that the path exists in the Docker image.
To do so, add the argument below to the `docker run` command.

```
 -v /full/path/to/keystore.p12:/usr/share/data-prepper/keystore.p12
```