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

Example Data Prepper configuration file (data-prepper-config.yaml):
```yaml
ssl: true
keyStoreFilePath: "/usr/share/data-prepper/keystore.jks"
keyStorePassword: "password"
privateKeyPassword: "other_password"
serverPort: 1234
```
