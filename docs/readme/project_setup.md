## Project Setup

## Building from source

To build the project from source, run 

```
./gradlew build
```

from the project root. 

## Running the project

After building, the project can be run from the executable JAR **data-prepper-core-$VERSION**
found in the **build/libs** directory of the **data-prepper-core** subproject. The executable JAR takes
two arguments:
1. A Pipeline configuration file. More details [here](overview.md#sample-pipeline-configuration)
2. A Data Prepper configuration file. More details [here](project_setup.md#data-prepper-configuration)

Example running command:

```
java -jar data-prepper-core-$VERSION.jar pipelines.yml data-prepper.yml
```

Optionally add `"-Dlog4j.configurationFile=config/log4j2.properties"` to the command if you would like to pass a custom log4j2 properties file.

### APIs
Running the project locally will expose a server on port 4900 by default. The following 
APIs are available:

* /list
  * lists running pipelines
* /shutdown
  * starts a graceful shutdown of the Data Prepper
* /metrics/prometheus
  * returns a scrape of the Data Prepper metrics in Prometheus text format
* /metrics/sys
  * returns JVM metrics in Prometheus text format

### Running the example app
To run the example app against your local changes, use the docker found [here](https://github.com/opendistro-for-elasticsearch/data-prepper/tree/master/examples/dev/trace-analytics-sample-app)

## Data Prepper configuration
Data Prepper allows the following properties to be configured:

* `ssl`: boolean indicating TLS should be used for server APIs. Defaults to `true`
* `keyStoreFilePath`: string path to .jks keystore file. Required if `ssl` is `true`
* `keyStorePassword` string password for keystore. Optional, defaults to empty string
* `privateKeyPassword` string password for private key within keystore. Optional, defaults to empty string
* `serverPort`: integer port number to use for server APIs
* `log4jConfig`
  * `logLevel`
  * `filePath`
  * `maxFileSize`
  * `maxBackupIndex`
  
More details on Log4J properties [here](logs.md)

Below is an example of a Data Prepper configuration file:

```
ssl: true
keyStoreFilePath: "/usr/share/data-prepper/keystore.jks"
keyStorePassword: "password"
privateKeyPassword: "other_password"
serverPort: 1234
log4jConfig:
  logLevel: "DEBUG"
  filePath: "file.txt"
  maxFileSize: "1GB"
  maxBackupIndex: "10"
```
