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
1. A Pipeline configuration file. More details [here](overview.md#Sample configuration)
2. An optional Data Prepper configuration file

Example running command:

```
java -jar data-prepper-core-$VERSION pipelines.yml dataPrepper.yml
```

### APIs
Running the project locally will expose a server on port 4900 by default. The following 
APIs are available:

* /list
  * lists running pipelines
* /shutdown
  * starts a graceful shutdown of the Data Prepper
* /metrics/prometheus
  * returns a scrape of the Data Prepper metrics in Prometheus
  text format

### Running the example app
To run the example app against your local changes, use the docker found [here](https://github.com/opendistro-for-elasticsearch/data-prepper/tree/master/examples/dev/trace-analytics-sample-app)

## Data Prepper configuration
Data Prepper allows the following properties to be configured

* `useTls`: boolean indicating TLS should be used for server APIs. Defaults to `true`
* `keyStoreFilePath`: string path to .jks keystore file. Required if `useTls` is `true`
* `keyStorePassphrase` string passphrase for keystore. Required if `useTls` is `true`
* `serverPort`: integer port number to use for server APIs
* `log4jConfig`
  * `logLevel`
  * `filePath`
  * `maxFileSize`
  * `maxBackupIndex`
  
More details on Log4J properties [here](logs.md)

Below is an example of a Data Prepper configuration file:

```
useTls: true
keyStoreFilePath: "/usr/share/data-prepper/keystore.jks"
keyStorePassphrase: "password"
serverPort: 1234
log4jConfig:
  logLevel: "DEBUG"
  filePath: "file.txt"
  maxFileSize: "1GB"
  maxBackupIndex: "10"
```
