# Project Setup

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
1. A Pipeline configuration file
2. A Data Prepper configuration file

See [configuration](configuration.md) docs for more information.

Example java command:
```
java -jar data-prepper-core-$VERSION.jar pipelines.yaml data-prepper-config.yaml
```

Optionally add `"-Dlog4j.configurationFile=config/log4j2.properties"` to the command if you would like to pass a custom log4j2 properties file. If no properties file is provided, Data Prepper will default to the log4j2.properties file in the *shared-config* directory.

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
To run the example app against your local changes, use the docker found [here](https://github.com/opensearch-project/data-prepper/tree/master/examples/dev/trace-analytics-sample-app)
