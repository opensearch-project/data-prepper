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


### Running the example app
To run the example app against your local changes, use the docker found [here](https://github.com/opendistro-for-elasticsearch/data-prepper/tree/master/examples/dev/trace-analytics-sample-app)

## Data Prepper configuration
Data Prepper allows the following properties to be configured

* Server port
* Log4J Properties
  * Log level
  * Log file
  * Max file size
  * Max backup index
  
More details on Log4J properties [here](logs.md)

Below is an example of a Data Prepper configuration file:

```
serverPort: 1234
log4jConfig:
  logLevel: "DEBUG"
  filePath: "file.txt"
  maxFileSize: "1GB"
  maxBackupIndex: "10"
```
