# Data Prepper Developer Guide

This page is for anybody who wishes to contribute code to Data Prepper. Welcome!

## Contributions

First, please read our [contribution guide](../CONTRIBUTING.md) for more information on how to contribute to Data Prepper.

## Installation Prerequisites

### JDK Versions

Running Data Prepper requires JDK 8 and above.

Running the integration tests requires JDK 14 or 15.


## Building from source

The assemble task will build the Jar files without running the integration
tests. You can use these jar files for running DataPrepper. If you are just
looking to use DataPrepper and modify it, this build
is faster than running the integration test suite and only requires JDK 8+.

To build the project from source, run

```
./gradlew assemble
```

from the project root. 

### Full Project Build

Running the build command will assemble the Jar files needed
for running DataPrepper. It will also run the integration test
suite.

To build, run

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

## More Information

We have the following pages for specific development guidance on the topics:

* [Plugin Development](plugin_development.md)
* [Error Handling](error_handling.md)
* [Logs](logs.md)
