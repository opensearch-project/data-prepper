# Logging
Data Prepper uses [SLF4J](http://www.slf4j.org/) with a [Log4j 2 binding](http://logging.apache.org/log4j/2.x/log4j-slf4j-impl/). 

Default properties for Log4j 2 can be found in the log4j2.properties file in the *shared-config* directory.

Users are able to override these logging settings by setting their own "log4j.configurationFile" system property (see [Log4j 2 configuration docs](https://logging.apache.org/log4j/2.x/manual/configuration.html)).

Example:
```
java "-Dlog4j.configurationFile=config/custom-log4j2.properties" -jar data-prepper-core-$VERSION.jar pipelines.yml data-prepper.yml
```
