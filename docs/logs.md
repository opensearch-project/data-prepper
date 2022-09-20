# Logging
Data Prepper uses [SLF4J](http://www.slf4j.org/) with a [Log4j 2 binding](http://logging.apache.org/log4j/2.x/log4j-slf4j-impl/). 

For Data Prepper 2.0 or above, Log4j 2 configuration file can be found and edited in `config/log4j2.properties` in the application's home 
directory. Default properties for Log4j 2 can be found in `log4j2-rolling.properties` in the *shared-config* directory.

For Data Prepper before version 2.0, Log4j 2 configuration can be overridden by setting "log4j.configurationFile" system property when 
running Data Prepper. Default properties for Log4j 2 can be found in `log4j2.properties` in the *shared-config* directory. 

Example:
```
java "-Dlog4j.configurationFile=config/custom-log4j2.properties" -jar data-prepper-core-$VERSION.jar pipelines.yaml data-prepper-config.yaml
```

See [Log4j 2 configuration docs](https://logging.apache.org/log4j/2.x/manual/configuration.html) for more information on Log4j 2 
configurations.

