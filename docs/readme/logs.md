# Logging

In Data Prepper, logging is handled in each plugin using SLF4J, with Log4J as the logging backend.
Default properties for Log4J can be found in the log4j.properties file in the *shared-config* directory.
The following Log4J properties are configurable via the Data Prepper configuration file:

* Log level
  * Valid options are TRACE,  DEBUG, INFO, WARN, ERROR, FATAL, and OFF
* Log file
  * Path to file for log output
* Max file size
  * Max size of the log file
* Max backup index
  * Max number of backup log files before the oldest is erased
 
## Example

Below is an example of a Data Prepper configuration file setting
custom values for Log4J properties

```
log4jConfig:
  logLevel: "DEBUG"
  filePath: "logs/DataPrepper.log"
  maxFileSize: "1GB"
  maxBackupIndex: 10
```