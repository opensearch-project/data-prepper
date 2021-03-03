# Blocking Buffer

This is a buffer based off `LinkedBlockingQueue` bounded to the specified capacity. One can read and write records with specified timeout value.

## Usages
Example `.yaml` configuration
```
buffer:
    - bounded_blocking:
```
*Note*: *By default, Data Prepper uses only one buffer. the `bounded_blocking` buffer, so this section in the `.yaml` need not be defined unless one wants to mention a custom buffer or tune the buffer settings* 

## Configuration
- buffer_size => An `int` representing max number of unchecked records the buffer accepts (num of unchecked records = num of records written into the buffer + num of in-flight records not yet checked by the Checkpointing API). Default is `512`.
- batch_size => An `int` representing max number of records the buffer returns on read. Default is `8`.

##Metrics
This plugin inherits the common metrics defined in [AbstractBuffer](https://github.com/opendistro-for-elasticsearch/data-prepper/blob/main/data-prepper-api/src/main/java/com/amazon/dataprepper/model/buffer/AbstractBuffer.java)

## Developer Guide
This plugin is compatible with Java 14. See 
- [CONTRIBUTING](https://github.com/opendistro-for-elasticsearch/data-prepper/blob/main/CONTRIBUTING.md) 
- [monitoring](https://github.com/opendistro-for-elasticsearch/data-prepper/blob/main/docs/readme/monitoring.md)