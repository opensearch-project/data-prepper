# Blocking Buffer

This is a buffer based off `LinkedBlockingQueue` bounded to the specified capacity. One can read and write records with specified timeout value.

## Configuration
- buffer_size => An `int` representing max number of records the buffer accepts.
- batch_size => An `int` representing max number of records the buffer returns on read.

## Usages
Example `.yaml` configuration
```
buffer:
    - bounded_blocking:
```
*Note*: *By default, Data Prepper uses only one buffer. the `bounded_blocking` buffer, so this section in the `.yaml` need not be defined unless one wants to mention a custom buffer or tune the buffer settings* 

## Developer Guide
This plugin is compatible with Java 14. See 
- [CONTRIBUTING](https://github.com/opendistro-for-elasticsearch/data-prepper/blob/master/CONTRIBUTING.md) 
- [monitoring](https://github.com/opendistro-for-elasticsearch/data-prepper/blob/master/docs/readme/monitoring.md)