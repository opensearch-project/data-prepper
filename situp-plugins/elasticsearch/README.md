# Elasticsearch sink

This is the SITUP Elasticsearch sink plugin, sending records to Elasticsearch cluster via REST client.

## Configuration

The Elasticsearch sink should be configured as part of SITUP pipeline yaml file. An example configuration is as follows

```$xslt
pipeline:
  ...
  sink:
    elasticsearch:
      hosts: ["http://localhost:9200"]
      batch_size: 4194304
``` 

### Hosts

A list of IP addresses of elasticsearch nodes.

### Batch size (Optional)

A long of batch size in bulk requests in bytes. Default to 5242880 (5MB). Set to -1 to disable.

## Compatibility

This plugin is compatible with Java 14.