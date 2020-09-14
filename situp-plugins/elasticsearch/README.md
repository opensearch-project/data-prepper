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
      bulk_size: 4
``` 

### Hosts

A list of IP addresses of elasticsearch nodes.

### Bulk size (Optional)

A long of bulk size in bulk requests in MB. Default to 5 MB. Set to < 0 to disable.

## Compatibility

This plugin is compatible with Java 14.