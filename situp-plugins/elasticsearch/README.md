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
      dlq_file: /your/local/dlq-file
``` 

### Hosts

A list of IP addresses of elasticsearch nodes.

### DLQ file (Optional)

A String of absolute file path for DLQ failed output records.

## Compatibility

This plugin is compatible with Java 14.