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
      bulk_size: 4
``` 

### Hosts

A list of IP addresses of elasticsearch nodes.

### DLQ file (Optional)

A String of absolute file path for DLQ failed output records.

### Bulk size (Optional)

A long of bulk size in bulk requests in MB. Default to 5 MB. If set to be less than 0, 
all the records received from the upstream processor at a time will be sent as a single bulk request. 
If a single record turns out to be larger than the set bulk size, it will be sent as a bulk request of a single document.

## Compatibility

This plugin is compatible with Java 14.