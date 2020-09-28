# Elasticsearch sink

This is the SITUP Elasticsearch sink plugin, sending records to Elasticsearch cluster via REST client.

## Configuration

The Elasticsearch sink should be configured as part of SITUP pipeline yaml file according to the following use cases.

### Raw span trace analytics

```$xslt
pipeline:
  ...
  sink:
    elasticsearch:
      hosts: ["http://localhost:9200"]
      trace_analytics_raw: true
      dlq_file: /your/local/dlq-file
      bulk_size: 4
```

The elasticsearch sink will reserve `otel-v1-apm-span-*` as index pattern and `otel-v1-apm-span` as index alias for record ingestion.

### Service map trace analytics

```$xslt
pipeline:
  ...
  sink:
    elasticsearch:
      hosts: ["http://localhost:9200"]
      trace_analytics_service_map: true
      dlq_file: /your/local/dlq-file
      bulk_size: 4
```

The elasticsearch sink will reserve `otel-v1-apm-service-map` as index for record ingestion.

### Custom data

```$xslt
pipeline:
  ...
  sink:
    elasticsearch:
      hosts: ["http://localhost:9200"]
      index: "some-index"
      template_file: /your/local/template-file.json
      document_id_field: "someId"
      dlq_file: /your/local/dlq-file
      bulk_size: 4
```

User needs to provide custom index for record ingestion.

## Parameters

### Hosts

A list of IP addresses of elasticsearch nodes.

### Trace analytics raw

A boolean flag indicates APM trace analytics raw span data type. e.g.

```$xslt
{
  "traceId":"bQ/2NNEmtuwsGAOR5ntCNw==",
  "spanId":"mnO/qUT5ye4=",
  "name":"io.opentelemetry.auto.servlet-3.0",
  "kind":"SERVER",
  "status":{},
  "startTime":"2020-08-20T05:40:46.041011600Z",
  "endTime":"2020-08-20T05:40:46.089556800Z",
  ...
}
```

Default value is false.

### Trace analytics service map

A boolean flag indicates APM trace analytics service map data type. e.g.

```$xslt
{
  "hashId": "aQ/2NNEmtuwsGAOR5ntCNwk=",
  "serviceName": "Payment",
  "kind": "Client",
  "target":
  {
    "domain": "Purchase",
    "resource": "Buy"
  },
  "destination":
  {
    "domain": "Purchase",
    "resource": "Buy"
  },
  "traceGroupName": "MakePayement.auto"
}
```

Default value is false. 

### Custom data type

If both `trace_analytics_raw` and `trace_analytics_service_map` are set to false, the ES sink will ingest custom data type, i.e.
user needs to provide custom [index](#index).

### <a name="index"></a>Index

A String used as index name for custom data type.

### <a name="template_file"></a>Template file (Optional)

A json file path to be read as index template for custom data ingestion. The json file content should be the json value of
`"template"` key in the json content of elasticsearch [Index templates API](https://www.elastic.co/guide/en/elasticsearch/reference/7.8/index-templates.html), 
e.g. [otel-v1-apm-span-index-template.json](https://github.com/opendistro-for-elasticsearch/simple-ingest-transformation-utility-pipeline/blob/master/situp-plugins/elasticsearch/src/main/resources/otel-v1-apm-span-index-template.json)

### DLQ file (Optional)

A String of absolute file path for DLQ failed output records. 
If not provided, failed records will be written into the default log file.

### Bulk size (Optional)

A long of bulk size in bulk requests in MB. Default to 5 MB. If set to be less than 0, 
all the records received from the upstream processor at a time will be sent as a single bulk request. 
If a single record turns out to be larger than the set bulk size, it will be sent as a bulk request of a single document.

## Compatibility

This plugin is compatible with Java 14.