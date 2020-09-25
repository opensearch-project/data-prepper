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
      trace_analytics_raw: true
      trace_analytics_service_map: false
      template_file: /your/local/template-file.json
      dlq_file: /your/local/dlq-file
      bulk_size: 4
``` 

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

Default value is true.

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

Default value is false. If set to true, user also needs to set `trace_analytics_raw` to false.

### Custom data type

If both `trace_analytics_raw` and `trace_analytics_service_map` are set to false, the ES sink will ingest custom data type, i.e.
user needs to provide custom [`index_alias`](#index_alias).

### <a name="index_alias"></a>Index alias

A String used as index alias if `trace_analytics_raw` is `true` and otherwise used just as index name. Default value depends on data type flags as follows:

- `trace_analytics_raw`: otel-v1-apm-span-index
- `trace_analytics_service_map`: otel-v1-apm-service-map  
- `custom`: No default value. User needs to provide the index name

`index_alias` is also reserved for index template name and `index_patterns` in the [index template](https://www.elastic.co/guide/en/elasticsearch/reference/7.8/index-templates.html) created by the Elasticsearch sink. 

- `trace_analytics_raw`: 
   - `name`: `<index_alias>-index-template` 
   - `index_patterns`: `[<index_alias>-*]`
- `trace_analytics_service_map`: 
   - `name`: `<index_alias>-index-template`
   - `index_patterns`: `[<index_alias>]`
- `custom`: User needs to provide template file path. See [Template file](#template_file)
   - `name`: `<index_alias>-index-template`
   - `index_patterns`: `[<index_alias>]`

### <a name="template_file"></a>Template file (Optional)

A json file path to be read as index template for APM data ingestion. The json file content should be the json value of
`"template"` key in the json content of elasticsearch [Index templates API](https://www.elastic.co/guide/en/elasticsearch/reference/7.8/index-templates.html).

Default template file (no template file path given) depends on `index_type`:

- `trace_analytics_raw`: [otel-v1-apm-span-index-template.json](https://github.com/opendistro-for-elasticsearch/simple-ingest-transformation-utility-pipeline/blob/master/situp-plugins/elasticsearch/src/main/resources/otel-v1-apm-span-index-template.json)

- `trace_analytics_service_map`: [otel-v1-apm-service-map-index-template.json](https://github.com/opendistro-for-elasticsearch/simple-ingest-transformation-utility-pipeline/blob/master/situp-plugins/elasticsearch/src/main/resources/otel-v1-apm-service-map-index-template.json)

- `custom`: None (no index template will be created)

### DLQ file (Optional)

A String of absolute file path for DLQ failed output records. 
If not provided, failed records will be written into the default log file.

### Bulk size (Optional)

A long of bulk size in bulk requests in MB. Default to 5 MB. If set to be less than 0, 
all the records received from the upstream processor at a time will be sent as a single bulk request. 
If a single record turns out to be larger than the set bulk size, it will be sent as a bulk request of a single document.

## Compatibility

This plugin is compatible with Java 14.