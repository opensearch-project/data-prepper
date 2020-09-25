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
      index_type: raw
      index_alias: "apm-span"
      template_file: /your/local/template-file.json
      dlq_file: /your/local/dlq-file
      bulk_size: 4
``` 

### Hosts

A list of IP addresses of elasticsearch nodes.

### Index type (Optional)

A String takes value among `raw`, `service_map` and `custom`. Default to `raw`.

- `raw`: represents APM raw span data. e.g.

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

- `service_map`: represents APM service map data. e.g.

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

- `custom`: custom data. User needs to provide `index_alias` explicitly.

### Index alias

A String used as index alias if `index_type` is `raw` and otherwise used as index name. Default value depends on `index_type` value as follows:

- `raw`: otel-v1-apm-span-index
- `service_map`: otel-v1-apm-service-map  
- `custom`: No default value. User needs to provide the index name

`index_alias` is also reserved for index template name and `index_patterns` in the [index template](https://www.elastic.co/guide/en/elasticsearch/reference/7.8/index-templates.html) created by the Elasticsearch sink. 

- `raw`: 
   - `name`: `<index_alias>-index-template` 
   - `index_patterns`: `[<index_alias>-*]`
- `service_map`: 
   - `name`: `<index_alias>-index-template`
   - `index_patterns`: `[<index_alias>]`
- `custom`: User needs to provide template file path. See [Template file](#template_file)
   - `name`: `<index_alias>-index-template`
   - `index_patterns`: `[<index_alias>]`

### <a name="template_file"></a>Template file (Optional)

A json file path to be read as index template for APM data ingestion. The json file content should be the json value of
`"template"` key in the json content of elasticsearch [Index templates API](https://www.elastic.co/guide/en/elasticsearch/reference/7.8/index-templates.html).

Default template json (no template file path given) depends on `index_type`:

- `raw`: 

```$xslt
{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 2
  },
  "mappings": {
    "date_detection": false,
    "dynamic_templates": [
      {
        "strings_as_keyword": {
          "mapping": {
            "ignore_above": 1024,
            "type": "keyword"
          },
          "match_mapping_type": "string"
        }
      }
    ],
    "_source": {
      "enabled": true
    },
    "properties": {
      "traceId": {
        "ignore_above": 1024,
        "type": "keyword"
      },
      "spanId": {
        "ignore_above": 1024,
        "type": "keyword"
      },
      "name": {
        "ignore_above": 1024,
        "type": "keyword"
      },
      "kind": {
        "ignore_above": 1024,
        "type": "keyword"
      },
      "startTime": {
        "type": "date_nanos"
      },
      "endTime": {
        "type": "date_nanos"
      },
      "resource.attributes.service.name": {
        "ignore_above": 1024,
        "type": "keyword"
      }
    }
  }
}
```

- `service_map`

```$xslt
{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 2
  },
  "mappings": {
    "date_detection": false,
    "dynamic_templates": [
      {
        "strings_as_keyword": {
          "mapping": {
            "ignore_above": 1024,
            "type": "keyword"
          },
          "match_mapping_type": "string"
        }
      }
    ],
    "_source": {
      "enabled": true
    },
    "properties": {
      "hashId": {
        "ignore_above": 1024,
        "type": "keyword"
      },
      "serviceName": {
        "ignore_above": 1024,
        "type": "keyword"
      },
      "kind": {
        "ignore_above": 1024,
        "type": "keyword"
      },
      "destination": {
        "properties": {
          "domain": {
            "ignore_above": 1024,
            "type": "keyword"
          },
          "resource": {
            "ignore_above": 1024,
            "type": "keyword"
          }
        }
      },
      "target": {
        "properties": {
          "domain": {
            "ignore_above": 1024,
            "type": "keyword"
          },
          "resource": {
            "ignore_above": 1024,
            "type": "keyword"
          }
        }
      },
      "traceGroupName": {
        "ignore_above": 1024,
        "type": "keyword"
      }
    }
  }
}
```

- `custom`: no index template will be created

### DLQ file (Optional)

A String of absolute file path for DLQ failed output records. 
If not provided, failed records will be written into the default log file.

### Bulk size (Optional)

A long of bulk size in bulk requests in MB. Default to 5 MB. If set to be less than 0, 
all the records received from the upstream processor at a time will be sent as a single bulk request. 
If a single record turns out to be larger than the set bulk size, it will be sent as a bulk request of a single document.

## Compatibility

This plugin is compatible with Java 14.