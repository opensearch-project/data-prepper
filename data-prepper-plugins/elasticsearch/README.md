# Elasticsearch sink

This is the Data Prepper Elasticsearch sink plugin, sending records to Elasticsearch cluster via REST client.

## Configuration

The Elasticsearch sink should be configured as part of Data Prepper pipeline yaml file according to the following use cases.

### Raw span trace analytics

```$xslt
pipeline:
  ...
  sink:
    elasticsearch:
      hosts: ["https://localhost:9200"]
      cert: path/to/cert
      username: YOUR_USERNAME_HERE
      password: YOUR_PASSWORD_HERE
      trace_analytics_raw: true
      dlq_file: /your/local/dlq-file
      bulk_size: 4
```

The Elasticsearch sink will reserve `otel-v1-apm-span-*` as index pattern and `otel-v1-apm-span` as index alias for record ingestion.

### Service map trace analytics

```$xslt
pipeline:
  ...
  sink:
    elasticsearch:
      hosts: ["https://localhost:9200"]
      cert: path/to/cert
      username: YOUR_USERNAME_HERE
      password: YOUR_PASSWORD_HERE
      trace_analytics_service_map: true
      dlq_file: /your/local/dlq-file
      bulk_size: 4
```

The Elasticsearch sink will reserve `otel-v1-apm-service-map` as index for record ingestion.

### Send Trace data to Amazon Elasticsearch domain

You can use the Elasticsearch sink plugin to send data to Amazon Elasticsearch domain. 

#### Send Raw span data 

```$xslt
pipeline:
  ...
  sink:
    elasticsearch:
      hosts:["https://foo.us-east-1.es.amazonaws.com"]
      aws_sigv4:true
      aws_region: "us-east-1"
      trace_analytics_raw: true
```

#### Send Service Map span data 

```$xslt
pipeline:
  ...
  sink:
    elasticsearch:
      hosts:["https://foo.us-east-1.es.amazonaws.com"]
      aws_sigv4:true
      aws_region: "us-east-1"
      trace_analytics_service_map: true
```

The plugin uses the default credential chain. Run `aws configure` using the AWS CLI to set your credentials. 


## Parameters

### Hosts

A list of IP addresses of Opendistro for Elasticsearch cluster or url of the Amazon Elasticsearch domain.

### Amazon Elasticsearch

`aws_sigv4` boolean flag to use the sink plugin to send data to Amazon Elasticsearch domain. Default value is `false`. 

### Amazon Elasticsearch Region

`aws_region` the aws region where the aes domain is located. Default value is `us-east-1`

### cert
CA certificate that is pem encoded. Accepts both .pem or .crt. This enables the client to trust the CA that has signed the certificate that ODFE is using.
Default is null. 

### Username

A String of username used in the [cognito](https://opendistro.github.io/for-elasticsearch-docs/docs/security/access-control/users-roles) of ODFE cluster.

### Password

A String of password used in the [cognito](https://opendistro.github.io/for-elasticsearch-docs/docs/security/access-control/users-roles) of ODFE cluster.

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
`"template"` key in the json content of Elasticsearch [Index templates API](https://www.elastic.co/guide/en/elasticsearch/reference/7.8/index-templates.html), 
e.g. [otel-v1-apm-span-index-template.json](https://github.com/opendistro-for-elasticsearch/simple-ingest-transformation-utility-pipeline/blob/master/dataPrepper-plugins/elasticsearch/src/main/resources/otel-v1-apm-span-index-template.json)

### DLQ file (Optional)

A String of absolute file path for DLQ failed output records. 
If not provided, failed records will be written into the default log file.

### Bulk size (Optional)

A long of bulk size in bulk requests in MB. Default to 5 MB. If set to be less than 0, 
all the records received from the upstream processor at a time will be sent as a single bulk request. 
If a single record turns out to be larger than the set bulk size, it will be sent as a bulk request of a single document.

## Compatibility

This plugin is compatible with Java 14.