# Elasticsearch sink

This is the Data Prepper Elasticsearch sink plugin, sending records to Elasticsearch cluster via REST client.

## Usages

The Elasticsearch sink should be configured as part of Data Prepper pipeline yaml file according to the following use cases.

### <a name="raw_span_trace_analytics"></a>Raw span trace analytics

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

The elasticsearch sink will reserve `otel-v1-apm-span-*` as index pattern and `otel-v1-apm-span` as index alias for record ingestion.

### <a name="service_map_trace_analytics"></a>Service map trace analytics

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

The elasticsearch sink will reserve `otel-v1-apm-service-map` as index for record ingestion.

### Amazon Elasticsearch Service

The elasticsearch sink can also be configured for Amazon Elasticsearch Service domain. See [security](security.md) for details.

```$xslt
pipeline:
  ...
  sink:
    elasticsearch:
      hosts: ["https://your-amazon-elasticssearch-service-endpoint"]
      aws_sigv4: true 
      cert: path/to/cert
      insecure: false
      trace_analytics_service_map: true
      bulk_size: 4
```

## Configuration

- `hosts`: A list of IP addresses of elasticsearch nodes.

- `cert`(optional): CA certificate that is pem encoded. Accepts both .pem or .crt. This enables the client to trust the CA that has signed the certificate that ODFE is using.
Default is null. 

- `aws_sigv4`: A boolean flag to sign the HTTP request with AWS credentials. Only applies to Amazon Elasticsearch Service. See [security](security.md) for details. Default to `false`. 

- `aws_region`: A String represents the region of Amazon Elasticsearch Service domain, e.g. us-west-2. Only applies to Amazon Elasticsearch Service. Defaults to `us-east-1`.

- `insecure`: A boolean flag to turn off SSL certificate verification. If set to true, CA certificate verification will be turned off and insecure HTTP requests will be sent. Default to `false`.

- `username`(optional): A String of username used in the [internal users](https://opendistro.github.io/for-elasticsearch-docs/docs/security/access-control/users-roles) of ODFE cluster. Default is null.

- `password`(optional): A String of password used in the [internal users](https://opendistro.github.io/for-elasticsearch-docs/docs/security/access-control/users-roles) of ODFE cluster. Default is null.

- `trace_analytics_raw`(optional): A boolean flag indicates APM trace analytics raw span data type. e.g.
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
Default value is false. Set it to true for [Raw span trace analytics](#raw_span_trace_analytics). Set it to false for [Service map trace analytics](#service_map_trace_analytics).

- `trace_analytics_service_map`(optional): A boolean flag indicates APM trace analytics service map data type. e.g.
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
Default value is false. Set it to true for [Service map trace analytics](#service_map_trace_analytics). Set it to false for [Raw span trace analytics](#raw_span_trace_analytics).

- <a name="index"></a>`index`: A String used as index name for custom data type. Applicable and required only If both `trace_analytics_raw` and `trace_analytics_service_map` are set to false.

- <a name="template_file"></a>`template_file`(optional): A json file path to be read as index template for custom data ingestion. The json file content should be the json value of
`"template"` key in the json content of elasticsearch [Index templates API](https://www.elastic.co/guide/en/elasticsearch/reference/7.8/index-templates.html), 
e.g. [otel-v1-apm-span-index-template.json](https://github.com/opendistro-for-elasticsearch/data-prepper/blob/main/data-prepper-plugins/elasticsearch/src/main/resources/otel-v1-apm-span-index-template.json)

- `dlq_file`(optional): A String of absolute file path for DLQ failed output records. Defaults to null.
If not provided, failed records will be written into the default data-prepper log file (`logs/Data-Prepper.log`).

- `bulk_size` (optional): A long of bulk size in bulk requests in MB. Default to 5 MB. If set to be less than 0, 
all the records received from the upstream prepper at a time will be sent as a single bulk request. 
If a single record turns out to be larger than the set bulk size, it will be sent as a bulk request of a single document.

## Metrics

Besides common metrics in [AbstractSink](https://github.com/opendistro-for-elasticsearch/data-prepper/blob/main/data-prepper-api/src/main/java/com/amazon/dataprepper/model/sink/AbstractSink.java), elasticsearch sink introduces the following custom metrics.

### Timer

- `bulkRequestLatency`: measures latency of sending each bulk request including retries.

### Counter

- `bulkRequestErrors`: measures number of errors encountered in sending bulk requests.
- `documentsSuccess`: measures number of documents successfully sent to ES by bulk requests including retries.
- `documentsSuccessFirstAttempt`: measures number of documents successfully sent to ES by bulk requests on first attempt.
- `documentErrors`: measures number of documents failed to be sent by bulk requests. 

## Developer Guide

This plugin is compatible with Java 8. See 

- [CONTRIBUTING](https://github.com/opendistro-for-elasticsearch/data-prepper/blob/main/CONTRIBUTING.md) 
- [monitoring](https://github.com/opendistro-for-elasticsearch/data-prepper/blob/main/docs/readme/monitoring.md)