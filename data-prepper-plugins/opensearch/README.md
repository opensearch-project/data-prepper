# OpenSearch Sink

This is the Data Prepper OpenSearch sink plugin that sends records to an OpenSearch cluster via REST client. You can use the sink to send data to OpenSearch, Amazon OpenSearch Service, or Elasticsearch.

The OpenSearch sink plugin supports OpenSearch 1.0 and greater and Elasticsearch 7.3 and greater.

## Usages

The OpenSearch sink should be configured as part of Data Prepper pipeline yaml file.

### Raw span trace analytics

```
pipeline:
  ...
  sink:
    opensearch:
      hosts: ["https://localhost:9200"]
      cert: path/to/cert
      username: YOUR_USERNAME_HERE
      password: YOUR_PASSWORD_HERE
      index_type: trace-analytics-raw
      dlq_file: /your/local/dlq-file
      max_retries: 20
      bulk_size: 4
```

The OpenSearch sink will reserve `otel-v1-apm-span-*` as index pattern and `otel-v1-apm-span` as index alias for record ingestion.

### </a>Service map trace analytics

```
pipeline:
  ...
  sink:
    opensearch:
      hosts: ["https://localhost:9200"]
      cert: path/to/cert
      username: YOUR_USERNAME_HERE
      password: YOUR_PASSWORD_HERE
      index_type: trace-analytics-service-map
      dlq_file: /your/local/dlq-file
      bulk_size: 4
```

The OpenSearch sink will reserve `otel-v1-apm-service-map` as index for record ingestion.

### Amazon OpenSearch Service

The OpenSearch sink can also be configured for an Amazon OpenSearch Service domain. See [security](security.md) for details.

```
pipeline:
  ...
  sink:
    opensearch:
      hosts: ["https://your-amazon-opensearch-service-endpoint"]
      aws_sigv4: true
      cert: path/to/cert
      insecure: false
      index_type: trace-analytics-service-map
      bulk_size: 4
```

## Configuration

- `hosts`: A list of IP addresses of OpenSearch nodes.

- `cert`(optional): CA certificate that is pem encoded. Accepts both .pem or .crt. This enables the client to trust the CA that has signed the certificate that the OpenSearch cluster is using.
Default is null.

- `aws_serverless`: A boolean flag to indicate the OpenSearch backend is Amazon OpenSearch Serverless. Default to `false`.

- `aws_sigv4`: A boolean flag to sign the HTTP request with AWS credentials. Only applies to Amazon OpenSearch Service. See [security](security.md) for details. Default to `false`.

- `aws_region`: A String represents the region of Amazon OpenSearch Service domain, e.g. us-west-2. Only applies to Amazon OpenSearch Service. Defaults to `us-east-1`.

- `aws_sts_role_arn`: A IAM role arn which the sink plugin will assume to sign request to Amazon OpenSearch Service. If not provided the plugin will use the default credentials.

- `aws_sts_header_overrides`: An optional map of header overrides to make when assuming the IAM role for the sink plugin.

- `insecure`: A boolean flag to turn off SSL certificate verification. If set to true, CA certificate verification will be turned off and insecure HTTP requests will be sent. Default to `false`.

- `aws` (Optional) : AWS configurations. See [AWS Configuration](#aws_configuration) for details. If this option is present, `aws_` options are not expected to be present. If any of `aws_` options are present along with this, error is thrown.

- `socket_timeout`(optional): An integer value indicates the timeout in milliseconds for waiting for data (or, put differently, a maximum period inactivity between two consecutive data packets). A timeout value of zero is interpreted as an infinite timeout. If this timeout value is either negative or not set, the underlying Apache HttpClient would rely on operating system settings for managing socket timeouts.

- `connect_timeout`(optional): An integer value indicates the timeout in milliseconds used when requesting a connection from the connection manager. A timeout value of zero is interpreted as an infinite timeout. If this timeout value is either negative or not set, the underlying Apache HttpClient would rely on operating system settings for managing connection timeouts.

- `username`(optional): A String of username used in the [internal users](https://opensearch.org/docs/latest/security-plugin/access-control/users-roles/) of OpenSearch cluster. Default is null.

- `password`(optional): A String of password used in the [internal users](https://opensearch.org/docs/latest/security-plugin/access-control/users-roles/) of OpenSearch cluster. Default is null.

- `proxy`(optional): A String of the address of a forward HTTP proxy. The format is like "<host-name-or-ip>:\<port\>". Examples: "example.com:8100", "http://example.com:8100", "112.112.112.112:8100". Note: port number cannot be omitted.

- `index_type` (optional): a String from the list [`custom`, `trace-analytics-raw`, `trace-analytics-service-map`, `management_disabled`], which represents an index type. Defaults to `custom` if `aws_serverless` is `false`, otherwise defaults to `management_disabled`. This index_type instructs Sink plugin what type of data it is handling. 

```
    APM trace analytics raw span data type example:
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

    APM trace analytics service map data type example:
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
- <a name="index"></a>`index`: A String used as index name for custom data type. Applicable and required only If index_type is explicitly `custom` or defaults to be `custom`.
  * This index name can be a plain string, such as `application`, `my-index-name`.
  * This index name can also be a plain string plus a date-time pattern as a suffix, such as `application-%{yyyy.MM.dd}`, `my-index-name-%{yyyy.MM.dd.HH}`. When OpenSearch Sink is sending data to OpenSearch, the date-time pattern will be replaced by actual UTC time. The pattern supports all the symbols that represent one hour or above and are listed in [Java DateTimeFormatter](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html). For example, with an index pattern like `my-index-name-%{yyyy.MM.dd}`, a new index is created for each day such as `my-index-name-2022.01.25`. For another example, with an index pattern like `my-index-name-%{yyyy.MM.dd.HH}`, a new index is created for each hour such as `my-index-name-2022.01.25.13`.
  * This index name can also be a formatted string (with or without date-time pattern suffix), such as `my-${index}-name`. When OpenSearchSink is sending data to OpenSearch, the format portion "${index}" will be replaced by it's value in the event that is being processed. The format may also be like "${index1/index2/index3}" in which case the field "index1/index2/index3" is searched in the event and replaced by its value.

- <a name="template_file"></a>`template_file`(optional): A json file path or AWS S3 URI to be read as index template for custom data ingestion. The json file content should be the json value of
`"template"` key in the json content of OpenSearch [Index templates API](https://opensearch.org/docs/latest/opensearch/index-templates/), 
e.g. [otel-v1-apm-span-index-template.json](https://github.com/opensearch-project/data-prepper/blob/main/data-prepper-plugins/opensearch/src/main/resources/otel-v1-apm-span-index-template.json)

- `number_of_shards` (optional): The number of primary shards that an index should have on the destination OpenSearch server. This parameter is effective only when `template_file` is either explicitly provided in Sink configuration or built-in. If this parameter is set, it would override the value in index template file. OpenSearch documentation has [more about this parameter](https://opensearch.org/docs/latest/opensearch/rest-api/index-apis/create-index/).

- `number_of_replicas` (optional): The number of replica shards each primary shard should have on the destination OpenSearch server. For example, if you have 4 primary shards and set number_of_replicas to 3, the index has 12 replica shards. This parameter is effective only when `template_file` is either explicitly provided in Sink configuration or built-in. If this parameter is set, it would override the value in index template file. OpenSearch documentation has [more about this parameter](https://opensearch.org/docs/latest/opensearch/rest-api/index-apis/create-index/).

- `dlq_file`(optional): A String of absolute file path for DLQ failed output records. Defaults to null.
If not provided, failed records will be written into the default data-prepper log file (`logs/Data-Prepper.log`). If the `dlq` option is present along with this, an error is thrown.

- `dlq` (optional): DLQ configurations. See [DLQ](https://github.com/opensearch-project/data-prepper/tree/main/data-prepper-plugins/failures-common/src/main/java/org/opensearch/dataprepper/plugins/dlq/README.md) for details. If the `dlq_file` option is present along with this, an error is thrown.

- `max_retries`(optional): A number indicating the maximum number of times OpenSearch Sink should try to push the data to the OpenSearch server before considering it as failure. Defaults to `Integer.MAX_VALUE`.
If not provided, the sink will try to push the data to OpenSearch server indefinitely because default value is very high and exponential backoff would increase the waiting time before retry.

- `bulk_size` (optional): A long of bulk size in bulk requests in MB. Default to 5 MB. If set to be less than 0,
all the records received from the upstream prepper at a time will be sent as a single bulk request.
If a single record turns out to be larger than the set bulk size, it will be sent as a bulk request of a single document.

- `document_id_field` (optional): A string of document identifier which is used as `id` for the document when it is stored in the OpenSearch. Each incoming record is searched for this field and if it is present, it is used as the id for the document, if it is not present, a unique id is generated by the OpenSearch when storing the document. Standard Data Prepper Json pointer syntax is used for retrieving the value. If the field has "/" in it then the incoming record is searched in the json sub-objects instead of just in the root of the json object. For example, if the field is specified as `info/id`, then the root of the event is searched for `info` and if it is found, then `id` is searched inside it. The value specified for `id` is used as the document id

- `routing_field` (optional): A string of routing field which is used as hash for generating sharding id for the document when it is stored in the OpenSearch. Each incoming record is searched for this field and if it is present, it is used as the routing field for the document, if it is not present, default routing mechanism used by the OpenSearch when storing the document. Standard Data Prepper Json pointer syntax is used for retrieving the value. If the field has "/" in it then the incoming record is searched in the json sub-objects instead of just in the root of the json object. For example, if the field is specified as `info/id`, then the root of the event is searched for `info` and if it is found, then `id` is searched inside it. The value specified for `id` is used as the routing id

- `ism_policy_file` (optional): A String of absolute file path or AWS S3 URI for an ISM (Index State Management) policy JSON file. This policy file is effective only when there is no built-in policy file for the index type. For example, `custom` index type is currently the only one without a built-in policy file, thus it would use the policy file here if it's provided through this parameter. OpenSearch documentation has more about [ISM policies.](https://opensearch.org/docs/latest/im-plugin/ism/policies/)

- `s3_aws_region` (optional): A String represents the region of S3 bucket to read `template_file` or `ism_policy_file`, e.g. us-west-2. Only applies to Amazon OpenSearch Service. Defaults to `us-east-1`.

- `s3_aws_sts_role_arn` (optional): An IAM role arn which the sink plugin will assume to read `template_file` or `ism_policy_file` from S3. If not provided the plugin will use the default credentials.

- `trace_analytics_raw`: No longer supported starting Data Prepper 2.0. Use `index_type` instead.

- `trace_analytics_service_map`: No longer supported starting Data Prepper 2.0. Use `index_type` instead.

- `document_root_key`: The key in the event that will be used as the root in the document. The default is the root of the event. If the key does not exist the entire event is written as the document. If the value at the `document_root_key` is a basic type (ie String, int, etc), the document will have a structure of `{"data": <value of the document_root_key>}`. For example, If we have the following sample event: 

```
{
    status: 200,
    message: null,
    metadata: {
        sourceIp: "123.212.49.58",
        destinationIp: "79.54.67.231",
        bytes: 3545,
        duration: "15 ms"
    }
}
```
With the `document_root_key` set to `status`. The document structure would be `{"data": 200}`. Alternatively if, the `document_root_key` was provided as `metadata`. The document written to OpenSearch would be:

```
{
    sourceIp: "123.212.49.58"
    destinationIp: "79.54.67.231"
    bytes: 3545,
    duration: "15 ms"
}
```

### <a name="aws_configuration">AWS Configuration</a>

* `region` (Optional) : The AWS region to use for credentials. Defaults to [standard SDK behavior to determine the region](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/region-selection.html).
* `sts_role_arn` (Optional) : The STS role to assume for requests to AWS. Defaults to null, which will use the [standard SDK behavior for credentials](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials.html).
* `sts_header_overrides` (Optional): A map of header overrides to make when assuming the IAM role for the sink plugin.
* `serverless` (Optional): A boolean flag to indicate the OpenSearch backend is Amazon OpenSearch Serverless. Default to `false`.

## Metrics
### Management Disabled Index Type

Normally Data Prepper manages the indices it needs within OpenSearch. When `index_type` is set to
`management_disabled`, Data Prepper will not perform any index management on your behalf. You must
provide your own mechanism for creating the indices with the correct mappings applied. Data Prepper
will not use ISM, create templates, or even validate that the index exists. This setting can be
useful when you want to minimize the OpenSearch permissions which you grant to Data Prepper. But,
you should only use it if you are proficient with OpenSearch index management.

With management disabled, Data Prepper can run with only being granted the
`["indices:data/write/index", "indices:data/write/bulk*", "indices:admin/mapping/put"]` permissions on
the desired indices. It is strongly recommend to retain the `"indices:admin/mapping/put"`
permission. If Data Prepper lacks this permission, then it cannot write any documents
that rely on dynamic mapping. You would need to take great care to ensure that every possible field
is explicitly mapped by your index template.

## Metrics

Besides common metrics in [AbstractSink](https://github.com/opensearch-project/data-prepper/blob/main/data-prepper-api/src/main/java/org/opensearch/dataprepper/model/sink/AbstractSink.java), OpenSearch sink introduces the following custom metrics.

### Timer

- `bulkRequestLatency`: measures latency of sending each bulk request including retries.

### Counter

- `bulkRequestErrors`: measures number of errors encountered in sending bulk requests.
- `documentsSuccess`: measures number of documents successfully sent to ES by bulk requests including retries.
- `documentsSuccessFirstAttempt`: measures number of documents successfully sent to ES by bulk requests on first attempt.
- `documentErrors`: measures number of documents failed to be sent by bulk requests.
- `bulkRequestFailed`: measures number of bulk requests failed at the request level.
- `bulkRequestNumberOfRetries`: measures number of times bulk requests are retried.
- `bulkBadRequestErrors`: measures number of errors due to bad bulk requests. `RestStatus` values of `BAD_REQUEST`, `EXPECTATION_FAILED`, `UNPROCESSABLE_ENTITY`, `FAILED_DEPENDENCY`, and `NOT_ACCEPTABLE` are mapped to this errors counter.
- `bulkRequestNotAllowedErrors`: measures number of errors due to requests that are not allowed. `RestStatus` values of `UNAUTHORIZED`, `FORBIDDEN`, `PAYMENT_REQUIRED`, `METHOD_NOT_ALLOWED`, `PROXY_AUTHENTICATION`, `LOCKED`, and `TOO_MANY_REQUESTS` are mapped to this errors counter.
- `bulkRequestInvalidInputErrors`: measures number of errors due to requests with invalid input. `RestStatus` values of `REQUEST_ENTITY_TOO_LARGE`, `REQUEST_URI_TOO_LONG`, `REQUESTED_RANGE_NOT_SATISFIED`, `LENGTH_REQUIRED`, `PRECONDITION_FAILED`, `UNSUPPORTED_MEDIA_TYPE`, and `CONFLICT` are mapped to this errors counter.
- `bulkRequestNotFoundErrors`: measures number of errors due to resource/URI not found. `RestStatus` values of `NOT_FOUND` and `GONE` are mapped to this errors counter.
- `bulkRequestTimeoutErrors`: measures number of requests failed with timeout error. `RestStatus` value of `REQUEST_TIMEOUT` is mapped to this errors counter.
- `bulkRequestServerErrors`: measures the number of requests failed with 5xx errors. `RestStatus` value of 500-599 are mapped to this errors counter.

### End-to-End acknowledgements

If the events received by the OpenSearch Sink have end-to-end acknowledgements enabled (which is tracked using the presence of EventHandle in the event received for processing), then upon successful posting to OpenSearch or upon successful write to DLQ, a positive acknowledgement is sent to the acknowledgementSetManager, otherwise a negative acknowledgement is sent.

### Distribution Summary
- `bulkRequestSizeBytes`: measures the distribution of bulk request's payload sizes in bytes.

## Developer Guide

This plugin is compatible with Java 8. See

- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md) 
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
