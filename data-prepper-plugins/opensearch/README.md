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

### Service map trace analytics

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

### Log analytics

```
pipeline:
  ...
  sink:
    opensearch:
      hosts: ["https://localhost:9200"]
      cert: path/to/cert
      username: YOUR_USERNAME_HERE
      password: YOUR_PASSWORD_HERE
      index_type: log-analytics
      dlq_file: /your/local/dlq-file
      max_retries: 20
      bulk_size: 4
```

The OpenSearch sink will reserve `logs-otel-v1-*` as index pattern and `logs-otel-v1` as index alias for record ingestion.

### Metric analytics

```
pipeline:
  ...
  sink:
    opensearch:
      hosts: ["https://localhost:9200"]
      cert: path/to/cert
      username: YOUR_USERNAME_HERE
      password: YOUR_PASSWORD_HERE
      index_type: metric-analytics
      dlq_file: /your/local/dlq-file
      max_retries: 20
      bulk_size: 4
```

The OpenSearch sink will reserve `metric-otel-v1-*` as index pattern and `metric-otel-v1` as index alias for record ingestion.


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

- `aws_sigv4`: A boolean flag to sign the HTTP request with AWS credentials. Only applies to Amazon OpenSearch Service. See [security](security.md) for details. Default to `false`.

- `aws_region`: A String represents the region of Amazon OpenSearch Service domain, e.g. us-west-2. Only applies to Amazon OpenSearch Service. Defaults to `us-east-1`.

- `aws_sts_role_arn`: A IAM role arn which the sink plugin will assume to sign request to Amazon OpenSearch Service. If not provided the plugin will use the default credentials.

- `aws_sts_external_id`: An optional external ID to use when assuming an IAM role.

- `aws_sts_header_overrides`: An optional map of header overrides to make when assuming the IAM role for the sink plugin.

- `insecure`: A boolean flag to turn off SSL certificate verification. If set to true, CA certificate verification will be turned off and insecure HTTP requests will be sent. Default to `false`.

- `aws` (Optional) : AWS configurations. See [AWS Configuration](#aws_configuration) for details. SigV4 is enabled by default when this option is used. If this option is present, `aws_` options are not expected to be present. If any of `aws_` options are present along with this, error is thrown.

- `socket_timeout`(optional): An integer value indicates the timeout in milliseconds for waiting for data (or, put differently, a maximum period inactivity between two consecutive data packets). A timeout value of zero is interpreted as an infinite timeout. If this timeout value is either negative or not set, the underlying Apache HttpClient would rely on operating system settings for managing socket timeouts.

- `connect_timeout`(optional): An integer value indicates the timeout in milliseconds used when requesting a connection from the connection manager. A timeout value of zero is interpreted as an infinite timeout. If this timeout value is either negative or not set, the underlying Apache HttpClient would rely on operating system settings for managing connection timeouts.

- `username`(optional): A String of username used in the [internal users](https://opensearch.org/docs/latest/security-plugin/access-control/users-roles/) of OpenSearch cluster. Default is null.

- `password`(optional): A String of password used in the [internal users](https://opensearch.org/docs/latest/security-plugin/access-control/users-roles/) of OpenSearch cluster. Default is null.

- `proxy`(optional): A String of the address of a forward HTTP proxy. The format is like "<host-name-or-ip>:\<port\>". Examples: "example.com:8100", "http://example.com:8100", "112.112.112.112:8100". Note: port number cannot be omitted.

- `index_type` (optional): a String from the list [`custom`, `trace-analytics-raw`, `trace-analytics-service-map`, `metric-analytics`, `log-analytics`, `management_disabled`], which represents an index type. Defaults to `custom` if `serverless` is `false` in [AWS Configuration](#aws_configuration), otherwise defaults to `management_disabled`. This index_type instructs Sink plugin what type of data it is handling.

- `enable_request_compression` (optional): A boolean that enables or disables request compression when sending requests to OpenSearch. For `distribution_version` set to `es6`, default value is `false`, otherwise default value is `true`.

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
- <a name="index"></a>`index`: A String used as index name for custom data type. Applicable and required only If `index_type` is explicitly `custom` or defaults to be `custom`.
  * This index name can be a plain string, such as `application`, `my-index-name`.
  * This index name can also be a plain string with a date-time pattern, such as `application-%{yyyy.MM.dd}`, `my-index-name-%{yyyy.MM.dd.HH}`, `index-%{yyyy-MM-dd}-dev`. When OpenSearch Sink is sending data to OpenSearch, the date-time pattern will be replaced by actual UTC time. The pattern supports all the symbols that represent one hour or above and are listed in [Java DateTimeFormatter](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html). For example, with an index pattern like `my-index-name-%{yyyy.MM.dd}`, a new index is created for each day such as `my-index-name-2022.01.25`. For another example, with an index pattern like `my-index-name-%{yyyy.MM.dd.HH}`, a new index is created for each hour such as `my-index-name-2022.01.25.13`.
  * This index name can also be a formatted string (with or without date-time pattern suffix), such as `my-${index}-name`. When OpenSearchSink is sending data to OpenSearch, the format portion "${index}" will be replaced by it's value in the event that is being processed. The format may also be like "${index1/index2/index3}" in which case the field "index1/index2/index3" is searched in the event and replaced by its value.
    - Additionally, the formatted string can include expressions to evaluate to format the index name. For example, `my-${index}-${getMetadata(\"some_metadata_key\")}-name` will inject both the `index` value from the Event, as well as the value of `some_metadata_key` from the Event metadata to construct the index name.
- `normalize_index` (optional): If true, the plugin will try to make dynamic index names (index names with format options specified in `${}`) valid according to [index naming restrictions](https://opensearch.org/docs/2.11/api-reference/index-apis/create-index/#index-naming-restrictions). Any invalid characters will be removed. Default value is false. 
- <a name="template_type"></a>`template_type`(optional): Defines what type of OpenSearch template to use. The available options are `v1` and `index-template`. The default value is `v1`, which uses the original OpenSearch templates available at the `_template` API endpoints. Select `index-template` to use composable index templates which are available at OpenSearch's `_index_template` endpoint. Note: when `distribution_version` is `es6`, `template_type` is enforced into `v1`.

- <a name="template_file"></a>`template_file`(optional): A json file path or AWS S3 URI to be read as index template for custom data ingestion. The json file content should be the json value of
`"template"` key in the json content of OpenSearch [Index templates API](https://opensearch.org/docs/latest/opensearch/index-templates/),
e.g. [otel-v1-apm-span-index-template.json](https://github.com/opensearch-project/data-prepper/blob/main/data-prepper-plugins/opensearch/src/main/resources/otel-v1-apm-span-index-template.json)

- `number_of_shards` (optional): The number of primary shards that an index should have on the destination OpenSearch server. This parameter is effective only when `template_file` is either explicitly provided in Sink configuration or built-in. If this parameter is set, it would override the value in index template file. OpenSearch documentation has [more about this parameter](https://opensearch.org/docs/latest/opensearch/rest-api/index-apis/create-index/).

- `number_of_replicas` (optional): The number of replica shards each primary shard should have on the destination OpenSearch server. For example, if you have 4 primary shards and set number_of_replicas to 3, the index has 12 replica shards. This parameter is effective only when `template_file` is either explicitly provided in Sink configuration or built-in. If this parameter is set, it would override the value in index template file. OpenSearch documentation has [more about this parameter](https://opensearch.org/docs/latest/opensearch/rest-api/index-apis/create-index/).

- `dlq_file`(optional): A String of absolute file path for DLQ failed output records. Defaults to null.
If not provided, failed records will be written into the default data-prepper log file (`logs/Data-Prepper.log`). If the `dlq` option is present along with this, an error is thrown.

- `action`(optional): A string indicating the type of action to be performed. Supported values are "create", "update", "upsert", "delete" and "index". Default value is "index". It also be an expression which evaluates to one of the supported values mentioned earlier.

- `actions`(optional): This is an alternative to `action`. `actions` can have multiple actions, each with a condition. The first action for which the condition evaluates to true is picked as the action for an event. The action must be one of the supported values mentioned under `action` field above. Just like in case of `action`, the `type` mentioned in `actions` can be an expression which evaluates to one of the supported values. For example, the following configuration shows different action types for different conditions.

```
  sink:
    - opensearch
        actions:
          - type: "create"
             when: "/some_key == CREATE"
          - type: "index"
             when: "/some_key == INDEX"
          - type: "upsert"
             when: "/some_key == UPSERT"
          - type: "update"
             when: "/some_key == UPDATE"
          - type: "delete"
             when: "/some_key == DELETE"
         # default case
          - type: "index"
```

- `dlq` (optional): DLQ configurations. See [DLQ](https://github.com/opensearch-project/data-prepper/tree/main/data-prepper-plugins/failures-common/src/main/java/org/opensearch/dataprepper/plugins/dlq/README.md) for details. If the `dlq_file` option is present along with this, an error is thrown.

- `max_retries`(optional): A number indicating the maximum number of times OpenSearch Sink should try to push the data to the OpenSearch server before considering it as failure. Defaults to `Integer.MAX_VALUE`.
If not provided, the sink will try to push the data to OpenSearch server indefinitely because default value is very high and exponential backoff would increase the waiting time before retry.

- `bulk_size` (optional): A long of bulk size in bulk requests in MB. Default to 5 MB. If set to be less than 0,
all the records received from the upstream prepper at a time will be sent as a single bulk request.
If a single record turns out to be larger than the set bulk size, it will be sent as a bulk request of a single document.

- `estimate_bulk_size_using_compression` (optional): A boolean dictating whether to compress the bulk requests when estimating
the size. This option is ignored if request compression is not enabled for the OpenSearch client. This is an experimental
feature and makes no guarantees about the accuracy of the estimation. Default is false.

- `max_local_compressions_for_estimation` (optional): An integer of the maximum number of times to compress a partially packed
bulk request when estimating its size. Bulk size accuracy increases with this value but performance degrades. This setting is experimental
and is ignored unless `estimate_bulk_size_using_compression` is enabled. Default is 2.

- `flush_timeout` (optional): A long of the millisecond duration to try packing a bulk request up to the bulk_size before flushing.
If this timeout expires before a bulk request has reached the bulk_size, the request will be flushed as-is. Set to -1 to disable
the flush timeout and instead flush whatever is present at the end of each batch. Default is 60,000, or one minute.

- `document_id_field` (optional) (deprecated) : A string of document identifier which is used as `id` for the document when it is stored in the OpenSearch. Each incoming record is searched for this field and if it is present, it is used as the id for the document, if it is not present, a unique id is generated by the OpenSearch when storing the document. Standard Data Prepper Json pointer syntax is used for retrieving the value. If the field has "/" in it then the incoming record is searched in the json sub-objects instead of just in the root of the json object. For example, if the field is specified as `info/id`, then the root of the event is searched for `info` and if it is found, then `id` is searched inside it. The value specified for `id` is used as the document id. This field can also be a Data Prepper expression that is evaluated to determine the document_id_field. For example, setting to `getMetadata(\"some_metadata_key\")` will use the value of the metadata key as the `document_id`

- `document_id` (optional): A string of document identifier which is used as `id` for the document when it is stored in the OpenSearch. Each incoming record is searched for this field and if it is present, it is used as the id for the document, if it is not present, a unique id is generated by the OpenSearch when storing the document. Standard Data Prepper Json pointer syntax is used for retrieving the value. If the field has "/" in it then the incoming record is searched in the json sub-objects instead of just in the root of the json object. For example, if the field is specified as `info/id`, then the root of the event is searched for `info` and if it is found, then `id` is searched inside it. The value specified for `id` is used as the document id. This field can also be a Data Prepper expression that is evaluated to determine the `document_id`. For example, setting to `getMetadata(\"some_metadata_key\")` will use the value of the metadata key as the document_id
  * This `document_id` string can also be a formatted string, such as `doc-${docId}-name`. When OpenSearchSink is sending data to OpenSearch, the format portion "${docId}" will be replaced by it's value in the event that is being processed. The format may also be like "${docId1/docId2/docId3}" in which case the field "docId1/docId2/docId3" is searched in the event and replaced by its value.
  * Additionally, the formatted string can include expressions to evaluate to format the document id. For example, `my-${docId}-${getMetadata(\"some_metadata_key\")}-name` will inject both the `docId` value from the Event, as well as the value of `some_metadata_key` from the Event metadata to construct the document id.

- `routing_field` (optional) (deprecated) : A string of routing field which is used as hash for generating sharding id for the document when it is stored in the OpenSearch. Each incoming record is searched for this field and if it is present, it is used as the routing field for the document, if it is not present, default routing mechanism used by the OpenSearch when storing the document. Standard Data Prepper Json pointer syntax is used for retrieving the value. If the field has "/" in it then the incoming record is searched in the json sub-objects instead of just in the root of the json object. For example, if the field is specified as `info/id`, then the root of the event is searched for `info` and if it is found, then `id` is searched inside it. The value specified for `id` is used as the `routing id`

- `routing` (optional): A string which is used as hash for generating sharding id for the document when it is stored in the OpenSearch. Each incoming record is searched for this field and if it is present, it is used as the routing field for the document, if it is not present, default routing mechanism used by the OpenSearch when storing the document. Standard Data Prepper Json pointer syntax is used for retrieving the value. If the field has "/" in it then the incoming record is searched in the json sub-objects instead of just in the root of the json object. For example, if the field is specified as `info/id`, then the root of the event is searched for `info` and if it is found, then `id` is searched inside it. The value specified for `id` is used as the routing id.
  * This routing string can also be a formatted string, such as `routing-${rid}-name`. When OpenSearchSink is sending data to OpenSearch, the format portion "${rid}" will be replaced by it's value in the event that is being processed. The format may also be like "${rid1/rid2/rid3}" in which case the field "rid1/rid2/rid3" is searched in the event and replaced by its value.
  * Additionally, the formatted string can include expressions to evaluate to format the routing string. For example, `my-${rid}-${getMetadata(\"some_metadata_key\")}-name` will inject both the `rid` value from the Event, as well as the value of `some_metadata_key` from the Event metadata to construct the routing string.
  Examples:
  1. Routing config with simple strings
```
  sink:
    opensearch:
      hosts: ["https://your-amazon-opensearch-service-endpoint"]
      aws_sigv4: true
      cert: path/to/cert
      insecure: false
      routing: "test_routing_string"
      bulk_size: 4
```

  2. Routing config with keys from event
```
  sink:
    opensearch:
      hosts: ["https://your-amazon-opensearch-service-endpoint"]
      aws_sigv4: true
      cert: path/to/cert
      insecure: false
      routing: "${/info/id}"
      bulk_size: 4
```

  3. Routing config with more complex expressions
```
  sink:
    opensearch:
      hosts: ["https://your-amazon-opensearch-service-endpoint"]
      aws_sigv4: true
      cert: path/to/cert
      insecure: false
      routing: '${/info/id}-test-${getMetadata("metadata_key")}'
      bulk_size: 4
```

- `pipeline` (optional): A string which is used to represent the pipeline Id for preprocessing documents. Each incoming record is searched for this field and if it is present, it is used as the pipeline field for the document. Standard Data Prepper Json pointer syntax is used for retrieving the value. If the field has "/" in it then the incoming record is searched in the json sub-objects instead of just in the root of the json object. For example, if the field is specified as `info/id`, then the root of the event is searched for `info` and if it is found, then `id` is searched inside it. The value specified for `id` is used as the pipeline id. This field can also be a Data Prepper expression that is evaluated to determine the `pipeline_id`. For example, setting to `getMetadata(\"some_metadata_key\")` will use the value of the metadata key as the pipeline_id.

- `ism_policy_file` (optional): A String of absolute file path or AWS S3 URI for an ISM (Index State Management) policy JSON file. This policy file is effective only when there is no built-in policy file for the index type. For example, `custom` index type is currently the only one without a built-in policy file, thus it would use the policy file here if it's provided through this parameter. OpenSearch documentation has more about [ISM policies.](https://opensearch.org/docs/latest/im-plugin/ism/policies/)

- `s3_aws_region` (optional): A String represents the region of S3 bucket to read `template_file` or `ism_policy_file`, e.g. us-west-2. Only applies to Amazon OpenSearch Service. Defaults to `us-east-1`.

- `s3_aws_sts_role_arn` (optional): An IAM role arn which the sink plugin will assume to read `template_file` or `ism_policy_file` from S3. If not provided the plugin will use the default credentials.

- `s3_aws_sts_external_id` (optional): An external ID that be attached to Assume Role requests.

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
- `include_keys`: A list of keys to be included (retained). The key in the list cannot contain '/'. This option can work together with `document_root_key`.

For example, If we have the following sample event:
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
if `include_keys` is set to ["status", "metadata"], the document written to OpenSearch would be:
```
{
    status: 200,
    metadata: {
        sourceIp: "123.212.49.58"
    }
}
```
if you have also set `document_root_key` as "metadata", and the include_keys as ["sourceIp, "bytes"], the document written to OpenSearch would be:
```
{
   sourceIp: "123.212.49.58",
   bytes: 3545
}
```

- `exclude_keys`: Similar to include_keys except any keys in the list will be excluded. Note that you should not have both include_keys and exclude_keys in the configuration at the same time.

For example, If we have the following sample event:
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
if `exclude_keys` is set to ["message", "status"], the document written to OpenSearch would be:
```
{
    metadata: {
        sourceIp: "123.212.49.58",
        destinationIp: "79.54.67.231",
        bytes: 3545,
        duration: "15 ms"
    }
}
```
- `distribution_version`: A String indicating whether the sink backend version is Elasticsearch 6 or above (i.e. Elasticsearch 7.x or OpenSearch). `es6` represents Elasticsearch 6; `default` represents latest compatible backend version (Elasticsearch 7.x, OpenSearch 1.x, OpenSearch 2.x). Default to `default`.

### <a name="aws_configuration">AWS Configuration</a>

* `region` (Optional) : The AWS region to use for credentials. Defaults to [standard SDK behavior to determine the region](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/region-selection.html).
* `sts_role_arn` (Optional) : The STS role to assume for requests to AWS. Defaults to null, which will use the [standard SDK behavior for credentials](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials.html).
* `sts_header_overrides` (Optional): A map of header overrides to make when assuming the IAM role for the sink plugin.
* `serverless` (Optional): A boolean flag to indicate the OpenSearch backend is Amazon OpenSearch Serverless. Default to `false`. Notice that [ISM policies.](https://opensearch.org/docs/latest/im-plugin/ism/policies/) is not supported in Amazon OpenSearch Serverless and thus any ISM related configuration value has no effect, i.e. `ism_policy_file`.
* `serverless_options` (Optional): Additional options you can specify when using serverless.

#### <a name="serverless_configuration">Serverless Configuration</a>
* `network_policy_name` (Optional): The serverless network policy name being used. If both `collection_name` and `vpce_id` are specified, then this network policy will be attempted to be created or update. On the managed OpenSearch Ingestion Service, the `collection_name` and `vpce_id` fields are automatically set.
* `collection_name` (Optional): The serverless collection name.
* `vpce_id` (Optional): The VPCE ID connected to Amazon OpenSearch Serverless.

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

# OpenSearch Source

This is the Date Prepper OpenSearch source plugin that processes indices for either OpenSearch, Elasticsearch,
or Amazon OpenSearch Service clusters. It is ideal for migrating index data from a cluster.

Note: Only fully tested versions will be listed below. It is likely many more versions are supported already, but it is untested.

The OpenSearch source is compatible with the following OpenSearch versions:
* 2.5

And is compatible with the following Elasticsearch versions:
* 7.10

# Usages

### Minimum required config with username and password

```yaml
opensearch-source-pipeline:
  source:
    opensearch:
      connection:
        insecure: true
      hosts: [ "https://localhost:9200" ]
      username: "username"
      password: "password"
```

### Full config example

```yaml
opensearch-source-pipeline:
  source:
    opensearch:
      indices:
        include:
          - index_name_regex: "test-index-.*"
        exclude:
          - index_name_regex: "test-index-[1-9].*"
      scheduling:
        rate: "PT1H"
        start_time: "2023-06-02T22:01:30.00Z"
        job_count: 2
      search_options:
        search_context_type: "none"
        batch_size: 1000
      connection:
        insecure: false
        cert: "/path/to/cert.crt"
        socket_timeout: "100ms"
        connection_timeout: "100ms"
      hosts: [ "https://localhost:9200" ]
      username: "username"
      password: "password"
```

### Amazon OpenSearch Service

The OpenSearch source can also be configured for an Amazon OpenSearch Service domain.

```yaml
opensearch-source-pipeline:
  source:
    opensearch:
      connection:
        insecure: true
      hosts: [ "https://search-my-domain-soopywaovobopgs8ywurr3utsu.us-east-1.es.amazonaws.com" ]
      aws:
        region: "us-east-1"
        sts_role_arn: "arn:aws:iam::123456789012:role/my-domain-role"
```

### Using Metadata

When the OpenSearch source constructs Data Prepper Events from documents in the cluster, the
document index is stored in the `EventMetadata` with an `opensearch-index` key, and the document_id is
stored in the `EventMetadata` with a `opensearch-document_id` key. This allows conditional routing based on the index or document_id,
among other things. For example, one could send to an OpenSearch sink and use the same index and document_id from the source cluster in
the destination cluster. A full config example for this use case is below

```yaml
opensearch-source-pipeline:
  source:
    opensearch:
      connection:
        insecure: true
      hosts: [ "https://source-cluster:9200" ]
      username: "username"
      password: "password"
  sink:
    - opensearch:
        hosts: [ "https://sink-cluster:9200" ]
        username: "username"
        password: "password"
        document_id_field: "getMetadata(\"opensearch-document_id\")"
        index: "${getMetadata(\"opensearch-index\"}"
```

## Configuration

- `hosts` (Required) : A list of IP addresses of OpenSearch or Elasticsearch nodes.


- `username` (Optional) : A String of username used in the internal users of OpenSearch cluster. Default is null.


- `password` (Optional) : A String of password used in the internal users of OpenSearch cluster. Default is null.


- `disable_authentication` (Optional) : A boolean that can disable authentication if the cluster supports it. Defaults to false.


- `aws` (Optional) : AWS configurations. See [AWS Configuration](#aws_configuration) for details. SigV4 is enabled by default when this option is used.


- `search_options` (Optional) : See [Search Configuration](#search_configuration) for details


- `indices` (Optional): See [Indices Configurations](#indices_configuration) for filtering options.


- `scheduling` (Optional): See [Scheduling Configuration](#scheduling_configuration) for details


- `connection` (Optional): See [Connection Configuration](#connection_configuration)

### <a name="aws_configuration">AWS Configuration</a>

* `region` (Optional) : The AWS region to use for credentials. Defaults to [standard SDK behavior to determine the region](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/region-selection.html).


* `sts_role_arn` (Optional) : The STS role to assume for requests to AWS. Defaults to null, which will use the [standard SDK behavior for credentials](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials.html).


* `sts_header_overrides` (Optional): A map of header overrides to make when assuming the IAM role for the source plugin.

### <a name="search_configuration">Search Configuration</a>

* `search_context_type` (Optional) : A direct override for which type of search context should be used to search documents.
  Options include `point_in_time`, `scroll`, or `none` (just search after).
  By default, the OpenSearch source will attempt to use `point_in_time` on a cluster by auto-detecting that the cluster version and distribution
  supports Point in Time. If the cluster does not support `point_in_time`, then `scroll` is the default behavior.


* `batch_size` (Optional) : The amount of documents to read in at once while searching.
  This size is passed to the search requests for all search context types (`none` (search_after), `point_in_time`, or `scroll`).
  Defaults to 1,000.

### <a name="scheduling_configuration">Scheduling Configuration</a>

Schedule the start time and amount of times an index should be processed. For example,
a `rate` of `PT1H` and a `job_count` of 3 would result in each index getting processed 3 times, starting at `start_time`
and then every hour after the first time the index is processed.

* `rate` (Optional) : A String that indicates the rate to process an index based on the `job_count`.
  Supports ISO_8601 notation Strings ("PT20.345S", "PT15M", etc.) as well as simple notation Strings for seconds ("60s") and milliseconds ("1500ms").
  Defaults to 8 hours, and is only applicable when `job_count` is greater than 1.



* `job_count` (Optional) : An Integer that specifies how many times each index should be processed. Defaults to 1.



* `start_time` (Optional) : A String in the format of a timestamp that is compatible with Java Instant (i.e. `2023-06-02T22:01:30.00Z`).
  Processing will be delayed until this timestamp is reached. The default start time is to start immediately.

### <a name="connection_configuration">Connection Configuration</a>

* `insecure` (Optional): A boolean flag to turn off SSL certificate verification. If set to true, CA certificate verification will be turned off and insecure HTTP requests will be sent. Default to false.


* `cert` (Optional) : CA certificate that is pem encoded. Accepts both .pem or .crt. This enables the client to trust the CA that has signed the certificate that the OpenSearch cluster is using. Default is null.


* `socket_timeout` (Optional) : A String that indicates the timeout duration for waiting for data. Supports ISO_8601 notation Strings ("PT20.345S", "PT15M", etc.) as well as simple notation Strings for seconds ("60s") and milliseconds ("1500ms"). If this timeout value not set, the underlying Apache HttpClient would rely on operating system settings for managing socket timeouts.


* `connection_timeout` (Optional) : A String that indicates the timeout duration used when requesting a connection from the connection manager. Supports ISO_8601 notation Strings ("PT20.345S", "PT15M", etc.) as well as simple notation Strings for seconds ("60s") and milliseconds ("1500ms"). If this timeout value is either negative or not set, the underlying Apache HttpClient would rely on operating system settings for managing connection timeouts.

### <a name="indices_configuration">Indices Configuration</a>

Can be used to filter which indices should be processed.
An index will be processed if its name matches one of the `index_name_regex`
patterns in the `include` list, and does not match any of the pattern in the `exclude` list.
The default behavior is to process all indices.

* `include` (Optional) : A List of [Index Configuration](#index_configuration) that defines which indices should be processed. Defaults to an empty list.


* `exclude` (Optional) : A List of [Index Configuration](#index_configuration) that defines which indices should not be processed.

#### <a name="index_configuration">Index Configuration</a>

* `index_name_regex`: A regex pattern to represent the index names for filtering

## Developer guide

### Integration tests

#### Run OpenSearch

Start an instance of OpenSearch that listens to opens port 9200 with default user admin:admin.
```
docker run -p 9200:9200 -e "discovery.type=single-node" -e "OPENSEARCH_INITIAL_ADMIN_PASSWORD=yourStrongPassword123!" opensearchproject/opensearch:latest
```

#### Run tests

```
./gradlew data-prepper-plugins:opensearch:integrationTest -Dtests.opensearch.host=localhost:9200 -Dtests.opensearch.user=admin -Dtests.opensearch.password=yourStrongPassword123!
```
