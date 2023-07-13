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
  processor:
    - add_entries:
        entries:
          - key: "document_id"
            value_expression: "getMetadata(\"document_id\")"
          - key: "index"
            value_expression: "getMetadata(\"index\")"
  sink:
    - opensearch:
        hosts: [ "https://sink-cluster:9200" ]
        username: "username"
        password: "password"
        document_id_field: "document_id"
        index: "copied-${index}"
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
