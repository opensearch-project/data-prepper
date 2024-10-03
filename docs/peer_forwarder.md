
## Peer Forwarder
An HTTP service which performs peer forwarding of `Event` between Data Prepper nodes for aggregation. Currently, supported by `aggregate`, `service_map`, `otel_traces` processors.

Peer Forwarder groups events based on the identification keys provided the processors.
For `service_map` and `otel_traces` it's `traceId` by default and can not be configured.
It's configurable for `aggregate` processor using `identification_keys` configuration option. You can find more information about identification keys [here](https://github.com/opensearch-project/data-prepper/tree/main/data-prepper-plugins/aggregate-processor#identification_keys).

---

Presently peer discovery is provided by either a static list or by a DNS record lookup or AWS Cloudmap.

### Static discovery mode
Static discover mode allows Data Prepper node to discover nodes using a list of IP addresses or domain names.
```yaml
peer_forwarder:
  discovery_mode: static
  static_endpoints: ["data-prepper1", "data-prepper2"]
```

### DNS lookup discovery mode
We recommend using DNS discovery over static discovery when scaling out a Data Prepper cluster. The core concept is to configure a DNS provider to return a list of Data Prepper hosts when given a single domain name.
This is a [DNS A Record](https://www.cloudflare.com/learning/dns/dns-records/dns-a-record/) which indicates a list of IP addresses of a given domain.
```yaml
peer_forwarder:
  discovery_mode: dns
  domain_name: "data-prepper-cluster.my-domain.net"
```

### AWS Cloud Map discovery mode

[AWS Cloud Map](https://docs.aws.amazon.com/cloud-map/latest/dg/what-is-cloud-map.html) provides API-based service discovery as well as DNS-based service discovery.

Peer forwarder can use the API-based service discovery. To support this you must have an existing
namespace configured for API instance discovery. You can create a new one following the instructions
provided by the [Cloud Map documentation](https://docs.aws.amazon.com/cloud-map/latest/dg/working-with-namespaces.html).

Your Data Prepper configuration needs to include:
* `aws_cloud_map_namespace_name` - Set to your Cloud Map Namespace name
* `aws_cloud_map_service_name` - Set to the service name within your specified Namespace
* `aws_region` - The AWS region where your namespace exists.
* `discovery_mode` - Set to `aws_cloud_map`

Your Data Prepper configuration can optionally include:
* `aws_cloud_map_query_parameters` - Key/value pairs to filter the results based on the custom attributes attached to an instance. Only instances that match all the specified key-value pairs are returned.

Example configuration:

```yaml
peer_forwarder:
  discovery_mode: aws_cloud_map
  aws_cloud_map_namespace_name: "my-namespace"
  aws_cloud_map_service_name: "data-prepper-cluster"
  aws_cloud_map_query_parameters:
    instance_type: "r5.xlarge"
  aws_region: "us-east-1"
```

The Data Prepper must also be running with the necessary permissions. The following
IAM policy shows the necessary permissions.

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "CloudMapPeerForwarder",
            "Effect": "Allow",
            "Action": "servicediscovery:DiscoverInstances",
            "Resource": "*"
        }
    ]
}
```
---
## Configuration

* `port`(Optional): An `int` between 0 and 65535 represents the port peer forwarder server is running on. Default value is `4994`.
* `request_timeout`(Optional): Duration - An `int` representing the request timeout in milliseconds for Peer Forwarder HTTP server. Default value is `10000`.
* `server_thread_count`(Optional): An `int` representing number of threads used by Peer Forwarder server. Defaults to `200`.
* `client_thread_count`(Optional): An `int` representing number of threads used by Peer Forwarder client. Defaults to `200`.
* `maxConnectionCount`(Optional): An `int` representing maximum number of open connections for Peer Forwarder server. Default value is `500`.
* `discovery_mode`(Optional): A `String` representing the peer discovery mode to be used. Allowable values are `local_node`, `static`, `dns`, and `aws_cloud_map`. Defaults to `local_node` which processes events locally.
* `static_endpoints`(Optional): A `list` containing endpoints of all Data Prepper instances. Required if `discovery_mode` is set to `static`.
* `domain_name`(Optional): A `String` representing single domain name to query DNS against. Typically, used by creating multiple [DNS A Records](https://www.cloudflare.com/learning/dns/dns-records/dns-a-record/) for the same domain. Required if `discovery_mode` is set to `dns`.
* `aws_cloud_map_namespace_name`(Optional) - A `String` representing the Cloud Map namespace when using AWS Cloud Map service discovery. Required if `discovery_mode` is set to `aws_cloud_map`.
* `aws_cloud_map_service_name`(Optional) - A `String` representing the Cloud Map service when using AWS Cloud Map service discovery. Required if `discovery_mode` is set to `aws_cloud_map`.
* `aws_cloud_map_query_parameters`(Optional): A `Map` of Key/value pairs to filter the results based on the custom attributes attached to an instance. Only instances that match all the specified key-value pairs are returned.
* `buffer_size`(Optional): An `int` representing max number of unchecked records the buffer accepts (num of unchecked records = num of records written into the buffer + num of in-flight records not yet checked by the Checkpointing API). Default is `512`.
* `batch_size`(Optional): An `int` representing max number of records the buffer returns on read. Default is `48`.
* `batch_delay`(Optional): An `int` representing the maximum duration in milliseconds to retrieve `batch_size` records from the peer forwarder buffer. If the `batch_size` has not been reached before this duration is exceeded, a partial batch is used. If this value is set to 0, all available records up to the batch size will be immediately returned. If the buffer is empty, the buffer will block for up to 5 milliseconds to wait for records. Default value is `3000`.
* `aws_region`(Optional) : A `String` represents the AWS region to use `ACM`, `S3` or `AWS Cloud Map`. Required if `use_acm_certificate_for_ssl` is set to `true` or `ssl_certificate_file` and `ssl_key_file` is `AWS S3` path or if `discovery_mode` is set to `aws_cloud_map`.
* `drain_timeout`(Optional) : A `Duration` representing the wait time for the peer forwarder to complete processing data before shutdown.
* `forwarding_batch_size`(Optional) : An `int` representing the maximum number of records to send in each request to a peer. Default value is `1500`, maximum value is `15000`.
* `forwarding_batch_queue_depth`(Optional) : An `int` representing the depth of the batching queue. This value is a scalar used to determine the size of the LinkedBlockingQueues used for batching records before they are sent to a peer. The queue size is determined by the formula: `workers` * `forwarding_batch_size` * `forwarding_batch_queue_depth`. Default value is `1`.
* `forwarding_batch_timeout`(Optional) : A `Duration` representing the maximum time that can occur between flushing batches to a peer. Default is `3s`.

### SSL
The SSL configuration for setting up trust manager for peer forwarding client to connect to other Data Prepper instances.

* `ssl`(Optional) : A `boolean` that enables TLS/SSL. Default value is `true`.
* `ssl_certificate_file`(Optional) : A `String` representing the SSL certificate chain file path or AWS S3 path. S3 path example `s3://<bucketName>/<path>`. Defaults to `config/default_certificate.pem` which is default certificate file. Read more about how the certificate file is generated [here](https://github.com/opensearch-project/data-prepper/tree/main/examples/certificates).
* `ssl_key_file`(Optional) : A `String` represents the SSL key file path or AWS S3 path. S3 path example `s3://<bucketName>/<path>`. Defaults to `config/default_private_key.pem` which is default private key file. Read more about how the private key file is generated [here](https://github.com/opensearch-project/data-prepper/tree/main/examples/certificates).
* `ssl_insecure_disable_verification`(Optional) : A `boolean` that disables the verification of server's TLS certificate chain. Default value is `false`.
* `ssl_fingerprint_verification_only`(Optional) : A `boolean` that disables the verification of server's TLS certificate chain and instead verifies only the certificate fingerprint. Default value is `false`.
* `use_acm_certificate_for_ssl`(Optional) : A `boolean` that enables TLS/SSL using certificate and private key from AWS Certificate Manager (ACM). Default is `false`.
* `acm_certificate_arn`(Optional) : A `String` represents the ACM certificate ARN. ACM certificate take preference over S3 or local file system certificate. Required if `use_acm_certificate_for_ssl` is set to `true`.
* `acm_private_key_password`(Optional) : A `String` that represents the ACM private key password which that will be used to decrypt the private key. If it's not provided, a random password will be generated.
* `acm_certificate_timeout_millis`(Optional) : An `int` representing the timeout in milliseconds for ACM to get certificates. Default value is `120000`.
* `aws_region`(Optional) : A `String` represents the AWS region to use `ACM`, `S3` or `AWS Cloud Map`. Required if `use_acm_certificate_for_ssl` is set to `true` or `ssl_certificate_file` and `ssl_key_file` is `AWS S3` path or if `discovery_mode` is set to `aws_cloud_map`.

```yaml
peer_forwarder:
  ssl: true
  ssl_certificate_file: "<cert-file-path>"
  ssl_key_file: "<private-key-file-path>"
```

### Authentication

* `authentication`(Optional) : A `Map` that enables mTLS. It can either be `mutual_tls` or `unauthenticated`. Default value is `unauthenticated`.
```yaml
peer_forwarder:
  authentication:
    mutual_tls:
```

## Metrics

Core Peer Forwarder introduces the following custom metrics and all the metrics are prefixed by `core.peerForwarder`

### Timer

- `requestForwardingLatency`: measures latency of forwarding requests by peer forwarder client.
- `requestProcessingLatency`: measures latency of processing requests by peer forwarder server.

### Counter

- `requests`: measures total number of forwarded requests.
- `requestsFailed`: measures total number of failed requests. Requests with HTTP response code other than `200`.
- `requestsSuccessful`:  measures total number of successful requests. Requests with HTTP response code `200`.
- `requestsTooLarge`: measures total number of requests which are too large to be written to peer forwarder buffer. Requests with HTTP response code `413`.
- `requestTimeouts`: measures total number of requests which timed out while writing content to peer forwarder buffer. Requests with HTTP response code `408`.
- `requestsUnprocessable`: measures total number of requests which failed due to unprocessable entity. Requests with HTTP response code `422`.
- `badRequests`: measures total number of requests with bad request format. Requests with HTTP response code `400`.
- `recordsSuccessfullyForwarded`: measures total number of forwarded records successfully.
- `recordsFailedForwarding`: measures total number of records failed to be forwarded.
- `recordsToBeForwarded`: measures total number of records to be forwarded.
- `recordsToBeProcessedLocally`: measures total number of records to be processed locally.
- `recordsActuallyProcessedLocally`: measures total number of records actually processed locally. Sum of `recordsToBeProcessedLocally` and `recordsFailedForwarding`.
- `recordsReceivedFromPeers`: measures total number of records received from remote peers.

### Gauge

- `peerEndpoints`: measures number of dynamically discovered peer data-prepper endpoints. For `static` mode, the size is fixed.
