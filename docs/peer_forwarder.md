
## Peer Forwarder
An HTTP service which performs peer forwarding of `Event` between Data Prepper nodes for aggregation. Currently, supported by `aggregate`, `service_map_stateful`, `otel_trace_raw` processors.

Peer Forwarder groups events based on the identification keys provided the processors.
For `service_map_stateful` and `otel_trace_raw` it's `traceId` by default and can not be configured.
It's configurable for `aggregate` processor using `identification_keys` configuration option. You can find more information about identification keys [here](https://github.com/opensearch-project/data-prepper/tree/main/data-prepper-plugins/aggregate-processor#identification_keys).

---

Presently peer discovery is provided by either a static list or by a DNS record lookup or AWS Cloudmap.

### Static discovery mode
```yaml
peer_forwarder:
  discovery_mode: static
  static_endpoints: ["data-prepper1", "data-prepper2"]
```
### DNS lookup discovery mode
```yaml
peer_forwarder:
  discovery_mode: dns
  domain_name: "data-prepper-cluster.my-domain.net"
```
For DNS cluster setup, see [Operating a Cluster with DNS Discovery](#DNS_Discovery).

### AWS Cloud Map discovery mode

[AWS CloudMap](https://docs.aws.amazon.com/cloud-map/latest/dg/what-is-cloud-map.html) provides API-based service discovery as well as DNS-based service discovery.

Peer forwarder can use the API-based service discovery. To support this you must have an existing
namespace configured for API instance discovery. You can create a new one following the instructions
provided by the [CloudMap documentation](https://docs.aws.amazon.com/cloud-map/latest/dg/working-with-namespaces.html).

Example configuration:

```yaml
peer_forwarder:
  discovery_mode: aws_cloud_map
  awsCloudMapNamespaceName: "my-namespace"
  awsCloudMapServiceName: "data-prepper-cluster"
  awsCloudMapQueryParameters:
    instance_type: "r5.xlarge"
  awsRegion: "us-east-1"
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

* `port`(Optional): An `int` between 0 and 65535 represents the port peer forwarder server is running on. Default value is `21892`.
* `request_timeout`(Optional): Duration - A int representing the request timeout in milliseconds for Peer Forwarder HTTP server. Default value is `10000`.
* `server_thread_count`(Optional): An `int` representing number of threads used by Peer Forwarder server. Defaults to `200`.
* `client_thread_count`(Optional): An `int` representing number of threads used by Peer Forwarder client. Defaults to `200`.
* `maxConnectionCount`(Optional): An `int` representing maximum number of open connections for Peer Forwarder server. Default value is `500`.
* `discovery_mode`(Optional): A `String` representing the peer discovery mode to be used. Allowable values are `local_node`, `static`, `dns`, and `aws_cloud_map`. Defaults to `local_node` which processes events locally.
* `static_endpoints`(Optional): A `list` containing endpoints of all Data Prepper instances. Required if `discovery_mode` is set to `static`.
* `domain_name`(Optional): A `String` representing single domain name to query DNS against. Typically, used by creating multiple [DNS A Records](https://www.cloudflare.com/learning/dns/dns-records/dns-a-record/) for the same domain. Required if `discovery_mode` is set to `dns`.
* `awsCloudMapNamespaceName`(Optional) - A `String` representing the CloudMap namespace when using AWS CloudMap service discovery. Required if `discovery_mode` is set to `aws_cloud_map`.
* `awsCloudMapServiceName`(Optional) - A `String` representing the CloudMap service when using AWS CloudMap service discovery. Required if `discovery_mode` is set to `aws_cloud_map`.
* `aws_cloud_map_query_parameters`(Optional): A `Map` of Key/value pairs to filter the results based on the custom attributes attached to an instance. Only instances that match all the specified key-value pairs are returned.
* `buffer_size`(Optional): An `int` representing max number of unchecked records the buffer accepts (num of unchecked records = num of records written into the buffer + num of in-flight records not yet checked by the Checkpointing API). Default is `512`.
* `batch_size`(Optional): An `int` representing max number of records the buffer returns on read. Default is `48`.

### SSL
The SSL configuration for setting up trust manager for peer forwarding client to connect to other Data Prepper instances.

* `ssl`(Optional) : A `boolean` that enables TLS/SSL. Default value is `false`.
* `ssl_certificate_file`(Optional) : A `String` representing the SSL certificate chain file path or AWS S3 path. S3 path example `s3://<bucketName>/<path>`. Required if `ssl` is set to `true`.
* `ssl_key_file`(Optional) : A `String` represents the SSL key file path or AWS S3 path. S3 path example `s3://<bucketName>/<path>`. Required if `ssl` is set to `true`.
* `ssl_insecure_disable_verification`(Optional) : A `boolean` that disables the verification of server's TLS certificate chain. Default value is `false`.
* `use_acm_certificate_for_ssl`(Optional) : A `boolean` that enables TLS/SSL using certificate and private key from AWS Certificate Manager (ACM). Default is `false`.
* `acm_certificate_arn`(Optional) : A `String` represents the ACM certificate ARN. ACM certificate take preference over S3 or local file system certificate. Required if `use_acm_certificate_for_ssl` is set to `true`.
* `acm_certificate_timeout_millis`(Optional) : An 'int' representing the timeout in milliseconds for ACM to get certificates. Default value is `120000`.
* `aws_region`(Optional) : A `String` represents the AWS region to use `ACM`, `S3` or `AWS Cloudmap`. Required if `use_acm_certificate_for_ssl` is set to `true` or `ssl_certificate_file` and `ssl_key_file` is `AWS S3` path or if `discovery_mode` is set to `aws_cloud_map`.

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

### <a name="DNS_Discovery"></a>Operating a Cluster with DNS Discovery
DNS discovery is recommended when scaling out a Data Prepper cluster. The core concept is to configure a DNS provider to return a list of Data Prepper hosts when given a single domain name.

#### With Kubernetes (Recommended)
[Kubernetes](https://kubernetes.io/) is the recommended approach to managing a Data Prepper cluster. In addition to handling tasks like failure detection and host replacement, it also maintains an internal DNS service for pod discovery. [Headless services](https://kubernetes.io/docs/concepts/services-networking/service/#headless-services) allow for a single service address to map to all Data Prepper pods. This DNS entry is automatically kept up-to-date as pods are created/replaced/destroyed.

See the /examples/dev/k8s directory for a working example using minikube.

#### With a custom DNS server
A DNS server (like [dnsmasq](http://www.thekelleys.org.uk/dnsmasq/doc.html)) can be configured to maintain a list of Data Prepper hosts via config files. Data Prepper hosts must be configured to use the custom DNS server as their DNS provider. The list of hosts must be manually updated whenever a new Data Prepper host is created. See the [/examples/dev/dns directory](https://github.com/opensearch-project/data-prepper/tree/master/examples/dev/dns) for a set of sample dnsmasq configuration files.

#### With Amazon Route 53 Private Hosted Zones
[Private hosted zones](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/hosted-zones-private.html) enable Amazon Route 53 to "respond to DNS queries for a domain and its subdomains within one or more VPCs that you create with the Amazon VPC service." Similar to the custom DNS server approach, except that Route 53 maintains the list of Data Prepper hosts. Suffers from the same drawback in that the list must be manually kept up-to-date.



