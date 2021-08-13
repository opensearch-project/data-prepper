# Peer Forwarder
This prepper forwards `ExportTraceServiceRequests` via gRPC to other Data Prepper instances.

## Usage
The primary usecase of this prepper is to ensure that groups of traces are aggregated by trace ID and processed by the same Prepper instance.

Presently peer discovery is provided by either a static list (configured in yaml) or by a DNS record lookup.

Static list example:
```
prepper:
    - peer_forwarder:
        time_out: 300
        span_agg_count: 48
        discovery_mode: "static"
        static_endpoints:
        - "data-prepper1"
        - "data-prepper2"
```
DNS lookup example:
```
prepper:
    - peer_forwarder:
        time_out: 300
        span_agg_count: 48
        discovery_mode: "dns"
        domain_name: "data-prepper-cluster.my-domain.net"
```
For DNS cluster setup, see [Operating a Cluster with DNS Discovery](#DNS_Discovery). To
setup for AWS Cloud Map, see [Using AWS CloudMap](#AWS_CloudMap_Discovery).

### <a name="DNS_Discovery"></a>Operating a Cluster with DNS Discovery
DNS discovery is recommended when scaling out a Data Prepper cluster. The core concept is to configure a DNS provider to return a list of Data Prepper hosts when given a single domain name.

#### With Kubernetes (Recommended)
[Kubernetes](https://kubernetes.io/) is the recommended approach to managing a Data Prepper cluster. In addition to handling tasks like failure detection and host replacement, it also maintains an internal DNS service for pod discovery. [Headless services](https://kubernetes.io/docs/concepts/services-networking/service/#headless-services) allow for a single service address to map to all Data Prepper pods. This DNS entry is automatically kept up-to-date as pods are created/replaced/destroyed.

See the /examples/dev/k8s directory for a working example using minikube.

#### With a custom DNS server
A DNS server (like [dnsmasq](http://www.thekelleys.org.uk/dnsmasq/doc.html)) can be configured to maintain a list of Data Prepper hosts via config files. Data Prepper hosts must be configured to use the custom DNS server as their DNS provider. The list of hosts must be manually updated whenever a new Data Prepper host is created. See the [/examples/dev/dns directory](https://github.com/opensearch-project/data-prepper/tree/master/examples/dev/dns) for a set of sample dnsmasq configuration files.

#### With Amazon Route 53 Private Hosted Zones
[Private hosted zones](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/hosted-zones-private.html) enable Amazon Route 53 to "respond to DNS queries for a domain and its subdomains within one or more VPCs that you create with the Amazon VPC service." Similar to the custom DNS server approach, except that Route 53 maintains the list of Data Prepper hosts. Suffers from the same drawback in that the list must be manually kept up-to-date.

### <a name="AWS_CloudMap_Discovery"></a>Using AWS CloudMap

An alternative to DNS discovery is [AWS CloudMap](https://docs.aws.amazon.com/cloud-map/latest/dg/what-is-cloud-map.html).
CloudMap provides API-based service discovery as well as DNS-based service discovery.

Peer forwarder can use the API-based service discovery. To support this you must have an existing
namespace configured for API instance discovery. You can create a new one following the instructions
provided by the [CloudMap documentation](https://docs.aws.amazon.com/cloud-map/latest/dg/working-with-namespaces.html).

Your pipeline configuration needs to include:

* `awsCloudMapNamespaceName` - Set to your CloudMap Namespace name
* `awsCloudMapServiceName` - Set to the service name within your specified Namespace
* `awsRegion` - The AWS region where your namespace exists.
* `discovery_mode` - Set to `aws_cloud_map`

Example configuration:

```
  prepper:
    - peer_forwarder:
        discovery_mode: aws_cloud_map
        awsCloudMapNamespaceName: my-namespace
        awsCloudMapServiceName: data-prepper-cluster
        awsRegion: us-east-1
```

The DataPrepper must also be running with the necessary permissions. The following
IAM policy shows the necessary permissions.

```
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

## Configuration

* `time_out`: timeout in seconds for sending `ExportTraceServiceRequest`. Defaults to 3 seconds.
* `span_agg_count`: batch size for number of spans per `ExportTraceServiceRequest`. Defaults to 48.
* `target_port`: the destination port to forward requests to. Defaults to `21890`
* `discovery_mode`: peer discovery mode to be used. Allowable values are `static`, `dns`, and `aws_cloud_map`. Defaults to `static`
* `static_endpoints`: list containing endpoints of all Data Prepper instances.
* `domain_name`: single domain name to query DNS against. Typically used by creating multiple [DNS A Records](https://www.cloudflare.com/learning/dns/dns-records/dns-a-record/) for the same domain.
* `awsCloudMapNamespaceName` - specifies the CloudMap namespace when using AWS CloudMap service discovery
* `awsCloudMapServiceName` - specifies the CloudMap service when using AWS CloudMap service discovery

### SSL
The SSL configuration for setting up trust manager for peer forwarding client to connect to other Data Prepper instances. The SSL configuration should be same as the one used for OTel Trace Source.

* `ssl(Optional)` => A boolean enables TLS/SSL. Default is ```true```.
* `sslKeyCertChainFile(Optional)` => A `String` represents the SSL certificate chain file path or AWS S3 path. S3 path example ```s3://<bucketName>/<path>```. Required if ```ssl``` is set to ```true```.
* `useAcmCertForSSL(Optional)` => A boolean enables TLS/SSL using certificate and private key from AWS Certificate Manager (ACM). Default is ```false```.
* `acmCertificateArn(Optional)` => A `String` represents the ACM certificate ARN. ACM certificate take preference over S3 or local file system certificate. Required if ```useAcmCertForSSL``` is set to ```true```.
* `awsRegion(Optional)` => A `String` represents the AWS region to use ACM or S3. Required if ```useAcmCertForSSL``` is set to ```true``` or ```sslKeyCertChainFile``` and ```sslKeyFile``` is ```AWS S3 path```.


## Metrics

Besides common metrics in [AbstractPrepper](https://github.com/opensearch-project/data-prepper/blob/main/data-prepper-api/src/main/java/com/amazon/dataprepper/model/prepper/AbstractPrepper.java), peer-forwarder introduces the following custom metrics.

### Timer

- `latency`: measures latency of forwarded requests.

### Counter

- `requests`: measures total number of forwarded requests.
- `errors`: measures number of failed requests.

### Gauge

- `peerEndpoints`: measures number of dynamically discovered peer data-prepper endpoints. For `static` mode, the size is fixed.

## Developer Guide

This plugin is compatible with Java 14. See

- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md) 
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/readme/monitoring.md)
