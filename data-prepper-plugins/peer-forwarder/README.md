# Peer Forwarder
This prepper forwards `ExportTraceServiceRequests` via gRPC to other Data Prepper instances. The primary usecase of this prepper is 
to ensure that groups of traces are aggregated by trace ID and processed by the same Prepper instance.

Presently peer discovery is provided by either a static list (configured in yaml) or by a DNS record lookup.

## Configuration
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

* `time_out`: timeout in seconds for sending `ExportTraceServiceRequest`. Defaults to 3 seconds.
* `span_agg_count`: batch size for number of spans per `ExportTraceServiceRequest`. Defaults to 48.
* `discovery_mode`: peer discovery mode to be used. Allowable values are `static` and `dns`. Defaults to `static`
* `static_endpoints`: list containing endpoints of all Data Prepper instances.
* `domain_name`: single domain name to query DNS against. Typically used by creating multiple [DNS A Records](https://www.cloudflare.com/learning/dns/dns-records/dns-a-record/) for the same domain.
* `ssl` => Default is ```false```.
* `sslKeyCertChainFile` => Should be provided if ```ssl``` is set to ```true```

## Operating a Cluster with DNS Discovery
DNS discovery is recommended when scaling out a Data Prepper cluster. The core concept is to configure a DNS provider to return a list of Data Prepper hosts when given a single domain name.

### With a custom DNS server
A DNS server (like [dnsmasq](http://www.thekelleys.org.uk/dnsmasq/doc.html)) can be configured to maintain a list of Data Prepper hosts via config files. Data Prepper hosts must be configured to use the custom DNS server as their DNS provider. The list of hosts must be manually updated whenever a new Data Prepper host is created. See the [/examples/dev/dns directory](https://github.com/opendistro-for-elasticsearch/data-prepper/tree/master/examples/dev/dns) for a set of sample dnsmasq configuration files.

### With Amazon Route 53 Private Hosted Zones
[Private hosted zones](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/hosted-zones-private.html) enable Amazon Route 53 to "respond to DNS queries for a domain and its subdomains within one or more VPCs that you create with the Amazon VPC service." Similar to the custom DNS server approach, except that Route 53 maintains the list of Data Prepper hosts. Suffers from the same drawback in that the list must be manually kept up-to-date.

### With Kubernetes (Recommended)
[Kubernetes](https://kubernetes.io/) is the recommended approach to managing a Data Prepper cluster. In addition to handling tasks like failure detection and host replacement, it also maintains an internal DNS service for pod discovery. [Headless services](https://kubernetes.io/docs/concepts/services-networking/service/#headless-services) allow for a single service address to map to all Data Prepper pods. This DNS entry is automatically kept up-to-date as pods are created/replaced/destroyed.
