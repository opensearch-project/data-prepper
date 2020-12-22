# Peer Forwarder
This processor forwards `ExportTraceServiceRequest` via gRPC to other Data Prepper instances. The primary usecase of this processor is 
to ensure that groups of traces are aggregated by trace ID and processed by the same Prepper instance.

Presently peer discovery is provided by either a static list (configured in yaml) or by a DNS record lookup.

## Configuration
Static list example:
```
processor:
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
processor:
    - peer_forwarder:
        time_out: 300
        span_agg_count: 48
        discovery_mode: "dns"
        hostname_for_dns_lookup: "data-prepper-cluster"
```

* `time_out`: timeout in seconds for sending `ExportTraceServiceRequest`. Defaults to 3 seconds.
* `span_agg_count`: batch size for number of spans per `ExportTraceServiceRequest`. Defaults to 48.
* `discovery_mode`: peer discovery mode to be used. Allowable values are `static` and `dns`. Defaults to `static`
* `static_endpoints`: list containing endpoints of all Data Prepper instances
* `hostname_for_dns_lookup`: single hostname to query DNS against. Typically used by creating multiple [DNS A Records] (https://www.cloudflare.com/learning/dns/dns-records/dns-a-record/) for the same domain
* `ssl` => Default is ```false```.
* `sslKeyCertChainFile` => Should be provided if ```ssl``` is set to ```true```