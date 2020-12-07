# Peer Forwarder

This is a processor that forwards `ExportTraceServiceRequest` through [grpc] towards the given peer Data-Prepper source Ip addresses. 
It regroups spans by trace ID and forward the group to the corresponding peer by consistent hashing. 

## Configuration

Example configuration is as follows:

```
processor:
    - peer_forwarder:
       data_prepper_ips: ["70.162.32.1", "70.162.32.2"]
       time_out: 300
       span_agg_count: 48
```

- `data_prepper_ips`: IP addresses for all running Data-Preppers including the localhost.
- `time_out`: timeout in seconds for sending `ExportTraceServiceRequest`. Defaults to 300
- `span_agg_count`: batch size for number of spans per `ExportTraceServiceRequest`. Defaults to 48.