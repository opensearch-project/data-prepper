# 2021-03-25 Version 0.8.0 (beta)

## Highlights:
* Horizontally-scaling clusters can now be deployed by using the new _Peer Forwarder_ plugin. 
  * Refer to the [README file](https://github.com/opendistro-for-elasticsearch/data-prepper/blob/main/data-prepper-plugins/peer-forwarder/README.md) for usage instructions
  * See the [deployment templates directory](https://github.com/opendistro-for-elasticsearch/data-prepper/tree/main/deployment-template) for new container-based deployment strategies.
* [Prometheus](https://prometheus.io/)-friendly metrics can now be scraped via a new /metrics endpoint.
  * See the [monitoring docs](../../docs/readme/monitoring.md) for more information

---

## Peer Forwarder Plugin
To enable horizontal scaling, it is expected to run Data Prepper instances in a cluster with requests distributed by a load balancer. This complicates preppers which maintain state between requests, specifically those which require that only one instance process all spans for a given trace. Peer forwarding facilitates this usecase by ensuring all spans for a given trace arrive at a specific Data Prepper instance.

The Peer Forwarding plugin enables instances in a Data Prepper cluster to forward requests to other specific instances within the cluster. Peer forwarding is accomplished by a consistent hashing algorithm to ensure that all requests for a given trace are routed to the same Data Prepper instance. The algorithm maps requests to addresses of known instances - these instances are provided via a peer discovery mechanism. As of version 0.8.0, peer discovery can be provided by either a static address list or by dynamic DNS lookups.

Refer to the [README file](https://github.com/opendistro-for-elasticsearch/data-prepper/blob/main/data-prepper-plugins/peer-forwarder/README.md) for usage instructions.

---

## Metrics
Version 0.8.0-beta offers APIs which provide JVM/system metrics and Plugin metrics.

Users can use these endpoints to connect to their existing [Prometheus](https://prometheus.io/) instance to start aggregating metrics. Additional tools such as [Grafana](https://grafana.com/) can then be used to provide dashboards and alerting.

See the [monitoring docs](../../docs/readme/monitoring.md) for a list of default metrics emitted by each component type (buffers, sinks, etc.)
