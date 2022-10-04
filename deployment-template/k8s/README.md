# Deploying with Kubernetes (k8s)
This directory contains a Kubernetes [configuration file](data-prepper-k8s.yaml) capable of provisioning a Data Prepper cluster.

Use [kubectl](https://kubernetes.io/docs/reference/kubectl/kubectl/) to apply these configurations files with `kubectl apply -f data-prepper-k8s.yaml` 

This will provision a scalable Data Prepper cluster capable of receiving trace data, however it is up to the user to decide how the service will be accessed by clients. A few examples include:
1. Adding a load balancer with an ingress controller (for server-side load balancing)
2. Exposing Kubernetes DNS so that clients can retrieve the addresses of Data Prepper pods (for client-side load balancing)
3. Using something like [ExternalDNS](https://github.com/kubernetes-sigs/external-dns) to synchronize an external DNS (for client-side load balancing)

---

## File Contents
### kind: ConfigMap
Contains a [ConfigMap](https://kubernetes.io/docs/concepts/configuration/configmap/) which provides pods the 2 configuration files required to launch a Data Prepper instance: _pipelines.yaml_ and _data-prepper-config.yaml_.

#### pipelines.yaml
This file contains the standard Trace Analytics usecase pipeline configuration. A few adjustments are needed by the user:
1. Replace the 2 `stdout` sinks with OpenSearch sinks to actually send data to OpenSearch.
2. Optionally enable TLS by providing the necessary key files for the `otel_trace_source` plugin.

#### data-prepper-config.yaml
This file disables TLS for the Data Prepper service APIs (e.g. /metrics), so please adjust this if you wish to enable encryption in transit. It also provides peer forwarder configuration with dns discover mode. Optionally enable TLS and mTLS by providing the necessary certificate files for the peer forwarder. Find more information about core peer forwarder [here](https://github.com/opensearch-project/data-prepper/blob/main/docs/peer_forwarder.md).

### kind: Service
Contains a headless [Service](https://kubernetes.io/docs/concepts/services-networking/service/) which enables Data Prepper pods to discover others pods in the cluster peers via periodic DNS lookups. No changes are required.

### kind: Deployment
Contains a [Deployment](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/) which will create a single Data Prepper pod by default. No changes are required, however you can adjust the initial replica count if desired.
