# Scaling and Tuning

Data Prepper for Trace Analytics in v0.8.x supports both vertical and horizontal scaling. 

To scale vertically, simply adjust the size of your single Data Prepper instance to meet your workload's demands. 

To scale horizontally, deploy multiple Data Prepper instances to form a cluster by using the [Core Peer Forwarder](https://github.com/opensearch-project/data-prepper/blob/main/docs/peer_forwarder.md). This enables Data Preppers to communicate with others in the cluster and is required for horizontally-scaling deployments.

## Scaling Tips

We would like to provide the users with some useful tips for scaling. We recommend the users to modify parameters based on their requirements. Also, monitor the Data Prepper host metrics and OpenSearch metrics to ensure the configuration is working as expected.

### Buffer

The total number of trace requests that Data Prepper is processing is equal to sum of `buffer_size` in `otel-trace-pipeline` and `raw-trace-pipeline`. 

The total number of trace requests inflight to OpenSearch is equal to the product of `batch_size` and `workers` in `raw-trace-pipeline`.

Our recommendation is that
 * have same `buffer_size` in `otel-trace-pipeline` and `raw-trace-pipeline`
 * `buffer_size` >= `workers` * `batch_size` in the `raw-trace-pipeline`
 

### Workers

The `workers` setting determines the number of threads that will be used by Data Prepper to process requests from the buffer. 

Our recommendation is that set the workers based on the CPU utilization, this value can be higher than available processors as the Data Prepper spends significant I/O time in sending data to OpenSearch.

### Heap

You can configure the heap of Data Prepper by setting the `JVM_OPTS` environmental variable. 

Our recommendation is that set the heap value should be minimum `4` * `batch_size` * `otel_send_batch_size` * `maximum size of indvidual span`.

As mentioned in the [setup](trace_setup.md#opentelemetry-collector), set `otel_send_batch_size` as `50` in your opentelemetry collector configuration.

### Disk

Data Prepper uses disk to store metadata required for service-map processing, we store only key fields `traceId`, `spanId`, `parentSpanId`, `spanKind`, `spanName` and `serviceName`. The service-map plugin ensures it only stores two files with each storing `window_duration` seconds of data. In our tests we found that for a throughput of `3000 spans/second`, the total disk usages was `4 MB`.

Data Prepper uses the disk to write logs. In the current version, you can redirect the logs to the path of your preference.


## AWS

[AWS EC2 Cloudformation](../deployment-template/ec2/data-prepper-ec2-deployment-cfn.yaml) template provides user-friendly mechanism to configure the above scaling attributes.

[Kubernetes config files](../deployment-template/k8s/README.md) and [EKS config files](../deployment-template/eks/README.md) are available to configure these attributes in a cluster deployment.

## Benchmark

We ran tests in a `r5.xlarge` with the below configuration,
 
 * `buffer_size` : `4096`
 * `batch_size` : `256`
 * `workers` : 8
 * `Heap` : 10GB
 
The above setup was able to handle a throughput of `2100` spans/second at `20` percent CPU utilization.
 