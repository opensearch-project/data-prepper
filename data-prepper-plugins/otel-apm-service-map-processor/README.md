# OpenTelemetry APM Service Map Processor

## Overview

The `otel_apm_service_map` processor analyzes OpenTelemetry trace spans to automatically generate Application Performance Monitoring (APM) service map relationships and metrics. It creates structured events that can be visualized as service topology graphs, showing how services communicate with each other and their performance characteristics.

## Key Features

- **Service Relationship Discovery**: Automatically identifies service-to-service connections from OpenTelemetry spans
- **APM Metrics Generation**: Creates latency, throughput, and error rate metrics for service interactions
- **Three-Window Processing**: Uses sliding time windows to ensure complete trace context
- **Environment-Aware**: Supports service environment grouping and custom attributes
- **Off-Heap Storage**: Efficient memory usage with MapDB for large-scale processing
- **Real-Time Processing**: Generates service map data as traces are processed

## Configuration

### Basic Configuration

```yaml
processor:
  - otel_apm_service_map:
      window_duration: 60s
      db_path: "data/otel-apm-service-map/"
      group_by_attributes:
        - "service.version"
        - "deployment.environment"
```

### Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `window_duration` | Duration | `60s` | Fixed time window in seconds for evaluating APM service map relationships |
| `db_path` | String | `"data/otel-apm-service-map/"` | Directory path for database files storing transient processing data |
| `group_by_attributes` | List\<String\> | `[]` | OpenTelemetry resource attributes to include in service grouping |
| `metric_timestamp_source` | String | `"arrival_time"` | Timestamp source for emitted metrics. `"arrival_time"` uses processing time at window evaluation (avoids late-span data loss in Prometheus/AMP). `"span_end_time"` uses the span's `endTime` field. |
| `metric_timestamp_granularity` | String | `"seconds"` | Truncation granularity for metric and service map timestamps. `"seconds"` truncates to second boundaries (1s collision window). `"minutes"` truncates to minute boundaries (60s collision window). |
| `dependency_nodes.enabled` | Boolean | `true` | Synthesize typed `database`, `external` and `messaging` nodes, broker edges and their RED metrics. See [External Dependency Nodes](#external-dependency-nodes). |
| `dependency_nodes.max_dependencies_per_service` | Integer | `100` | Distinct dependency names per source service per window; further dependencies collapse into `OtherRemoteService`. |
| `dependency_nodes.max_remote_operations_per_service` | Integer | `100` | Distinct dependency remote operations per source service per window; further operations collapse into `OtherRemoteOperation`. |
| `dependency_nodes.hostname_denylist_patterns` | List\<String\> | IP-derived host names | Regular expressions (case-insensitive, full match) for peer host names that must not create a node. |

### Advanced Configuration

```yaml
processor:
  - otel_apm_service_map:
      window_duration: 120s  # 2-minute windows for high-latency services
      db_path: "/tmp/apm-service-map/"
      metric_timestamp_source: arrival_time
      metric_timestamp_granularity: seconds
      group_by_attributes:
        - "service.version"
        - "deployment.environment"
        - "service.namespace"
        - "k8s.cluster.name"
```

### Metric Timestamp Source

The `metric_timestamp_source` option controls what timestamp is used for emitted metrics.

| Value | Timestamp used | Late-span safe | Description |
|---|---|---|---|
| `arrival_time` (default) | `clock.instant()` at window evaluation | Yes | All spans in a window share the same processing timestamp. Matches the [OTel Collector spanmetrics connector](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/connector/spanmetricsconnector) approach. |
| `span_end_time` | Span's `endTime` field | No | Each span's end time is used. Late-arriving spans may produce metrics with timestamps that collide with previously written data points, causing silent data loss in Prometheus/AMP. |

**Recommendation:** Use the default `arrival_time` unless you have a specific requirement for span-aligned timestamps and accept the risk of late-span data loss.

### Metric Timestamp Granularity

The `metric_timestamp_granularity` option controls the truncation granularity for all emitted timestamps (metrics and service map events).

| Value | Collision window (`span_end_time` mode) | Data points per window | Description |
|---|---|---|---|
| `seconds` (default) | 1 second | More (one per unique second) | Truncates to second boundaries. Minimizes collision risk in `span_end_time` mode. |
| `minutes` | 60 seconds | Fewer (one per unique minute) | Truncates to minute boundaries. Higher collision risk but fewer data points. |

In `arrival_time` mode, granularity has minimal impact since all spans in a window share the same `clock.instant()` — each window always produces one data point per label combination regardless of truncation.

## Environment Derivation

For every emitted `NodeOperationDetail` the processor populates `sourceNode.keyAttributes.environment` (and the matching `targetNode.keyAttributes.environment` on edge events) by inspecting OTel resource attributes — and where applicable span attributes — on the underlying spans. The same value is also written to each raw span as `attributes.derived.environment` by the companion `otel_traces` processor.

Lookup precedence (first match wins):

1. AWS platform detection from `cloud.platform` (and a few platform-specific signals listed below).
2. `deployment.environment.name` from `resource.attributes`.
3. `deployment.environment` from `resource.attributes` (legacy OTel key).
4. Default fallback: `generic:default`.

For every AWS attribute the processor checks both span-level attributes and `resource.attributes` (since most OTel SDKs emit them in the resource).

| Resource (or span) attributes | `environment` value |
|---|---|
| `cloud.platform=aws_api_gateway` (+ optional `aws.api_gateway.stage=<s>`) | `api-gateway:<s>` |
| `cloud.platform=aws_ec2` | `ec2:default` |
| `cloud.platform=aws_ecs` + `aws.ecs.launchtype=fargate` | `ecs-fargate:default` |
| `cloud.platform=aws_ecs` + `aws.ecs.launchtype=ec2` | `ecs-ec2:default` |
| `cloud.platform=aws_ecs` (launchtype absent or other) | `ecs:default` |
| `cloud.platform=aws_eks` | `eks:default` |
| `cloud.platform=aws_elastic_beanstalk` | `elastic-beanstalk:default` |
| `cloud.platform=aws_lambda` | `lambda:default` |
| `cloud.resource_id` starts with `arn:aws:lambda:` | `lambda:default` |
| `aws.lambda.invoked_arn` is set | `lambda:default` |
| `cloud.provider=aws` + `faas.name=<n>` | `lambda:default` |
| `deployment.environment.name=<env>` | `<env>` |
| `deployment.environment=<env>` | `<env>` (legacy) |
| (none of the above) | `generic:default` |

Attribute names follow the OpenTelemetry semantic conventions for [cloud](https://opentelemetry.io/docs/specs/semconv/registry/attributes/cloud/) and [AWS](https://opentelemetry.io/docs/specs/semconv/registry/attributes/aws/).

## Pipeline Examples

### Basic Pipeline

```yaml
version: "2"
otel-apm-service-map-pipeline:
  source:
    otel_trace_source:
      ssl: false
      port: 21890
  route:
      - service_map_events : '/eventType == "SERVICE_MAP"'
      - service_processed_metrics : '/eventType == "METRIC"'
  processor:
    - otel_apm_service_map:
        window_duration: 60s
        db_path: "data/otel-apm-service-map/"
  sink:
    - opensearch:
        hosts: ["https://localhost:9200"]
        index_type: otel-v2-apm-service-map
        username: "admin"
        password: "admin"
        routes: [service_map_events]
    - prometheus:
        ...
        routes: [service_processed_metrics]
```

### Multi-Environment Setup

```yaml
version: "2"
multi-env-apm-pipeline:
  source:
    otel_trace_source:
      ssl: false
      port: 21890
  route:
      - service_map_events : '/eventType == "SERVICE_MAP"'
      - service_processed_metrics : '/eventType == "METRIC"'
  processor:
    - otel_apm_service_map:
        window_duration: 90s
        db_path: "data/multi-env-service-map/"
        group_by_attributes:
          - "deployment.environment"
          - "service.version"
          - "service.namespace"
  sink:
    - prometheus:
        ...
        routes: [service_processed_metrics]
    - opensearch:
        hosts: ["https://localhost:9200"]
        index_type: otel-v2-apm-service-map
        routes: [service_map_events]
```

## Output Events

The processor generates two types of output events:

- **NodeOperationDetail events** (`eventType: "SERVICE_MAP"`) - Service topology and operation relationships
- **Metric events** (`eventType: "METRIC"`) - Aggregated performance metrics

### NodeOperationDetail Events

NodeOperationDetail is a unified event type that represents both service-to-service connections and operation-level relationships through dual hash fields.

#### Service Connection (Edge Event)

Represents a connection between two services with operation details:

```json
{
  "eventType": "SERVICE_MAP",
  "sourceNode": {
    "keyAttributes": {
      "environment": "production",
      "serviceName": "user-service"
    },
    "groupByAttributes": {
      "service.version": "1.2.3",
      "deployment.environment": "production"
    },
    "type": "service"
  },
  "targetNode": {
    "keyAttributes": {
      "environment": "production",
      "serviceName": "auth-service"
    },
    "groupByAttributes": {
      "service.version": "2.1.0"
    },
    "type": "service"
  },
  "sourceOperation": {
    "name": "GET /api/users",
    "attributes": {}
  },
  "targetOperation": {
    "name": "GET /validate",
    "attributes": {}
  },
  "nodeConnectionHash": "abc123",
  "operationConnectionHash": "def456",
  "timestamp": "2023-12-01T12:00:00Z"
}
```

#### Leaf Node Event

Represents a service with no outgoing calls:

```json
{
  "eventType": "SERVICE_MAP",
  "sourceNode": {
    "keyAttributes": {
      "environment": "production",
      "serviceName": "database-service"
    },
    "groupByAttributes": {},
    "type": "service"
  },
  "targetNode": null,
  "sourceOperation": {
    "name": "query",
    "attributes": {}
  },
  "targetOperation": null,
  "nodeConnectionHash": "ghi789",
  "operationConnectionHash": null,
  "timestamp": "2023-12-01T12:00:00Z"
}
```

### External Dependency Nodes

Downstream targets that do not emit their own `SERVER` span — databases, message brokers, and external services — are synthesized as typed nodes so they appear in the service map instead of being dropped. This is **enabled by default**, matching how common APM tools show uninstrumented dependencies. Set `enabled: false` to keep the output of earlier releases.

```yaml
processor:
  - otel_apm_service_map:
      dependency_nodes:
        enabled: true                            # default
        max_dependencies_per_service: 100        # default
        max_remote_operations_per_service: 100   # default
        # hostname_denylist_patterns replaces the default list when set
        hostname_denylist_patterns:
          - '^ip-\d{1,3}-\d{1,3}-\d{1,3}-\d{1,3}(\..*)?$'
          - '^egress-proxy$'
```

**Rollout.** Use an OpenSearch Dashboards version that includes the dependency-aware APM UI ([dashboards-observability#2898](https://github.com/opensearch-project/dashboards-observability/pull/2898) / [OpenSearch-Dashboards#12771](https://github.com/opensearch-project/OpenSearch-Dashboards/pull/12771)). Older Dashboards build the services list from `sourceNode`/`targetNode` without filtering on `type`, so they list databases, brokers and external endpoints as services; with an older Dashboards version, set `dependency_nodes.enabled: false`.

When enabled, a node's `type` is one of:

| `type` | Synthesized from | Example name |
|--------|------------------|--------------|
| `service` | An instrumented service (has a `SERVER` span) | `checkout` |
| `database` | A `CLIENT` span with `db.*` attributes and no child `SERVER` span | `postgresql:orders-db.internal` |
| `external` | A `CLIENT` span with HTTP/RPC/peer attributes and no child `SERVER` span | `api.example.com`, `AWS::DynamoDB` |
| `messaging` | A `PRODUCER`/`CONSUMER` span; the broker links producer → broker → consumer | `kafka:orders` |

The node **name** is best-effort peer identity:

- **database**: `{db.system}:{host}` (host from `server.address` / `net.peer.name` / `network.peer.address`), else `{db.system}:{db.namespace}`, else `{db.system}`. Flattened system keys (`db_system`, `db_system_name`, `db.system_name`) name the node the same way as dotted ones. IP-literal, loopback and denylisted hosts are skipped in favor of the next form. A span with `peer.service`, or an AWS SDK call (`rpc.system=aws-api`), keeps that name (e.g. `AWS::DynamoDB`). The plain system stays available as `dependencyAttributes["db.system.name"]`.
- **external**: `host:port` as derived by `OTelSpanDerivationUtil`, with the scheme-default ports `80` and `443` dropped so `api.example.com` and `api.example.com:443` are one node.
- **messaging**: `{messaging.system}:{messaging.destination.name}` (or the legacy `messaging.destination`).

The name is for display and grouping; it is deliberately not treated as a precise instance key. The `dependencyAttributes` map is the reliable identity — the exact peer attributes under canonical OpenTelemetry keys — so consumers filter the dependency's spans/logs on those rather than parsing the name. The field is omitted for `service` nodes and is **not** part of node identity (see below).

- **database**: `db.system.name`, `db.namespace`, `server.address`, `server.port`
- **messaging**: `messaging.system`, `messaging.destination.name` (the directional `messaging.operation` is emitted as the edge's operation, not here)
- **external**: `peer.service`, `server.address`, `server.port`, `rpc.system` (`url.full` is intentionally excluded — it carries per-request paths, query strings, and PII)

Unresolved (`UnknownRemoteService`), raw-IP (IPv4 or IPv6 literals, with or without a port), loopback (`localhost`, `*.localhost`) and denylisted peers are suppressed so the topology only shows named dependencies. The default denylist matches IP-derived host names: EC2 (`ip-10-0-0-5.ec2.internal`, `ec2-1-2-3-4.compute-1.amazonaws.com`) and Kubernetes pod DNS (`10-0-0-5.ns.pod.cluster.local`). Names that merely contain several colons, such as `AWS::DynamoDB` or an SNS topic ARN destination, are kept.

Synthesis also skips calls that are already represented or are not dependencies:

- **Instrumented peers.** An `external` peer whose `peer.service`, `server.address` or host (or the first label of a `*.svc` Kubernetes name) matches a service seen with a `SERVER` span in the three windows is not synthesized; it is that service with its `SERVER` span missing from the trace.
- **Nested `CLIENT` spans.** When an SDK span wraps a transport span in the same service (for example botocore over urllib3), only the outermost is synthesized. When an inner `CLIENT` span reaches a traced service, the outer one is not synthesized.
- **Repeated `CONSUMER` spans.** For one message, a `CONSUMER` span nested under a `CONSUMER` span for the same destination is skipped, and a non-`process` span (e.g. `receive`) is skipped when the trace has a `process` span for the same service and destination.

**Operations.** A dependency edge's source operation is the parent `SERVER` operation, else the nearest `CONSUMER` ancestor's operation, else the span's own operation; the same value is used in the topology and in the metric `operation` label. A `PRODUCER` edge uses the same rule. The remote operation of an HTTP call is `METHOD {url.template | http.route | first path segment}`, and just the path part when the method is missing. The full URL is never used.

**Brokers are shared.** A broker node always uses the environment `generic:default`, so producers and consumers in different environments connect through one node. Broker metrics use `remoteEnvironment=generic:default`.

**Consumer metrics.** `CONSUMER` spans emit client-style series keyed `service=<consumer>`, `remoteService=<broker>`, `remoteOperation=<messaging.operation>`, even though the topology edge is `broker -> consumer`. To chart a `broker -> consumer` edge, query the consumer's series filtered by `remoteService=<broker>`.

**Cardinality.** Per window, each source service (environment + name) may emit at most `max_dependencies_per_service` distinct dependency names and `max_remote_operations_per_service` distinct (dependency, remote operation) pairs. The values with the most calls in the window are admitted, ties broken by name, so the admitted set does not depend on the order traces are processed in. The rest become `OtherRemoteService` / `OtherRemoteOperation` in both the topology and the metrics, and `dependencyAttributes` is dropped for the overflow node. Calls collapsed into `OtherRemoteService` always use `OtherRemoteOperation` and do not count against the operation cap. `OtherRemoteService` is a reserved name: a real dependency with that exact name (for example from `peer.service`) keeps its own operations and attributes, but shares the overflow node in the topology. Two counters count collapsed calls, not nodes, with one increment per collapsed call: `dependencyCallsOverflowed` counts calls (including consumed messages) collapsed into `OtherRemoteService`, and `dependencyRemoteOperationCallsOverflowed` counts calls to an admitted dependency collapsed into `OtherRemoteOperation`. Service-to-service edges are not capped. Each Data Prepper node applies the caps independently.

**Index mapping.** The `otel-v2-apm-service-map` index template (version 1) maps `*.dependencyAttributes.*` strings as `keyword` with a `keyword` sub-field. Existing indices keep their mapping until the next rollover; indices created before the template use dynamic `text` with a `.keyword` sub-field. For exact `term` filters and aggregations, use `<attr>.keyword` (for example `targetNode.dependencyAttributes.server.address.keyword`): that path works on indices created before and after the template, so one query covers both behind the alias.

`dependencyAttributes` is **descriptive metadata, not identity**: it is excluded from a node's `equals`/`hashCode`, so it never affects `nodeConnectionHash`. This keeps existing service-node hashes byte-stable across an upgrade and keeps one logical dependency a single shared node even when a descriptive value (e.g. per-host DB attributes) varies between callers.

```json
{
  "targetNode": {
    "type": "database",
    "keyAttributes": { "environment": "eks:prod", "name": "postgresql:orders-db.internal" },
    "dependencyAttributes": {
      "db.system.name": "postgresql",
      "db.namespace": "orders",
      "server.address": "orders-db.internal",
      "server.port": "5432"
    }
  }
}
```

#### Known limitations

- **Window-boundary over-classification.** A `CLIENT` call to an instrumented service is normally resolved to that service via its child `SERVER` span. When the child is missing and the peer name does not match the service name (for example `checkout-svc` vs `checkout`), the target is still classified as `external` for those spans.
- **External nodes per caller environment.** An `external` or `database` node inherits the caller's environment, so a shared SaaS API called from several environments appears once per environment.
- **Proxies and sidecars.** Calls through an egress proxy or sidecar resolve to the proxy. Loopback names are suppressed; add other proxy host names to `hostname_denylist_patterns`.
- **Path cardinality.** Without `url.template`/`http.route`, the first path segment is used, so versioned APIs collapse to `GET /v1` and ID segments produce one operation per ID up to the cap.
- **Cap stability across windows.** Each window is admitted on its own, with no memory of earlier windows. A dependency whose call count ranks near the cap can therefore show its name in one window and `OtherRemoteService` in the next. Ties are broken by case-sensitive name order, so whenever call counts tie at the cap, the names that sort later are the ones collapsed.
- **Database port.** When the database system is known, a database node is named without the port, so that one database reached with and without an explicit default port stays one node. Two instances on one host (`db1:5432` and `db1:5433`) of the same system therefore merge into one node. `dependencyAttributes["server.port"]` shows the port of only one of the merged calls, or none if that call had no port.
- **External peers named like a service.** An `external` peer whose `peer.service`, `server.address` or host equals the name of a traced service (for example a third-party host `payments` next to a traced `payments` service) is assumed to be that service with its `SERVER` span missing. No dependency node is synthesized, so the call is left out of the map and of the client metrics. A different `peer.service` alone does not help while the host still matches. Call the peer by a fully qualified host name (for example `api.payments.example.com`), which is not matched.
- **Ephemeral messaging destinations.** Per-connection or per-request destinations (e.g. RabbitMQ `amq.gen-*` reply queues) each mint a distinct broker name until the cap is reached.
- **Host-less database spans.** Instrumentations that omit `server.address` (e.g. Go `XSAM/otelsql` without `otelsql.AttributesFromDSN`) produce a host-less `{db.system}` node (or `{db.system}:{db.namespace}` when a namespace is present) that is separate from the `{db.system}:{host}` node other callers of the same database produce. Emit the DSN attributes (`otelsql.AttributesFromDSN`) or set the same `peer.service` on every caller's spans, to merge them.
- **AWS SDK messaging.** Producers instrumented only with `rpc.system=aws-api` (no `messaging.system`) are not yet synthesized as messaging edges.

### Dual Hash Fields

NodeOperationDetail uses two hash fields for different query patterns:

- **`nodeConnectionHash`**: Hash of `sourceNode + targetNode`. Use `GROUP BY nodeConnectionHash` to get the service topology graph.
- **`operationConnectionHash`**: Hash of `sourceNode + targetNode + sourceOperation + targetOperation`. Use `GROUP BY operationConnectionHash` to get operation-level detail. Only present when both operations are known.

### Generated Metrics

The processor generates time-series metrics as JacksonMetric events:

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `request` | Sum (monotonic) | `1` | Number of requests |
| `error` | Sum (monotonic) | `1` | Number of error requests (HTTP 4xx) |
| `fault` | Sum (monotonic) | `1` | Number of fault requests (HTTP 5xx or ERROR status) |
| `latency` | Histogram | `s` | Request latency distribution |

**Histogram Bucket Boundaries:**
`[0.0, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0]`

All metrics use **delta aggregation temporality** (values are cumulative within each window only).

**Host Label:**

Every metric carries a `service_map_processor_host_id` label, so the series written by one instance
stay distinct from another's. The value is a truncated SHA-256 hash of the host identity resolved by
`HostContext`, which keeps the hostname itself out of the metrics.

> **Behavior change:** the identity behind this label used to be the local hostname alone, which is
> unresolvable in an ordinary container. It now falls back to the `HOSTNAME` environment variable, a
> local interface address, and a routing lookup. An instance which previously reported `localhost` or
> `unknown` therefore emits a different `service_map_processor_host_id` after upgrading, so queries
> which group by it see the old series end and a new one begin. Instances which already resolved a
> hostname are unaffected.

## Algorithm: NodeOperationDetail Generation

### OTel Trace Structure

For any cross-service call, OpenTelemetry produces this span pattern:

```
Service A: SERVER span (s1, op="GET /api/users")
  +-- Service A: INTERNAL span (i1)              [optional, 0 or more]
        +-- Service A: CLIENT span (c1)
              +-- Service B: SERVER span (s2, op="GET /users")
```

- **SERVER span**: handles an incoming request to the service
- **INTERNAL span**: intermediate processing within the service
- **CLIENT span**: makes an outgoing call to another service
- Parent-child links are via `spanId` / `parentSpanId`

### Three-Window Architecture

Spans are stored across three MapDB-backed time windows:

```
|  Previous Window  |  Current Window   |  Next Window    |
|  (old spans for   |  (being processed |  (incoming spans |
|   lookup context) |   this cycle)     |   accumulating)  |
```

- **nextWindow**: where ALL incoming spans are written (every `doExecute` call)
- **currentWindow**: processed when `windowDuration` elapses
- **previousWindow**: kept for lookup context (helps complete traces that span windows)

### Processing Flow

The pipeline framework calls `doExecute(records)` repeatedly. Each call follows this order:

```
doExecute(records):
    // STEP 1: Check window FIRST (before storing new spans)
    if windowDurationHasPassed():
        apmEvents = evaluateApmEvents()    // Phase 1 + Phase 2 + rotate
    else:
        apmEvents = EMPTY

    // STEP 2: Store incoming spans AFTER (always into nextWindow)
    for each span in records:
        spanData = processSpan(span)       // raw extraction only
        nextWindow.put(traceId, spanData)

    return apmEvents
```

**Critical ordering**: Step 1 happens BEFORE Step 2. New spans from the current batch go into the post-rotation nextWindow and are NOT included in the window being processed.

### Window Rotation

When `windowDurationHasPassed()` is true:

```
evaluateApmEvents():
    barrier.await()              // sync all processor threads
    if isMasterInstance():
        apmEvents = processCurrentWindowSpans()   // Phase 1 + Phase 2
        rotateWindows()
    barrier.await()              // sync again
    return apmEvents

rotateWindows():
    temp = previousWindow
    previousWindow = currentWindow    // just-processed becomes context
    currentWindow  = nextWindow       // accumulated spans become next to process
    nextWindow     = temp             // reuse old previous (cleared)
    nextWindow.clear()
    previousTimestamp = now
```

### Phase 1: Span Decoration (Two Passes)

Decoration runs on spans from ALL 3 windows to build relationships.

**Pass 1 - Decorate CLIENT spans:** For each CLIENT span, find its direct child SERVER span to learn the remote service:

```
c1 (CLIENT, service A)
  +-- child s2 (SERVER, service B)

ClientSpanDecoration(c1) = {
    parentServerOperationName: null,      <-- not yet known
    remoteService: "B",                   <-- from s2.serviceName
    remoteOperation: "GET /users",        <-- from s2.operationName
    remoteEnvironment: s2.environment,
    remoteGroupByAttributes: s2.groupByAttributes
}
```

**Pass 2 - Decorate SERVER spans + back-annotate CLIENT spans:** For each SERVER span, find CLIENT descendants from the same service via BFS:

```
s1 (SERVER, service A, op="GET /api/users")
  +-- ... INTERNAL spans ...
        +-- c1 (CLIENT, service A)

BFS from s1 finds c1 (same service, CLIENT kind)

ClientSpanDecoration(c1) UPDATED = {
    parentServerOperationName: "GET /api/users",   <-- NOW FILLED from s1
    remoteService: "B",                             <-- unchanged
    remoteOperation: "GET /users",                  <-- unchanged
}
```

The BFS walks through children, continuing as long as the child is from the **same service**, and collects CLIENT spans found along the way.

### Phase 2: NodeOperationDetail Emission (CLIENT-Primary Algorithm)

After decoration, each CLIENT span's decoration contains ALL the data needed for a full NodeOperationDetail:

| NodeOperationDetail field | Source |
|---|---|
| sourceNode (service A) | CLIENT span's own serviceName, environment, groupByAttributes |
| targetNode (service B) | decoration.remoteService, remoteEnvironment, remoteGroupByAttributes |
| sourceOperation | decoration.parentServerOperationName (from parent SERVER span) |
| targetOperation | decoration.remoteOperation (from child SERVER span) |

```
// Step 1: CLIENT spans -- primary emission
for each CLIENT span in processingSpans:
    decoration = getClientDecoration(clientSpan.spanId)
    if decoration exists AND remoteService != "unknown":
        sourceNode  = Node("service", clientSpan.environment, clientSpan.serviceName)
        targetNode  = Node("service", decoration.remoteEnvironment, decoration.remoteService)
        sourceOp    = Operation(decoration.parentServerOperationName)   // may be null
        targetOp    = Operation(decoration.remoteOperation)
        emit NodeOperationDetail(sourceNode, targetNode, sourceOp, targetOp)

// Step 2: Leaf SERVER spans -- services with no outgoing calls
for each SERVER span in processingSpans:
    if serverDecoration is null OR serverDecoration.clientDescendants is empty:
        sourceNode = Node("service", serverSpan.environment, serverSpan.serviceName)
        sourceOp   = Operation(serverSpan.operationName)
        emit NodeOperationDetail(sourceNode, null, sourceOp, null)
```

### What Each Span Contributes

```
                                     s1 provides:
                                       sourceOperation = s1.operationName
                                       (via Pass 2 back-annotation)
                                              |
s1 (SERVER, service A, op="GET /api/users")   |
  +-- ... INTERNAL spans ...                  |
        +-- c1 (CLIENT, service A)  <---------+
              |
              |   c1 provides (from itself):
              |     sourceNode = Node(A)
              |
              +-- s2 (SERVER, service B, op="GET /users")
                    |
                    |   s2 provides (via Pass 1 decoration):
                    |     targetNode = Node(B)
                    |     targetOperation = s2.operationName
                    v
              NodeOperationDetail {
                  sourceNode:  A (from c1)
                  targetNode:  B (from s2 via decoration)
                  sourceOp:    "GET /api/users" (from s1 via decoration)
                  targetOp:    "GET /users" (from s2 via decoration)
              }
```

### Edge Cases

| Child SERVER (s2) in any window? | Parent SERVER (s1) in any window? | Result |
|---|---|---|
| No | (irrelevant) | `remoteService = "unknown"` -- no event emitted |
| Yes | No | Event emitted with `nodeConnectionHash` only. `sourceOp = null`, `operationConnectionHash = null` |
| Yes | Yes | Full event with both hashes and both operations |

### Key Properties

- **No duplicates**: Each CLIENT span emits exactly once, each leaf SERVER span emits exactly once
- **Single entity type**: All emissions produce NodeOperationDetail with dual hash fields
- **Dedup at query time**: `GROUP BY nodeConnectionHash` for topology, `GROUP BY operationConnectionHash` for operations
- **Three-window lookup**: Decoration uses spans from all 3 windows (~3x windowDuration coverage)

### Metrics Generation

Metrics are generated alongside NodeOperationDetail events during Phase 2:

```
// Step 1: CLIENT spans
for each CLIENT span in processingSpans:
    if decoration exists AND remoteService != "unknown":
        emit NodeOperationDetail(...)
        if decoration.parentServerOperationName != null:
            generateMetricsForClientSpan(clientSpan, decoration)

// Step 2: SERVER spans
for each SERVER span in processingSpans:
    generateMetricsForServerSpan(serverSpan)         // ALL server spans
    if leaf (no CLIENT descendants):
        emit NodeOperationDetail(sourceNode, null, sourceOp, null)
```

**CLIENT span metrics** include `remoteService`, `remoteOperation`, and `remoteEnvironment` labels. Only generated when the full operation context is available (`parentServerOperationName != null`).

**SERVER span metrics** are generated for ALL SERVER spans regardless of leaf status. They include `service`, `operation`, and `environment` labels.

## Performance Considerations

### Memory Usage

- **Off-heap storage**: Uses MapDB to store span state data outside JVM heap
- **Window size impact**: Larger `window_duration` values require more storage
- **Trace volume**: Memory usage scales with the number of concurrent traces

### Storage Requirements

- **Database path**: Ensure sufficient disk space at the configured `db_path`
- **Cleanup**: Old database files are automatically cleaned up during window rotation
- **I/O performance**: Use fast storage (SSD) for better performance

### Monitoring Metrics

The processor exposes the following metrics for monitoring:

- `spansDbSize`: Total size of span databases in bytes
- `spansDbCount`: Total number of spans stored across all databases
- `dependencyCallsOverflowed` (counter): Dependency calls, including consumed messages, collapsed into `OtherRemoteService` by `max_dependencies_per_service` (not registered when `dependency_nodes.enabled` is false)
- `dependencyRemoteOperationCallsOverflowed` (counter): Calls to an admitted dependency collapsed into `OtherRemoteOperation` by `max_remote_operations_per_service` (not registered when `dependency_nodes.enabled` is false)

## Related Documentation

- [OpenTelemetry Trace Processing](../otel-trace-raw-processor/README.md)
- [Service Map State Management](../service-map-stateful/README.md)
