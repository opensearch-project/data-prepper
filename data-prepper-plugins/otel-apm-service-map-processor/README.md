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

### Advanced Configuration

```yaml
processor:
  - otel_apm_service_map:
      window_duration: 120s  # 2-minute windows for high-latency services
      db_path: "/tmp/apm-service-map/"
      group_by_attributes:
        - "service.version"
        - "deployment.environment"
        - "service.namespace"
        - "k8s.cluster.name"
```

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
| `latency_seconds` | Histogram | `s` | Request latency distribution |

**Histogram Bucket Boundaries:**
`[0.0, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0]`

All metrics use **delta aggregation temporality** (values are cumulative within each window only).

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

## Related Documentation

- [OpenTelemetry Trace Processing](../otel-trace-raw-processor/README.md)
- [Service Map State Management](../service-map-stateful/README.md)
