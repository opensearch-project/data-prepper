# NodeOperationDetail Generation Algorithm

## Overview

This document explains the full processor lifecycle and how `NodeOperationDetail` events are generated from OpenTelemetry trace spans in the `otel_apm_service_map` processor.

## OTel Trace Structure

For any cross-service call, OpenTelemetry produces this span pattern:

```
Service A: SERVER span (s1, op="GET /api/users")
  └── Service A: INTERNAL span (i1)              [optional, 0 or more]
        └── Service A: CLIENT span (c1)
              └── Service B: SERVER span (s2, op="GET /users")
```

- **SERVER span**: handles an incoming request to the service
- **INTERNAL span**: intermediate processing within the service (middleware, business logic)
- **CLIENT span**: makes an outgoing call to another service
- Parent-child links are via `spanId` / `parentSpanId`

## Processor Lifecycle

### How doExecute works

The pipeline framework calls `doExecute(records)` repeatedly — each time a batch of span records arrives, or periodically with empty batches if no data is flowing. There is no timer; window processing is **triggered by doExecute calls**.

The order within each `doExecute` call matters:

```
doExecute(records):
    // STEP 1: Check window FIRST (before storing new spans)
    if windowDurationHasPassed():
        apmEvents = evaluateApmEvents()    // Phase 1 (decorate) + Phase 2 (emit) + rotate
    else:
        apmEvents = EMPTY

    // STEP 2: Store incoming spans AFTER (always into nextWindow)
    for each span in records:
        spanData = processSpan(span)       // raw extraction only — NO decoration
        nextWindow.put(traceId, spanData)

    return apmEvents
```

### What processSpan Does (and Doesn't Do)

`processSpan` is a **pure data extraction step**. It takes a raw OTel `Span` object and converts it into a `SpanStateData` record for storage. There is **no decoration, no relationship resolution, no event emission** in this step.

```
processSpan(span) → SpanStateData:
    Extract from span:
        serviceName      = span.getServiceName()
        spanId           = span.getSpanId()
        parentSpanId     = span.getParentSpanId()   // null if empty
        traceId          = span.getTraceId()
        spanKind         = span.getKind()            // "SPAN_KIND_SERVER", "SPAN_KIND_CLIENT", etc.
        spanName         = span.getName()
        operationName    = span.getName()
        durationInNanos  = span.getDurationInNanos()
        status           = extractSpanStatus(span)   // "OK", "ERROR", etc.
        endTime          = span.getEndTime()         // ISO timestamp string
        groupByAttrs     = extractGroupByAttributes(span)   // from resource attributes
        spanAttributes   = extractSpanAttributes(span)      // HTTP status, resource, scope attrs

    return new SpanStateData(serviceName, spanId, parentSpanId, traceId,
                             spanKind, spanName, operationName, durationInNanos,
                             status, endTime, groupByAttrs, spanAttributes)
```

The resulting `SpanStateData` is a flat, serializable snapshot — it knows nothing about other spans, parent-child relationships, or remote services. All relationship resolution happens later in Phase 1 (decoration), which runs inside `evaluateApmEvents()`.

**Critical ordering**: Step 1 happens BEFORE Step 2. This means:
- The window check/process/rotate completes before new spans are stored
- New spans from THIS batch go into the **post-rotation** nextWindow
- They are NOT included in the window that was just processed
- `processSpan` (Step 2) never triggers decoration or emission — it only stores raw data

### Three-Window Architecture

Spans are stored across three MapDB-backed time windows:

```
|  Previous Window  |  Current Window   |  Next Window    |
|  (old spans for   |  (being processed |  (incoming spans |
|   lookup context) |   this cycle)     |   accumulating)  |
```

- **nextWindow**: where ALL incoming spans are written (every `doExecute` call, Step 2)
- **currentWindow**: processed when `windowDuration` elapses (Step 1)
- **previousWindow**: kept for lookup context (helps complete traces that span windows)

### Window Rotation

When `windowDurationHasPassed()` is true, `evaluateApmEvents()` fires:

```
windowDurationHasPassed():
    elapsed = now - previousTimestamp
    return elapsed >= windowDuration          // default windowDuration = 60s

evaluateApmEvents():
    barrier.await()              // sync all processor threads
    if isMasterInstance():
        apmEvents = processCurrentWindowSpans()   // Phase 1 + Phase 2
        rotateWindows()                            // slot-machine rotation
    barrier.await()              // sync again
    return apmEvents

rotateWindows():
    temp = previousWindow
    previousWindow = currentWindow    // just-processed window becomes context
    currentWindow  = nextWindow       // accumulated spans become next to process
    nextWindow     = temp             // reuse old previous (cleared)
    nextWindow.clear()
    previousTimestamp = now            // reset the timer
```

### Detailed Timeline (windowDuration = 60s)

```
t=0s: Processor starts
      previousTimestamp = t=0
      prev=[], curr=[], next=[]

─── doExecute calls during t=0..60s ───

t=5s: doExecute([spanA, spanB])
      Step 1: windowDurationHasPassed()? elapsed=5s < 60s → NO
      Step 2: spanA, spanB → nextWindow
      State: prev=[], curr=[], next=[A,B]

t=30s: doExecute([spanC])
      Step 1: elapsed=30s < 60s → NO
      Step 2: spanC → nextWindow
      State: prev=[], curr=[], next=[A,B,C]

─── First window boundary ───

t=65s: doExecute([spanD])
      Step 1: elapsed=65s >= 60s → YES → evaluateApmEvents()
              processCurrentWindowSpans() on curr=[] → nothing (empty)
              rotateWindows():
                prev = []  (was curr)
                curr = [A,B,C]  (was next)
                next = []  (cleared)
              previousTimestamp = t=65
      Step 2: spanD → nextWindow (now empty)
      State: prev=[], curr=[A,B,C], next=[D]
      Events emitted: none (currentWindow was empty)

─── doExecute calls during t=65..125s ───

t=90s: doExecute([spanE])
      Step 1: elapsed=25s < 60s → NO
      Step 2: spanE → nextWindow
      State: prev=[], curr=[A,B,C], next=[D,E]

─── Second window boundary ───

t=130s: doExecute([spanF])
      Step 1: elapsed=65s >= 60s → YES → evaluateApmEvents()
              processCurrentWindowSpans() on curr=[A,B,C]:
                lookupSpans = prev[] + curr[A,B,C] + next[D,E]
                processingSpans = curr[A,B,C]
                Phase 1: Decorate spans
                Phase 2: Emit NodeOperationDetail for spans A,B,C
              rotateWindows():
                prev = [A,B,C]  (was curr, now context)
                curr = [D,E]  (was next, will be processed next)
                next = []  (cleared)
              previousTimestamp = t=130
      Step 2: spanF → nextWindow
      State: prev=[A,B,C], curr=[D,E], next=[F]
      Events emitted: NodeOperationDetail events from spans A,B,C ← FIRST REAL OUTPUT

─── Third window boundary ───

t=195s: doExecute([spanG])
      Step 1: elapsed=65s >= 60s → YES → evaluateApmEvents()
              processCurrentWindowSpans() on curr=[D,E]:
                lookupSpans = prev[A,B,C] + curr[D,E] + next[F]
                processingSpans = curr[D,E]
                Phase 1: Decorate D,E (can find relationships in A,B,C from prev)
                Phase 2: Emit NodeOperationDetail for spans D,E
              rotateWindows():
                prev = [D,E]
                curr = [F]
                next = []
              previousTimestamp = t=195
      Step 2: spanG → nextWindow
      State: prev=[D,E], curr=[F], next=[G]
      Events emitted: NodeOperationDetail events from spans D,E
```

### Key Timing Observations

1. **Spans are processed TWO cycles after arrival** at startup:
   - Cycle 1 (t=65): Spans [A,B,C] are in nextWindow. Rotation moves them to currentWindow. But processCurrentWindowSpans ran on the OLD empty currentWindow.
   - Cycle 2 (t=130): Spans [A,B,C] are now in currentWindow. processCurrentWindowSpans processes them.
   - After startup, it's effectively one cycle delay (next → current → processed in the same rotation).

2. **No timer — driven by doExecute calls**: If no spans arrive and the pipeline stops calling doExecute, windows won't rotate. The pipeline framework ensures periodic calls.

3. **New spans never pollute the current processing**: Because Step 1 (check + process + rotate) happens BEFORE Step 2 (store spans), the batch being received never interferes with the window being processed.

4. **Lookup spans include the future**: When processing currentWindow, lookupSpans includes nextWindow too. This means spans that arrived AFTER the current window's spans can still help complete trace relationships (e.g., a child SERVER span arriving later helps decorate the parent CLIENT span).

### Processing a Window: processCurrentWindowSpans()

This is the **only place** where decoration and emission happen. It runs inside `evaluateApmEvents()`, which only fires when the window duration has elapsed. Neither `processSpan` nor any other code path triggers decoration.

```
processCurrentWindowSpans():
    ephemeralDecorations = new EphemeralSpanDecorations()  // fresh per cycle
    metricsState = new HashMap<MetricKey, MetricAggregationState>()

    // Build lookup maps from ALL 3 windows
    previousSpansByTraceId = buildSpansByTraceIdMap(previousWindow)
    currentSpansByTraceId  = buildSpansByTraceIdMap(currentWindow)
    nextSpansByTraceId     = buildSpansByTraceIdMap(nextWindow)

    // Process each trace that has spans in currentWindow
    for each traceId in currentSpansByTraceId:
        traceData = buildThreeWindowTraceData(traceId, previous, current, next)
        // traceData.lookupSpans     = spans from ALL 3 windows for this trace
        // traceData.processingSpans = spans from CURRENT window only for this trace

        // ─── PHASE 1: DECORATION (relationships resolved here, not in processSpan) ───
        decorateSpansInTraceWithEphemeralStorage(traceData)
            Pass 1: decorateClientSpansFirstPass(traceData)     // CLIENT → child SERVER
            Pass 2: decorateServerSpansSecondPass(traceData)    // SERVER → CLIENT descendants + back-annotate

        // ─── PHASE 2: EMISSION + METRICS ───
        generateNodeOperationDetailEvents(traceData, currentTime, metricsState)
            Step 1: CLIENT spans → NodeOperationDetail events + client metrics
            Step 2: SERVER spans → server metrics (all) + leaf NodeOperationDetail (leaf only)

    // Create metric events from aggregated state
    metrics = createMetricsFromAggregatedState(metricsState)
    metrics.sort(by time)

    return metrics + apmEvents    // metrics first, then NodeOperationDetail events
```

Important: a trace is only processed if it has at least one span in `currentWindow`. But for decoration, spans from all 3 windows are used as lookup context.

### Where Each Concern Lives

| Concern | Where | When |
|---|---|---|
| Raw data extraction | `processSpan()` | Every `doExecute` call (Step 2) |
| Relationship resolution | `decorateSpansInTraceWithEphemeralStorage()` | Only during window processing (Phase 1) |
| NodeOperationDetail emission | `generateNodeOperationDetailEvents()` Step 1 + Step 2 | Only during window processing (Phase 2) |
| Metric aggregation | `generateNodeOperationDetailEvents()` Step 1 + Step 2 | Only during window processing (Phase 2) |
| Metric event creation | `createMetricsFromAggregatedState()` | After all traces processed |

## Phase 1: Span Decoration (Two Passes)

Decoration runs on `lookupSpans` (all 3 windows) to build relationships.

### Pass 1: Decorate CLIENT spans

For each CLIENT span in **all 3 windows**, find its direct child SERVER span to learn the remote service:

```
c1 (CLIENT, service A)
  └── child s2 (SERVER, service B)

ClientSpanDecoration(c1) = {
    parentServerOperationName: null,      ← not yet known
    remoteService: "B",                   ← from s2.serviceName
    remoteOperation: "GET /users",        ← from s2.operationName
    remoteEnvironment: s2.environment,
    remoteGroupByAttributes: s2.groupByAttributes
}
```

If no child SERVER span exists (e.g., calling a database with no OTel instrumentation):
- `remoteService = "unknown"`
- `remoteOperation = "unknown"`

### Pass 2: Decorate SERVER spans + back-annotate CLIENT spans

For each SERVER span in **all 3 windows**:
1. Find CLIENT descendants from the same service via BFS (walks through INTERNAL spans)
2. Back-annotate those CLIENT spans with the parent SERVER operation name

```
s1 (SERVER, service A, op="GET /api/users")
  └── ... INTERNAL spans ...
        └── c1 (CLIENT, service A)

BFS from s1 finds c1 (same service, CLIENT kind)

ClientSpanDecoration(c1) UPDATED = {
    parentServerOperationName: "GET /api/users",   ← NOW FILLED from s1
    remoteService: "B",                             ← unchanged from Pass 1
    remoteOperation: "GET /users",                  ← unchanged
    remoteEnvironment: ...,
    remoteGroupByAttributes: ...
}
```

### BFS Rules for Finding CLIENT Descendants

The BFS in `findClientDescendantsForServerThreeWindow`:
- Starts from the SERVER span
- Walks down through children, grandchildren, etc. (multi-level)
- **Continues** as long as the child is from the **same service**
- **Stops** (prunes the branch) when the service name changes
- **Collects** CLIENT spans found along the way

Example:
```
s1 (SERVER, service=A)
  └── i1 (INTERNAL, service=A)      ← same service, keep going
        └── i2 (INTERNAL, service=A) ← same service, keep going
              ├── c1 (CLIENT, service=A) ← COLLECTED
              └── c2 (CLIENT, service=A) ← COLLECTED
                    └── s2 (SERVER, service=B) ← different service, STOP

Result: clientDescendants = [c1, c2]
```

## What a CLIENT Span Needs for Emission (Critical)

A CLIENT span `c1` is emitted from `processingSpans` (currentWindow). But decoration looks across
**all 3 windows** to resolve relationships. Two other spans provide context:

- **s2** (child SERVER span of `c1`) — provides `targetNode` and `targetOperation`
- **s1** (parent SERVER span of `c1`, same service, found via BFS) — provides `sourceOperation`

Neither `s1` nor `s2` needs to be in the same window as `c1`. They just need to exist somewhere
in the 3-window lookup range:

```
|  previousWindow       |  currentWindow        |  nextWindow           |
|                       |    c1 is HERE          |                       |
|                       |    (being processed)   |                       |
|  s1 can be here  ✓    |  s1 can be here  ✓     |  s1 can be here  ✓    |
|  s2 can be here  ✓    |  s2 can be here  ✓     |  s2 can be here  ✓    |
```

This gives a lookup range of roughly `3 × windowDuration` (default 3 × 60s = 180s).

### Three Possible Outcomes for a CLIENT Span

| s2 (child SERVER) in any window? | s1 (parent SERVER) in any window? | Result |
|---|---|---|
| **No** | (irrelevant) | `remoteService = "unknown"` → c1 filtered out → **no event** |
| **Yes** | **No** | Event emitted with `nodeConnectionHash` only. `sourceOp = null`, `operationConnectionHash = null` |
| **Yes** | **Yes** | **Full event** with both hashes and both operations |

### What Each Span Contributes

```
                                     s1 provides:
                                       sourceOperation = s1.operationName
                                       (via Phase 1 Pass 2 back-annotation)
                                              │
s1 (SERVER, service A, op="GET /api/users")   │
  └── ... INTERNAL spans ...                   │
        └── c1 (CLIENT, service A)  ◄──────────┘
              │
              │   c1 provides (from itself):
              │     sourceNode = Node(A, c1.environment, c1.serviceName)
              │
              └── s2 (SERVER, service B, op="GET /users")
                    │
                    │   s2 provides (via Phase 1 Pass 1 decoration):
                    │     targetNode = Node(B, s2.environment, s2.serviceName)
                    │     targetOperation = s2.operationName
                    │
                    ▼
              NodeOperationDetail {
                  sourceNode:  A (from c1)
                  targetNode:  B (from s2 via decoration)
                  sourceOp:    "GET /api/users" (from s1 via decoration)
                  targetOp:    "GET /users" (from s2 via decoration)
              }
```

### Why Missing Spans Still Produce Useful Output

- **s1 missing**: The edge A→B is still emitted with `nodeConnectionHash` set. Downstream can
  `GROUP BY nodeConnectionHash` to show the topology. The operation detail is incomplete, but
  the connection between services is captured. This commonly happens when s1 arrived in a window
  that already rotated out of the 3-window range.

- **s2 missing**: No event at all. Without knowing the target service, the edge has no meaning.
  This typically means s2 hasn't arrived yet, or the remote service has no OTel instrumentation.

## Phase 2: NodeOperationDetail Emission

After decoration, each CLIENT span's decoration contains ALL the data needed for a full NodeOperationDetail:

| NodeOperationDetail field | Source |
|---|---|
| sourceNode (service A) | CLIENT span's own serviceName, environment, groupByAttributes |
| targetNode (service B) | decoration.remoteService, remoteEnvironment, remoteGroupByAttributes |
| sourceOperation | decoration.parentServerOperationName (back-annotated from parent SERVER span) |
| targetOperation | decoration.remoteOperation (learned from child SERVER span in Pass 1) |

### Algorithm

Emission only iterates spans in `processingSpans` (current window).

```
// Step 1: CLIENT spans — primary emission (has all data via decoration)
for each CLIENT span in processingSpans:
    decoration = getClientDecoration(clientSpan.spanId)
    if decoration exists AND remoteService != "unknown":
        sourceNode  = Node("service", clientSpan.environment, clientSpan.serviceName, clientSpan.groupByAttributes)
        targetNode  = Node("service", decoration.remoteEnvironment, decoration.remoteService, decoration.remoteGroupByAttributes)
        sourceOp    = Operation(decoration.parentServerOperationName)    // may be null if s1 missing from all 3 windows
        targetOp    = Operation(decoration.remoteOperation)
        emit NodeOperationDetail(sourceNode, targetNode, sourceOp, targetOp, timestamp)

// Step 2: Leaf SERVER spans — services with no outgoing calls
for each SERVER span in processingSpans:
    serverDecoration = getServerDecoration(serverSpan.spanId)
    if serverDecoration is null OR serverDecoration.clientDescendants is empty:
        sourceNode = Node("service", serverSpan.environment, serverSpan.serviceName, serverSpan.groupByAttributes)
        sourceOp   = Operation(serverSpan.operationName)
        emit NodeOperationDetail(sourceNode, null, sourceOp, null, timestamp)
```

## Why This Works — All Scenarios

### Scenario 1: All spans in current window (common case)

```
Current window: [s1(SERVER,A), c1(CLIENT,A), s2(SERVER,B)]
```

- Decoration: c1 gets remoteService=B (from s2) and parentServerOp=s1.op (from s1)
- Step 1: c1 in processingSpans → emits full NodeOperationDetail(A→B, with ops)
- Step 2: s1 has CLIENT descendants → not a leaf → skipped. s2 has no CLIENT descendants → emits leaf(B→null)
- **Result: 1 edge event (A→B) + 1 leaf event (B)**

### Scenario 2: SERVER span (s1) in previous window, CLIENT (c1) in current

```
Previous window: [s1]
Current window:  [c1, s2]
```

- Decoration uses all 3 windows: c1 is back-annotated with s1.operationName (s1 found in lookup)
- Step 1: c1 in processingSpans → decoration has parentServerOp from s1 → emits full NodeOperationDetail(A→B)
- Step 2: s1 not in processingSpans → not iterated. s2 leaf → emits leaf(B→null)
- **Result: 1 edge (A→B, full detail) + 1 leaf (B). No data loss.**

### Scenario 3: CLIENT span (c1) in previous window, SERVER (s1) in current

```
Previous window: [c1]
Current window:  [s1, s2]
```

- Step 1: c1 NOT in processingSpans → not iterated → A→B edge not emitted from Step 1
- Step 2: s1 has CLIENT descendant c1 (found in lookup from previous window) → s1 is NOT a leaf → skipped
- **Result: A→B edge missed in this cycle. It was emitted in the previous cycle when c1 was in the processing window.**

### Scenario 4: Target SERVER span (s2) missing entirely

```
Current window: [s1, c1]    (s2 never arrived or in a far-away window)
```

- Pass 1 decoration: c1 has no child SERVER span → remoteService="unknown"
- Step 1: c1 filtered out (remoteService == "unknown") → nothing emitted
- Step 2: s1 has CLIENT descendant c1 → not a leaf → skipped
- **Result: edge not emitted (target unknown). Same behavior as before.**

### Scenario 5: Leaf service (no outgoing calls)

```
Current window: [s3(SERVER,C)]    (service C, no CLIENT descendants)
```

- Step 1: no CLIENT spans for C → nothing
- Step 2: s3's `ServerSpanDecoration.clientDescendants` is empty (Phase 1 Pass 2 BFS found no CLIENT
  spans from service C). This makes s3 a **leaf** — a SERVER span whose service never makes outgoing calls.
  Emits NodeOperationDetail(C→null, sourceOp only)
- **Result: leaf node captured. A leaf is determined by Phase 1 Pass 2: if BFS from a SERVER span
  finds zero CLIENT descendants from the same service across all 3 windows, it's a leaf.**

### Scenario 6: parentServerOperationName is null (s1 missing from all 3 windows)

This happens when the parent SERVER span (s1) is missing from ALL 3 windows — it either arrived
too early (rotated out beyond previousWindow) or never arrived. The CLIENT span c1 is in
currentWindow but s1 is nowhere in the lookup range. (See "What a CLIENT Span Needs" above.)

```
Previous window: []       (s1 already rotated out)
Current window:  [c1, s2]
Next window:     []
```

- Pass 1: c1 gets remoteService=B from s2 (s2 IS in lookup) ✓
- Pass 2: No SERVER span from service A found in any window → c1 never back-annotated → parentServerOperationName stays null
- Step 1: c1's decoration has parentServerOperationName=null, but remoteService is known (from s2)
- Emits NodeOperationDetail with sourceOp=null, targetOp=Operation(s2.operationName)
- **Result: edge A→B emitted without sourceOperation. operationConnectionHash is null, nodeConnectionHash is set.**

## Key Properties

- **No duplicates**: each CLIENT span emits exactly once, each leaf SERVER span emits exactly once
- **No safety net needed**: CLIENT span decoration already contains parentServerOperationName from Phase 1
- **Single entity type**: all emissions produce NodeOperationDetail with dual hash fields
- **Dedup at query time**: `GROUP BY nodeConnectionHash` for topology, `GROUP BY operationConnectionHash` for operation detail

## Metrics Generation (alongside NodeOperationDetail)

Metrics are generated **inside the same `generateNodeOperationDetailEvents()` method** as NodeOperationDetail events, during Phase 2. They are aggregated into a shared `metricsStateByKey` map across all traces in the window, then converted into metric events after all traces are processed.

### When Metrics Are Generated

```
generateNodeOperationDetailEvents(traceData, currentTime, metricsState):

    // Step 1: CLIENT spans
    for each CLIENT span in processingSpans:
        if decoration exists AND remoteService != "unknown":
            emit NodeOperationDetail(...)
            if decoration.parentServerOperationName != null:           // ← gated on full operation context
                generateMetricsForClientSpan(clientSpan, decoration)   // CLIENT-side metrics

    // Step 2: SERVER spans
    for each SERVER span in processingSpans:
        generateMetricsForServerSpan(serverSpan)                       // ← ALWAYS, not just leaf nodes
        if leaf (no CLIENT descendants):
            emit NodeOperationDetail(sourceNode, null, sourceOp, null)
```

### Metric Types

| Metric | Source Span | Condition | What It Measures |
|---|---|---|---|
| `request` (throughput) | SERVER | Always | Requests received by the service |
| `latency_seconds` | SERVER | Always | Response time of the service |
| `error` | SERVER | Always (rate derived from status) | Error rate of the service |
| `fault` | SERVER | Always (rate derived from HTTP 5xx) | Fault rate of the service |
| CLIENT-side metrics | CLIENT | Only when `parentServerOperationName != null` | Edge-level metrics for the connection |

### No Impact from the Algorithm Change

The new CLIENT-primary algorithm does **not** change metric generation:

- **Server metrics**: Generated for ALL SERVER spans in processingSpans, regardless of whether the span is a leaf or has CLIENT descendants. This is identical to the old algorithm.
- **Client metrics**: Generated only when `parentServerOperationName != null` (i.e., the CLIENT span's parent SERVER span was found in one of the 3 windows). This condition is identical to the old algorithm.

The only thing that changed is **NodeOperationDetail event emission** — CLIENT spans now emit with full operations instead of null operations, and the SERVER path only emits for leaf nodes instead of iterating all CLIENT descendants.
