# Spec: Per-Event Templated `additional_headers` for the OTLP Sink

## Summary

Allow the **values** of `additional_headers` in the OTLP sink to be **templates
resolved per event** from event content. The primary motivation is routing X-Ray
spans to **per-resource CloudWatch log groups / streams** via the
`x-aws-log-group` / `x-aws-log-stream` headers that AWS X-Ray (CloudWatch
Transaction Search) honours — without pre-creating anything and without one sink
instance per destination.

Example:

```yaml
sink:
  - otlp:
      endpoint: "https://xray.us-east-1.amazonaws.com/v1/traces"
      service_name: "xray"
      aws:
        region: "us-east-1"
      additional_headers:
        x-aws-log-group:  "/atp/e2e/${/resource/service.name}"
        x-aws-log-stream: "${/resource/host.id}"
```

For a span whose event has `resource.service.name = checkout` and
`resource.host.id = i-0abc`, this sends that span to log group
`/atp/e2e/checkout`, stream `i-0abc`. Spans for other services/hosts in the same
pipeline go to their own groups/streams.

### Why this is the right mechanism for traces

X-Ray has **no per-resource container to create** (unlike the CloudWatch Logs
sink, where "dynamic" means creating named log groups/streams). A trace is just
data POSTed to the regional X-Ray OTLP endpoint; with Transaction Search enabled,
X-Ray persists spans as CloudWatch Logs records, and the `x-aws-log-group` /
`x-aws-log-stream` request headers override the default `/aws/spans` destination.
So "dynamic destination for traces" reduces to **templating these header values
per event** — confirmed working today as *static* headers (see Background).

## Status of the prerequisite (confirmed)

A **static** header config has been verified end-to-end against AWS X-Ray:

```yaml
additional_headers:
  x-aws-log-group: "/atp/e2e/otlp-test"
  x-aws-log-stream: "default"
```

This proves two things the design relies on:

1. `x-aws-log-group` / `x-aws-log-stream` are accepted **unsigned** — the sink
   adds `additional_headers` *after* SigV4 signing
   (`OtlpHttpSender.buildHttpRequest:188`), so no `SigV4Signer` change is needed.
2. Both group and stream are independently controllable, and the protected-header
   denylist (`OtlpSinkConfig.isAdditionalHeadersValid`) permits `x-aws-*` (it
   blocks `authorization`, `host`, `content-*`, and `x-amz-*`).

## GitHub Issue

[Support per-event templated additional_headers in the OTLP sink for X-Ray
log-group/stream routing] (to be filed; link before opening the PR).

## Goals / Non-goals

**Goals**

1. `additional_headers` **values** may contain `${/field}` / `${expression}`
   templates resolved per event via `Event.formatString`.
2. Fully backward compatible: a config with no templated value (no `${`) behaves
   byte-for-byte as today (single batch, single send, evaluator never invoked).
3. Within each signal buffer, batches are **partitioned by the resolved header
   tuple** so every OTLP POST carries exactly one, correct header set.
4. No new silent-loss path: every enqueued event ends in exactly one terminal
   state (delivered, dropped-with-metric, or rejected) — the OTLP sink has **no
   DLQ**, so this matters.
5. Header injection and invalid X-Ray names are rejected, not forwarded.

**Non-goals (this PR)**

* Templating header **names** (keys stay static — preserves the denylist).
* Templating `endpoint` (stays static; SigV4 region/WebClient are bound once).
  Fail fast if `endpoint` contains `${`.
* Signed dynamic headers (all `additional_headers` are added unsigned, as today).
* gRPC, or any change to the trace/metric/log encoding.

## Branch Strategy

* Feature branch: `feature/otlp-sink-templated-additional-headers`
* Base: `main`; single PR targeting `main`; commits signed with DCO (`-s`).

---

## Background: current architecture (verified)

* **`OtlpSink.doOutput(records)`** — for each record,
  `OtlpSignalType.fromEvent(event)` picks one of three `OtlpSinkBuffer`s
  (`EnumMap` TRACE/METRIC/LOG); `buffer.add(record)` does a blocking
  `queue.put` (backpressure = lossless under normal operation).
* **`OtlpSinkBuffer.runTyped()`** — a **single-threaded** worker per signal.
  Polls the `LinkedBlockingQueue`, calls `handler.encodeEvent(event)` → protobuf
  `T`, accumulates **one** `List<Pair<T, EventHandle>> batch` with a running
  `batchSize` and a single `lastFlush`. Flushes when
  `batch.size() >= maxEvents` **or** `batchSize >= maxBatchBytes` **or**
  `now - lastFlush >= flushTimeoutMillis`. Encode failure / null →
  `event.getEventHandle().release(false)`. On `stop()` → a final flush of the
  single `batch`. A worker crash triggers `restartWorker()` with a fresh
  (method-local) batch.
* **`OtlpHttpSender`** — **one shared instance** across all three buffers (one
  `WebClient`, one `SigV4Signer`, built once from static config).
  `send(batch, handler, signalType)` builds the protobuf payload, gzips,
  `buildHttpRequest` signs (only `Content-Type` / `Content-Encoding` + SDK
  auth/host/date — `SigV4Signer.signRequest`), copies the signed headers, then
  **adds the static `additionalHeaders` map after signing** (`buildHttpRequest:188`,
  `headersBuilder.add`). Async `webClient.execute`; on success `handleResponse`
  parses OTLP partial-success; on failure `releaseAllEventHandle(false)`.
* **`OtlpSinkConfig`** — `endpoint` (`@NotBlank` static), `additional_headers`
  (`Map<String,String>`, static), `service_name`, `aws` (`@AssertTrue
  awsConfig != null`), `max_retries`. `isAdditionalHeadersValid()` denylist:
  `authorization, host, content-type, content-encoding, content-length`, prefix
  `x-amz-`. **No `ExpressionEvaluator` is injected anywhere.**
* **No DLQ.** README "Known Limitations": *"No support for DQL-based loss-less
  delivery in this release."* So resolution failures cannot be DLQ'd — they must
  be dropped (with a metric) or fall back to a configured default.

### Reference APIs (verified)

* `Event#formatString(String, ExpressionEvaluator, String defaultValue)` —
  `JacksonEvent.formatStringInternal:544`. Walks `${...}` placeholders; per
  placeholder tries `get(name)`, else (if a valid expression) evaluates it, else
  substitutes `defaultValue`, else **throws `EventKeyNotFoundException`** when
  `defaultValue == null`. A malformed template (no closing `}`) throws
  `RuntimeException`. **Note:** the default substitutes only when the value is
  `null` (missing key), *not* when the field is present but empty (`""`).
* `ExpressionEvaluator` — `isValidFormatExpression(String)`,
  `isValidExpressionStatement(String)`, `evaluate(String, Event)`. Injected into
  sinks via `@DataPrepperPluginConstructor` (as in `S3Sink`, `OpenSearchSink`).
* X-Ray / CloudWatch name rules (the values must satisfy): log group regex
  `[.\-_/#A-Za-z0-9]{1,512}`; log stream ≤ 512, no `:` or `*`.

---

## Configuration

| Key | Type | Default | Notes |
|---|---|---|---|
| `additional_headers` | `Map<String,String>` | `{}` | Values may now contain `${...}` templates. Names stay static. |
| `header_template_on_failure` | enum `drop` \| `default` | `drop` | Per-event policy when a templated value can't resolve or is invalid. `drop` = release-as-failure + metric (no DLQ exists). `default` = use the per-header default below. |
| `additional_headers_defaults` | `Map<String,String>` | `{}` | Literal fallback value per header name, used when policy is `default`. Must itself be valid (CWL rules for `x-aws-log-*`). |
| `max_open_header_partitions` | int | `256` (Min 1, Max 4096) | Cap on simultaneously open `(headerTuple)` partitions **per signal buffer**. On breach, force-flush oldest/largest before admitting a new tuple. |

`threshold` (`max_events`, `max_batch_size`, `flush_timeout`) keeps its meaning
but is applied **per partition** for flush triggering, with a global ceiling (see
Edge case #7/#8).

A header value is **templated** iff `value != null && value.contains("${")`.
If **no** value is templated → the legacy single-batch path runs unchanged.

---

## Resolution & partitioning model

For each event, **inside the worker loop at the `encodeEvent` point** (never at
enqueue — the `Event` must be in hand and consistent with the encoded bytes):

1. **Resolve** each templated header value via
   `event.formatString(template, expressionEvaluator, defaultForThisHeaderOrNull)`,
   wrapped in a per-event `try/catch`. Static values pass through unchanged.
2. **Validate/sanitize** every resolved value (and static ones too): reject
   CR/LF/NUL/control chars (injection); for `x-aws-log-group`/`x-aws-log-stream`
   enforce the CWL charset + length and reject empty/blank/whitespace and
   non-ASCII. On failure → apply `header_template_on_failure` (drop or default).
3. **Build the resolved header map** for the event: templated values + static
   values, single-value-per-name (`put`/last-wins, not `add`).
4. **Partition key** = an order-independent canonical tuple of **all per-event
   templated header name→value pairs** (group AND stream AND any other templated
   header). Static headers are excluded from the key but merged into every
   partition's sent map. Route the encoded `Pair<T,EventHandle>` into that
   partition.

Each partition (`PartitionState`) owns its own `batch`, running `batchSize`, and
`lastFlush`. The worker flushes per partition by size/count/time. The shared
`OtlpHttpSender.send(...)` is changed to take the **per-batch resolved header
map** as a parameter (no mutable shared header state).

---

## Implementation Plan (TDD)

> Tests first. JUnit 5 + Hamcrest + Mockito. Keep changes additive where
> possible (overloads) so existing tests/constructors keep compiling.

### Step 1 — Inject `ExpressionEvaluator`; config + construction-time validation

**Files:** `OtlpSink.java`, `configuration/OtlpSinkConfig.java`

* Add `ExpressionEvaluator` as a `@DataPrepperPluginConstructor` parameter on
  `OtlpSink` (pattern: `S3Sink`, `OpenSearchSink`). Tolerate `null` only when no
  header is templated; require non-null otherwise.
* Add the new config keys above. Keep the `isAdditionalHeadersValid` name
  denylist as-is (names stay static).
* **Fail fast** in the constructor: for each templated value call
  `expressionEvaluator.isValidFormatExpression(value)`; if `endpoint.contains("${")`
  throw with "endpoint does not support templating". Throw
  `InvalidPluginConfigurationException` naming the offending header.
* Precompute, once: which header names are templated, the static base map, and
  the per-header default map.

Tests: templated vs static detection; invalid template → startup failure;
`endpoint` template → startup failure; evaluator-null tolerated for static config.

### Step 2 — Header resolver

**File (new):** `http/HeaderResolver.java` (+ an immutable `HeaderTuple` value
type with order-independent `equals`/`hashCode`, e.g. backed by a sorted map).

* `ResolvedHeaders resolve(Event event)` →
  `{ Map<String,String> sendHeaders, HeaderTuple partitionKey }` or a
  `ResolutionFailure` (when policy `drop`).
* Implements steps 1–4 of the model: `formatString` per templated value,
  validate/sanitize, apply failure policy, build the send map, compute the key.
* Static-only config → returns a constant `sendHeaders` and an empty/constant
  key (single partition).

Tests: `${/field}` substitution; expression substitution; missing field →
default (policy `default`) / failure (policy `drop`); malformed template caught;
CRLF/NUL rejected; CWL-invalid group/stream rejected; empty/blank rejected;
non-ASCII rejected; non-scalar field (`toString`) rejected; key is
order-independent and includes all templated headers.

### Step 3 — Partition the worker loop

**File:** `buffer/OtlpSinkBuffer.java`

* Replace the single `batch`/`batchSize`/`lastFlush` with
  `Map<HeaderTuple, PartitionState>` (each: list, byte size, `lastFlush`
  initialized at partition creation).
* At the `encodeEvent` point, call `HeaderResolver.resolve(event)`; on failure,
  apply policy (drop → `release(false)` + metric, never reaches a partition);
  else append the encoded `Pair` to its partition.
* Flush per partition by `max_events` / `max_batch_size` / `flush_timeout`
  (per-partition timer). Enforce `max_open_header_partitions` + a global byte
  ceiling: force-flush oldest/largest before admitting a new tuple. Amortize the
  time-scan to once per poll tick; flush the just-touched partition inline on
  size.
* **Final flush on `stop()` iterates ALL partitions** (and drains partitions
  created while `running == false` but the queue is non-empty); the
  `InterruptedException`/`shutdownNow` path must not break before the
  all-partition drain.
* Resolution/encode failures handled locally so nothing escapes to
  `restartWorker()`; before any restart-triggering throwable, release all open
  partition handles `false`.

Tests: two tuples → two sends with correct headers each; per-partition timer (no
starvation); cap eviction; full-map final flush; poison event dropped + worker
survives; conservation (`#in == #released`).

### Step 4 — Make the sender header-stateless

**File:** `http/OtlpHttpSender.java`

* Change `send(batch, handler, signalType)` →
  `send(batch, handler, signalType, Map<String,String> resolvedHeaders)` (keep an
  overload delegating with the static map for backward compatibility).
  `buildHttpRequest(compressedPayload, resolvedHeaders)` adds the **parameter**
  map after signing (still `put`/single-value), never a mutable instance field.
* No other behavior change; `SigV4Signer` is already per-call stateless.

Tests: interleaved concurrent sends with distinct maps → no cross-contamination;
single-value (no duplicate header lines); static path unchanged.

### Step 5 — Metrics

**File:** `metrics/OtlpSinkMetrics.java`

Add **fixed, untagged** counters/gauges (never tag with resolved values):
`headerResolutionFailedCount`, `headerResolutionDroppedCount`,
`invalidHeaderValueCount`, `openHeaderPartitions` (gauge),
`distinctTuplesPerFlush` (summary). Maintain accounting conservation.

### Step 6 — README + docs

Document: templated `additional_headers`, the failure policy, the
unsigned-header + trusted-field security note, the cardinality guidance
(low-cardinality routing keys only), the static-name/`endpoint`-static
boundaries, and the new metrics. Add a documentation-website issue.

### Step 7 — Verify

```bash
./gradlew :data-prepper-plugins:otlp-sink:test
./gradlew :data-prepper-plugins:otlp-sink:build
./gradlew spotlessApply
```

---

## Files Changed

| File | Action | Description |
|---|---|---|
| `OtlpSink.java` | Modified | Inject `ExpressionEvaluator`; construction-time template/endpoint validation; pass resolver down. |
| `configuration/OtlpSinkConfig.java` | Modified | New keys (`header_template_on_failure`, `additional_headers_defaults`, `max_open_header_partitions`); keep name denylist; reject templated `endpoint`. |
| `http/HeaderResolver.java` + `HeaderTuple` | New | Per-event resolution, validation/sanitization, failure policy, partition key. |
| `buffer/OtlpSinkBuffer.java` | Modified | `Map<HeaderTuple,PartitionState>`; per-partition size/byte/timer flush; cap + global ceiling; full-map final flush; local failure handling. |
| `http/OtlpHttpSender.java` | Modified | `send(...)` takes per-batch resolved headers; header-stateless; single-value put. |
| `metrics/OtlpSinkMetrics.java` | Modified | New untagged counters/gauges. |
| `README.md` | Modified | Templating, failure policy, security/cardinality notes, boundaries, metrics. |
| Corresponding `*Test.java` | Modified/New | Per step. |

---

## Edge case catalog

> Severity: **critical** = data loss / security / crash; **high** = misrouting /
> hard-to-diagnose; **medium/low** = inefficiency or operability.

### Key design decisions forced by the edge cases

- **Resolve templated values inside the single-threaded worker, co-located with
  `encodeEvent` while the `Event` is in hand — never at enqueue.** Enqueue-time
  resolution changes the queue element type and risks the `Event` being recycled
  before encode, producing a tuple that disagrees with the encoded bytes.
- **Replace the worker's single `batch`/`batchSize`/`lastFlush` with
  `Map<HeaderTuple, PartitionState>`.** A shared batch carries one header set per
  POST → would silently misroute every tuple but one.
- **Partition key = full, order-independent tuple of ALL per-event templated
  header values** (group AND stream AND any other). Static headers excluded from
  the key but merged into every partition's sent map.
- **`OtlpHttpSender.send` takes a per-batch resolved-header map; zero mutable
  shared header state.** The sender is shared across TRACE/METRIC/LOG threads and
  `webClient.execute` is async — mutable shared headers race and cross-contaminate
  signals.
- **Detect templating per entry (`value.contains("${")`); no-template config
  takes the exact current path** (single send, evaluator never called) — 100%
  backward compatible with the confirmed-working static X-Ray config.
- **Validate template syntax and the name denylist at construction; fail fast**
  with `InvalidPluginConfigurationException`. A malformed `${` would otherwise
  fail 100% of events at runtime.
- **Inject `ExpressionEvaluator` via `@DataPrepperPluginConstructor`; tolerate
  null only for static configs.** Do not construct one inside the sink.
- **Header NAMES stay static; only VALUES are templated** — preserves the
  denylist; a templated value can't introduce a forbidden name.
- **Validate/sanitize EVERY resolved value (incl. static):** reject
  CR/LF/NUL/control chars, enforce CWL charset/length for `x-aws-log-*`, reject
  empty/blank/whitespace and non-ASCII. Fail-closed to default/drop.
- **Missing-field / unresolvable policy is explicit and configurable**, default
  `drop` (+metric) over silent `/aws/spans` misrouting; never hardcode
  `/aws/spans`. Prefer a per-header `defaultValue` so `formatString` doesn't
  throw.
- **Per-event resolution is isolated in try/catch and never throws out of the
  worker** — an escaping exception triggers `restartWorker()` with a fresh empty
  map, dropping in-flight partitions without releasing handles (loss + ack leak +
  poison-event crash loop).
- **`max_events`/`max_batch_size` per partition for flush, plus a global ceiling
  (`max_open_header_partitions` + total bytes).** Total memory ≈ partitions ×
  per-partition size; cap and force-flush oldest/largest.
- **Per-partition `lastFlush`, initialized at partition creation; amortized
  deadline-ordered scan.** A shared timer starves low-cardinality partitions.
- **Final flush on `stop()` drains ALL partitions; the interrupt path must not
  abort first.**
- **`EventHandle`s stay strictly per partition; each `send()` releases its own
  slice exactly once; failed events release once and are excluded from
  partitions.**
- **Templated headers are UNSIGNED — document and scope to trusted,
  low-cardinality routing fields; requires ARCC/security review.** Templating
  signed headers or `endpoint` is a non-goal / fail-fast.
- **Never tag metrics with resolved header values; conservation: every event ends
  in exactly one terminal counter.**

### 1. Batch partitioning, worker-loop state, flush semantics

| # | Scenario | Risk | Handling | Sev |
|---|---|---|---|---|
| 1 | Where resolution happens: enqueue vs worker loop. `encodeEvent` already runs in the worker; the `Event` is the only place field values live. | Enqueue resolution changes the queue element type / runs on the processor thread; splitting from encode risks the `Event` being recycled → tuple disagrees with encoded bytes (wrong group for the span sent). | Resolve at the `encodeEvent` point while the `Event` is held; route the encoded `Pair` into the partition. Never at enqueue. | high |
| 2 | Partition key built from only one templated header (or static headers dropped from the key). | Same group, different stream collapse into one partition → one POST → second stream's events misrouted to the wrong CloudWatch stream; whichever event flushes decides everyone's header. | Key = full resolved tuple of ALL per-event templated headers, order-independent (`equals`/`hashCode`). Static headers excluded from key but merged into every partition's send map. | critical |
| 3 | Worker keeps single `batch`/`batchSize`/`lastFlush`, swaps header map at send. | Shared batch mixes tuples; flush attaches one header set → all but one tuple wrong. The core failure the feature must prevent. | `Map<HeaderTuple, PartitionState>` each with its own list/size/`lastFlush`; append per tuple; flush per partition. | critical |
| 4 | Flush-by-time uses one shared `lastFlush`. | Hot partition resets it every flush → low-volume partition never times out, events sit until shutdown (starvation, unbounded latency); also over-eager flush of hot partitions. | Per-partition `lastFlush`, initialized at partition creation; flush any whose own elapsed ≥ timeout or size/bytes met. | high |
| 5 | Per-partition timers force scanning every partition each iteration. | O(partitions) per event/tick → CPU + delayed polling on the single worker. | Amortize the time-scan to once per poll tick (or min-heap by deadline); always check the just-touched partition's size inline. | medium |
| 6 | Many partitions simultaneously eligible to flush. | Map-order flush starves cold partitions; N async sends at once spike in-flight requests + signing (throttling). Cross-partition order not preserved. | Flush oldest-`lastFlush` first; optionally bound concurrent in-flight sends; document cross-partition ordering not preserved (within-partition preserved by single-threaded append). | medium |
| 7 | Per-partition vs global `max_events`/`max_batch_size`; `safeContentLimit = maxBatchSize*(retries+1)`. | Per-partition limits make total memory ≈ partitions × `maxBatchBytes` (exceeds the operator's mental model); global-only relies on the timer; a partition could exceed endpoint payload limit. | Apply limits per partition for flush (each POST within limits); chunk a partition that still exceeds the cap; enforce a separate global byte/partition ceiling; surface a gauge. | medium |

### 2. Cardinality blow-up, fan-out, signing cost

| # | Scenario | Risk | Handling | Sev |
|---|---|---|---|---|
| 8 | High-cardinality templated field (`${traceId}`, per-request id, per-host stream) → ~one tuple per event. | Many concurrent open batches in one worker → memory/OOM; tiny single-event POSTs → X-Ray throttling (429) + amplified per-flush signing. Defeats batching. | `max_open_header_partitions` cap with force-flush of oldest/largest (LRU) before admitting a new tuple; document low-cardinality routing keys only; open-partition gauge + WARN; global byte budget. | high |
| 9 | `SigV4Signer.signRequest` resolves creds + signs on every call; K sub-batches sign K times. | Signing/credential resolution becomes a hot path on the single worker; `resolveCredentials` may hit STS, adding latency → backpressure. | Confirm the credentials provider caches (AWS providers do); keep K low via cardinality guardrails; optional signing-latency metric. | medium |

### 3. EventHandle lifecycle, worker crash, shutdown

| # | Scenario | Risk | Handling | Sev |
|---|---|---|---|---|
| 10 | `stop()` final flush drains only the single `batch`; `stop()` calls `shutdownNow()`. | Draining one partition drops all others on shutdown without success release → loss + ack-set leaks (sources wedge/replay); `InterruptedException` may abort mid-drain. | Final flush iterates ALL partitions (and late-created ones); the interrupt path must not break before the all-partition drain. | critical |
| 11 | Worker throws → `restartWorker()` with a method-local (now empty) map; or `InterruptedException` mid-iteration. | Restart discards all accumulated partitions without releasing handles (`false`) → loss + ack leak; a poison event that always fails resolution → crash loop. | Handle resolution/encode failures locally so nothing escapes; before any restart-triggering throwable, release all partition handles `false`. Poison-event test: dropped + counted, worker survives. | high |
| 12 | Handles released per batch; with K sub-batches each `send` carries its slice; failures also release. | Global handle tracking releases handles for the wrong partition's outcome (ack corruption); double-release (dropped + partitioned) or never-release (ack leak → source stall). | Handles strictly per partition; each `send()` releases its slice (closure over `immutableBatch`); failed events release once and are excluded. Invariant: `release(true)+release(false) == #events`, no double-release. | high |

### 4. Header value validation, injection, CWL naming

| # | Scenario | Risk | Handling | Sev |
|---|---|---|---|---|
| 13 | Resolved value has CR/LF/NUL/control chars (`${attributes.user_input}` = `/svc/foo\r\nX-Injected: foo`). `formatString` doesn't sanitize; added verbatim, unsigned. | **Header/request splitting/injection** outside the signature; one poisoned span corrupts every flush carrying it; Armeria may reject (batch loss) or smuggle headers. | MANDATORY: reject any value with CR/LF/NUL/control (<0x20, 0x7F) — including static. On violation, don't add the header; fall back/drop per policy; `invalidHeaderValueCount`; WARN with header NAME only. Tests: `\r`, `\n`, `\r\n`, NUL. | critical |
| 14 | Resolved `x-aws-log-group` violates `[.\-_/#A-Za-z0-9]{1,512}` (`:`, space, `*`, non-ASCII, >512), or stream has `:`/`*` or >512. | One POST = one header set; X-Ray rejects the ENTIRE POST (all spans lost); a bad event-derived value repeatedly poisons every batch to it. Current path: non-2xx → `releaseAllEventHandle(false)`. | Validate against CWL rules for the `x-aws-log-*` headers before partitioning/sending; on invalid, fall back to configured default or drop per policy; metric + sampled WARN. X-Ray remains the authority. | critical |
| 15 | Resolved value empty/blank (field present but `""` — default applies only to null — or whitespace). | Empty `x-aws-log-*` is invalid → X-Ray rejects the POST; blank-but-present is easy to miss (header IS added); collapses routes into one empty-keyed partition. | Treat empty/blank for `x-aws-log-*` as a resolution failure (policy); never send empty. For general headers, omit rather than send empty. Tests: empty, whitespace-only. | high |
| 16 | Non-string field value (List/Map/number/boolean/nested) → `formatString` calls `val.toString()`. | `toString` has spaces/brackets/commas/`=` → illegal CWL name → garbage routing / batch loss. | Validate the final string against target rules; reject/fallback. Document: template scalar string fields only. Tests for list/map/number/boolean. | medium |
| 17 | Non-ASCII / multibyte value (CJK/accented service name). | HTTP header values are ~ISO-8859-1/ASCII; Armeria may reject/mangle (batch loss); CWL regex is ASCII-only → X-Ray rejects. | Validate ASCII (printable minus controls; CWL regex for `x-aws-log-*`); reject/fallback; never silently URL-encode/transliterate (mis-routes). Document ASCII-only. | medium |
| 18 | Leading/trailing/internal whitespace/tabs (`"  /svc/foo  "`). | HTTP may trim / obs-fold (silent value change → mis-route); spaces make CWL invalid → rejection. | Don't silently trim routing headers; reject whitespace for `x-aws-log-*` and fall back. Tests for whitespace variants. | low/medium |
| 19 | Very long value (>512 for CWL, KBs for general). | >512 → CWL rejection; oversized may exceed Armeria/endpoint header limit (batch loss); a huge injected value is a minor DoS. | Enforce max length (512 for `x-aws-log-*`; a few KB cap for general); reject/fallback + metric. | low |
| 20 | `buildHttpRequest` uses `headersBuilder.add` (append). | Static + templated value for the same name (or duplicate) → multi-valued header X-Ray rejects/misreads → batch loss. | Use `put`/`set` (single-value, last-wins); the resolved single-value-per-name map is the source of truth. Re-confirm no additional-header name duplicates a signed header. | medium |

### 5. Resolution failure, fallback policy, template semantics

| # | Scenario | Risk | Handling | Sev |
|---|---|---|---|---|
| 21 | Missing field, no default → `EventKeyNotFoundException`; malformed `${` → `RuntimeException`. Mixed-schema events fail intermittently. | If unhandled, propagates → worker crash (restart drops in-flight partitions = loss + ack leak) or inconsistent skips. The existing try/catch wraps only `encodeEvent`. | Wrap resolution in per-event try/catch like `encodeEvent`; on failure use a per-header default (so it never throws) or route to failure policy; `release(false)` + metric; never propagate. | high/critical |
| 22 | Fallback policy when unresolvable (drop / default / `/aws/spans`). No DLQ. | `/aws/spans` → silent misrouting; drop → true loss (no DLQ); undefined → throw → crash. | Explicit configurable policy, default safest: per-header default into `formatString` so resolution is deterministic; else drop + metric + WARN. Never hardcode `/aws/spans`. | high |
| 23 | One event's resolution throws while siblings succeed. | A single-pass resolve that aborts on first exception fails the entire flush (all signals' events lost) — disproportionate blast radius. | Per-event isolated try/catch; failing event handled per policy and excluded; others proceed. Test mixed batch. | medium |
| 24 | `formatString` precedence: literal key first; if null + valid expression, evaluate as a DP expression. Slashes/dots ambiguous. | Operator intends a field ref but it's read as an expression (or vice versa) → wrong/empty value → misrouting/drop; subtle in prod. | Document precedence (literal key → expression → default/throw) with X-Ray examples; validate at startup; ship tested known-good configs. | medium |
| 25 | `formatString` called per event per templated header. | CPU on the single worker → lower throughput, higher flush latency, deeper queue → backpressure. | Resolve each templated value once per event into a small map reused for key + send headers; optionally cache parsed templates; benchmark; resolution-latency metric. | low |

### 6. SigV4 signing and trust boundary

| # | Scenario | Risk | Handling | Sev |
|---|---|---|---|---|
| 26 | Shared `OtlpHttpSender` (`additionalHeaders` a final field) used by 3 signal threads; naive impl mutates the field per batch. | **Thread-safety/cross-signal corruption:** async `webClient.execute` callbacks read a header set changed under an in-flight request → a LOG request sent with a TRACE partition's headers. Intermittent. | `send(...)` is header-stateless: take `Map<String,String>` param (`Map.copyOf`), build request from it; no setter, no mutable field. Concurrency test: interleaved sends with distinct maps, no contamination. | critical |
| 27 | `x-aws-log-*` added after signing (unsigned); values now from event data, not trusted static config. | Untrusted span content controls CloudWatch routing via UNSIGNED headers, not bound to the payload; with CRLF this is a routing-integrity / cross-tenant surface (steer data into another tenant's group if the role authorizes broadly). | Document headers are UNSIGNED; scope templating to TRUSTED fields; strict CWL-charset validation; confirm the X-Ray role scopes to the intended log-group namespace. ARCC/security review before implementation. | high |
| 28 | User templates a header the target requires SIGNED (general feature). | Unsigned header ignored/rejected, or a signature-validating service rejects the batch. Works for `x-aws-*`, mysteriously fails otherwise. | Document all `additional_headers` are unsigned; restrict templated names to a known-safe set (`x-aws-*`) or warn non-`x-aws-*` is unsupported. Signed dynamic headers = non-goal. | high |
| 29 | PII/secrets in span attributes pulled into headers (unsigned, possibly logged by proxies) and into DP's own ERROR/WARN logs (`handleResponse` logs response bodies). | PII exposure to intermediaries/access logs; new validation/error code could log header values with PII. | Never log resolved header VALUES (name + reason only); document that templating exposes event data into request metadata; advise against sensitive fields; align with ARCC/BSC PII guidance. | medium |

### 7. Config, validation, DI, backward compatibility

| # | Scenario | Risk | Handling | Sev |
|---|---|---|---|---|
| 30 | Existing purely-static `additional_headers` (the confirmed-working X-Ray config). | Routing every config through `formatString`/partitioning changes behavior for all users: extra CPU, possible NPE if evaluator null, single-send → N-send — breaks known-good production ingestion. | Detect templating per entry (`value.contains("${")`). No templated entry → EXACT current path (static map once, no evaluator, single send). Test: `verifyNoInteractions(evaluator)` + one batch to sender for static config. | critical |
| 31 | `ExpressionEvaluator` must be injected; today the constructor wires only (creds, config, metrics, setting). | Unsupplied/null param fails plugin instantiation (outage) or latent NPE on first templated event; direct-construction tests break. | Add `ExpressionEvaluator` as a `@DataPrepperPluginConstructor` param; require non-null only when a header is templated; tolerate null otherwise; update call sites + `@VisibleForTesting` ctors via overloads. Don't construct one inside the sink. | critical |
| 32 | Template syntax validated only at runtime (`"${/aws/spans"` missing brace → `RuntimeException` per event). | A typo fails 100% of that signal's events — total loss until fixed, maybe found only at runtime; NPE if `isValidFormatExpression` called with a null evaluator. | Validate each templated value at construction via `isValidFormatExpression`; throw `InvalidPluginConfigurationException` naming the header before starting buffers; keep per-event try/catch as defense-in-depth. | high |
| 33 | Mixed static + templated headers; a literal value with a stray `${` (`"price_${"`). | All-or-nothing detection breaks the mixed case or misparses a literal `${`; a previously-literal `${` value now throws. | Per-entry detection: only `${`-containing values go through `formatString` (in the key); others pass through literally (resolved once, merged, excluded from key). Document any literal `${` is treated as a template (no escape today). | medium |
| 34 | Header KEY handling: `isAdditionalHeadersValid` lowercases + denies `authorization`/`host`/`content-*`/`x-amz-*`; design templates only VALUES. | Safe (a value can't change a NAME) but must be preserved; if keys ever become templatable the denylist is bypassable at runtime. The only value-side bypass is CRLF (covered). | Keep keys static, out of scope; keep the `@AssertTrue` denylist; test that templating applies to values only; ensure CRLF sanitization runs on every resolved value incl. static. | low/medium |
| 35 | Someone templates `endpoint: "${...}"`. | `endpoint` is static — builds one WebClient + derives region/SigV4 at construction; per-event endpoint breaks signing, region parsing, the shared client → silent 403/misrouting. | Keep `endpoint` static, out of scope; fail fast at validation if it contains `${` ("endpoint does not support templating"); document the boundary. | medium |
| 36 | Back-compat of `OtlpHttpSender` `@VisibleForTesting` constructors and existing tests (`Map.of()`). | Changing `send()`/ctor signatures breaks existing test constructors / external callers; could silently change the no-header default path. | Add overloads rather than changing signatures; keep `Map.of()` behavior identical; run the full module asserting no diff for static/no-header cases. | low |

### 8. Metrics and accounting

| # | Scenario | Risk | Handling | Sev |
|---|---|---|---|---|
| 37 | Temptation to tag failure/rejection metrics with resolved log-group/stream for observability. | High-cardinality user-controlled tags explode metric cardinality → registry OOM, CloudWatch/Prometheus cost spike — an outage vector. | No metric tagged with resolved values; small fixed set of new untagged counters/gauges (mirroring the pre-registered pattern). Test asserting no per-value tags. | high |
| 38 | Accounting of dropped/failed-resolution events vs `recordsOut`/`failed`/`rejected`. | `recordsIn != recordsOut + failed + rejected` hides silent loss (dangerous, no DLQ); double-counting if dropped pre-send but send also counts. | One terminal counter per event: a resolution drop increments exactly one new counter (+ generic failed), releases the handle `false` once, never reaches `send()`. Conservation test. | medium |

---

## Acceptance Criteria

1. **Backward compatible:** a config with no templated header value produces the
   exact current behavior (single batch/send, evaluator never invoked) — golden
   test with `verifyNoInteractions(evaluator)`.
2. Templated header values resolve per event via `${...}` (fields + expressions),
   resolved at the `encodeEvent` point on the worker thread.
3. Batches are partitioned by the full, order-independent resolved header tuple;
   every OTLP POST carries exactly one correct header set.
4. The shared `OtlpHttpSender` is header-stateless; concurrent sends from
   different signal buffers never cross-contaminate headers.
5. Invalid templates and a templated `endpoint` fail fast at construction with
   `InvalidPluginConfigurationException`.
6. Every resolved value (incl. static) is validated: CR/LF/NUL/control rejected;
   `x-aws-log-*` checked against CWL charset/length; empty/blank/non-ASCII
   rejected. Invalid → fallback (policy `default`) or drop (policy `drop`) — never
   a malformed/empty header, never aborts the batch.
7. No silent loss: per batch `#in == #delivered + #dropped + #rejected`; handles
   release exactly once with the correct value; resolution drops are counted.
8. `stop()` drains ALL partitions; a worker crash/restart does not drop in-flight
   partitions without releasing handles; a poison event is dropped and the worker
   survives.
9. `max_open_header_partitions` + global byte ceiling bound memory; exceeding
   triggers force-flush of oldest/largest, with an open-partition gauge.
10. No metric is tagged with a resolved header value.
11. License headers on new files; commits signed (DCO); README + doc issue filed.

## Risks & Notes

- **Security (unsigned, untrusted):** templated `additional_headers` are added
  unsigned and now carry untrusted event data that controls CloudWatch routing.
  CRLF sanitization is mandatory; templating must be scoped to trusted,
  low-cardinality fields; the X-Ray IAM role should scope to the intended
  log-group namespace. Requires ARCC/security review before implementation.
- **No DLQ:** unlike the CWL sink, the OTLP sink has no DLQ, so resolution
  failures are drop-or-fallback only. Default to drop+metric over silent
  `/aws/spans` misrouting.
- **Cardinality is the dominant operational risk:** safe for resource/service/
  region-style keys, dangerous for ids/trace-ids. Caps + gauge make the worst
  case survivable but can't make an unbounded template good; document guidance.
- **Scope of the X-Ray header semantics is AWS-owned:** the exact
  `x-aws-log-group` / `x-aws-log-stream` contract (and whether a separate stream
  header exists long-term) should be confirmed against current AWS docs; the
  feature treats X-Ray as the authority and validates client-side only against
  known CWL name rules.
- **`endpoint` and header names remain static** by design; revisit only with a
  separate proposal.
