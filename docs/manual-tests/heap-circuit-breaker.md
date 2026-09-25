# Manual Test: Heap Circuit Breaker

This playbook walks you through bootstrapping a single-node OpenSearch cluster
together with a locally-built Data Prepper instance so you can **manually
verify that the heap circuit breaker opens, rejects requests at the layers
we hardened, and recovers** when load drops.

It exercises:

| Priority | What it protects | Verified here |
|---|---|---|
| P1 | Armeria HTTP decorator rejects requests before any body processing | ✅ |
| P2 | gRPC service rejects before protobuf-to-domain parsing | ✅ |
| P3 | Pipeline worker pauses buffer reads while breaker is open | indirectly |
| P4 | Hysteresis: separate open / close thresholds (`usage` vs `close_usage`) | ✅ |
| P5 | Peer-forwarder receive buffer is wrapped by the breaker | ✅ (optional section) |

> **Why a tiny heap?** We pin Data Prepper to `-Xmx128m` so the breaker is
> reachable in seconds of synthetic load. The goal is to prove the behaviour
> end-to-end, not to benchmark.

---

## Topology

```
┌──────────────┐  OTLP/gRPC  ┌──────────────────┐   HTTP   ┌──────────────┐
│ telemetrygen │ ──────────▶ │   Data Prepper   │ ───────▶ │  OpenSearch  │
│  (load gen)  │   :21890    │    -Xmx128m      │  :9200   │  (Docker)    │
└──────────────┘   :21891    │ /metrics:4900    │          │   :9200      │
                   :21892    └──────────────────┘          └──────────────┘
```

---

## 0. Prerequisites

Install once:

- **Docker** (single-node OpenSearch will run in it).
- **JDK 17** (`java -version`).
- **`telemetrygen`** (OpenTelemetry contrib load generator):
  ```bash
  go install github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen@latest
  # ensures it's on PATH:
  export PATH="$PATH:$(go env GOPATH)/bin"
  telemetrygen --help >/dev/null && echo "telemetrygen OK"
  ```
- `curl`, `jq` (any modern Linux/macOS already has them).

---

## 1. Build Data Prepper from your branch

From the repo root:

```bash
./gradlew :release:archives:linux:assemble
```

This produces a runnable install tree. Capture its location:

```bash
export DP_VERSION=$(grep '^version=' gradle.properties | cut -d= -f2)
export DP_HOME="$PWD/release/archives/linux/build/install/opensearch-data-prepper-${DP_VERSION}-linux-x64"
ls "$DP_HOME"   # should show bin/ config/ lib/ pipelines/ ...
```

(On Apple Silicon / aarch64 Linux replace `linuxx64` with `linuxarm64` and
the `-linux-x64` suffix with `-linux-arm64`.)

---

## 2. Start a single-node OpenSearch cluster

Security is disabled to keep the testbed friction-free. **Do not copy these
flags into anything resembling a real environment.**

```bash
docker run -d --name cb-test-os \
  -p 9200:9200 -p 9600:9600 \
  -e "discovery.type=single-node" \
  -e "DISABLE_SECURITY_PLUGIN=true" \
  -e "OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m" \
  opensearchproject/opensearch:2

# Wait for it to be ready (~15–30s):
until curl -fs http://localhost:9200/_cluster/health >/dev/null; do
  echo "waiting for opensearch..."; sleep 2
done
curl -s http://localhost:9200 | jq '.version.number'
```

---

## 3. Drop in three config files

### 3a. `data-prepper-config.yaml`

```bash
cat > "$DP_HOME/config/data-prepper-config.yaml" <<'YAML'
ssl: false
metric_registries: [Prometheus]

# Heap circuit breaker tuned for the -Xmx128m heap below.
# - usage (80 MB)         ≈ 63% of heap  → trips easily under load
# - close_usage (50 MB)   ≈ 39% of heap  → forces hysteresis (Priority 4)
# - reset (2s)            → minimum dwell once tripped
# - check_interval (500ms)→ matches the upstream default
circuit_breakers:
  heap:
    usage: 80mb
    close_usage: 50mb
    reset: 2s
    check_interval: 500ms
YAML
```

### 3b. `pipelines.yaml` — one pipeline per OTel signal

This exercises Priority 1 / Priority 2 on **all three** OTel gRPC services.

```bash
cat > "$DP_HOME/pipelines/pipelines.yaml" <<'YAML'
traces-pipeline:
  source:
    otel_trace_source:
      ssl: false
  processor:
    - otel_traces:
  sink:
    - opensearch:
        hosts: [ "http://localhost:9200" ]
        insecure: true
        index: otel-traces

logs-pipeline:
  source:
    otel_logs_source:
      ssl: false
  sink:
    - opensearch:
        hosts: [ "http://localhost:9200" ]
        insecure: true
        index: otel-logs

metrics-pipeline:
  source:
    otel_metrics_source:
      ssl: false
  processor:
    - otel_metrics:
  sink:
    - opensearch:
        hosts: [ "http://localhost:9200" ]
        insecure: true
        index: otel-metrics
YAML
```

### 3c. `log4j2-rolling.properties` — surface the breaker logs

> **Critical.** The shipped log4j config has `rootLogger.level = warn`, which
> **hides** the INFO breaker open/close lines and the DEBUG peer-forwarder
> rejection line. Without this override you will think the breaker isn't
> tripping when it actually is.

```bash
cat > "$DP_HOME/config/log4j2-rolling.properties" <<'PROPS'
status = error
dest = err
name = PropertiesConfig

property.filename = log/data-prepper/data-prepper.log

appender.console.type = Console
appender.console.name = STDOUT
appender.console.layout.type = PatternLayout
appender.console.layout.pattern = %d{ISO8601} [%t] %-5p %40C - %m%n

appender.rolling.type = RollingFile
appender.rolling.name = RollingFile
appender.rolling.fileName = ${filename}
appender.rolling.filePattern = logs/data-prepper.log.%d{MM-dd-yy-HH}-%i.gz
appender.rolling.layout.type = PatternLayout
appender.rolling.layout.pattern = %d{ISO8601} [%t] %-5p %40C - %m%n
appender.rolling.policies.type = Policies
appender.rolling.policies.time.type = TimeBasedTriggeringPolicy
appender.rolling.policies.time.interval = 1
appender.rolling.policies.time.modulate = true
appender.rolling.policies.size.type = SizeBasedTriggeringPolicy
appender.rolling.policies.size.size = 100MB
appender.rolling.strategy.type = DefaultRolloverStrategy
appender.rolling.strategy.max = 168

rootLogger.level = warn
rootLogger.appenderRef.stdout.ref = STDOUT
rootLogger.appenderRef.file.ref = RollingFile

logger.pipeline.name = org.opensearch.dataprepper.pipeline
logger.pipeline.level = info
logger.parser.name = org.opensearch.dataprepper.parser
logger.parser.level = info
logger.plugins.name = org.opensearch.dataprepper.plugins
logger.plugins.level = info

# --- Circuit breaker visibility for this playbook ---
# Breaker open/close (INFO). Without this they are swallowed by the root WARN.
logger.breaker.name = org.opensearch.dataprepper.core.breaker
logger.breaker.level = info

# Priority 5 peer-forwarder rejection log line is DEBUG.
logger.peerforwarder.name = org.opensearch.dataprepper.core.peerforwarder
logger.peerforwarder.level = debug

# Priority 2 gRPC pre-parse path raises BufferWriteException("Circuit breaker is open.")
# which surfaces as DEBUG/INFO from the OTel sources depending on plugin code.
logger.oteltrace.name = org.opensearch.dataprepper.plugins.source.oteltrace
logger.oteltrace.level = debug
logger.otellogs.name = org.opensearch.dataprepper.plugins.source.otellogs
logger.otellogs.level = debug
logger.otelmetrics.name = org.opensearch.dataprepper.plugins.source.otelmetrics
logger.otelmetrics.level = debug
PROPS
```

---

## 4. Start Data Prepper (pinned heap)

Run **in the foreground** in its own terminal so you can watch the logs:

```bash
cd "$DP_HOME"
JAVA_OPTS="-Xms128m -Xmx128m" bin/data-prepper
```

You should see, during startup:

```
... INFO  ...HeapCircuitBreaker - Circuit breaker heap open threshold is 83886080 bytes, close threshold is 52428800 bytes.
```

That single line is your proof that the config was loaded with hysteresis
enabled (close_usage < usage). If it shows the same number twice → hysteresis
is **not** configured, recheck `data-prepper-config.yaml`.

---

## 5. Baseline sanity check

In a second terminal — small payload, breaker should stay closed:

```bash
telemetrygen traces \
  --otlp-endpoint localhost:21890 --otlp-insecure \
  --duration 5s --rate 10 --workers 2

sleep 3
curl -s 'http://localhost:9200/otel-traces*/_count' | jq
curl -s http://localhost:4900/metrics/prometheus | \
  grep -E '^core_circuitBreakers_heap_(open|memoryUsage)'
```

Expect:
- `_count > 0` (traces landed in OpenSearch).
- `core_circuitBreakers_heap_open 0.0` (closed).
- No `Circuit breaker tripped` log in the DP terminal.

---

## 6. Trip the breaker

In a third terminal — flood traces at high concurrency:

```bash
telemetrygen traces \
  --otlp-endpoint localhost:21890 --otlp-insecure \
  --workers 100 --rate 5000 --duration 60s
```

Within a few seconds the DP terminal should print, **repeatedly**:

```
... INFO  ...HeapCircuitBreaker - Circuit breaker tripped and open. 91234567 used memory bytes > 83886080 configured
```

> If you don't see it, raise `--rate` or `--workers`, or lower `usage` in the
> config. With `-Xmx128m` the breaker should trip on the first burst.

---

## 7. Observe (three quick checks while load is running)

### A. Prometheus state gauge

In a fourth terminal:

```bash
watch -n 0.5 'curl -s http://localhost:4900/metrics/prometheus \
  | grep -E "^core_circuitBreakers_heap_(open|memoryUsage)"'
```

Expect to see `core_circuitBreakers_heap_open` oscillate to `1.0` and stay
there while the load runs.

### B. Client-side `UNAVAILABLE` (P1 proof)

The Armeria decorator (Priority 1) returns HTTP **503** before any body is
parsed; gRPC surfaces this to the client as `UNAVAILABLE`. `telemetrygen`
prints errors on stderr:

```
... rpc error: code = Unavailable desc = ...
```

Non-zero `Unavailable` count = the HTTP decorator is rejecting **before**
protobuf parsing. That is what saves the 1–4 MB of allocations per request
the problem statement warns about.

### C. Server-side rejection logs (P2 proof)

In the DP terminal, while the breaker is open you should see, sporadically,
the `BufferWriteException("Circuit breaker is open.")` stack snippet coming
from `OTelTraceGrpcService` / `OTelLogsGrpcService` / `OTelMetricsGrpcService`
— this is the Priority 2 defence-in-depth check catching requests that
slipped past the HTTP decorator during the open/close race window.

---

## 8. Verify hysteresis (Priority 4)

Stop the load (Ctrl-C the `telemetrygen` from §6) and watch the DP terminal:

1. Heap usage starts dropping.
2. The breaker **does not** close immediately when usage crosses below 80 MB.
3. It stays open until usage falls below **50 MB** (`close_usage`), then logs:

```
... INFO  ...HeapCircuitBreaker - Circuit breaker closed. 47123456 used memory bytes <= 52428800 configured close threshold
```

That gap between "usage < 80mb" and "Circuit breaker closed" is the
oscillation-prevention band Priority 4 buys you. Without `close_usage`,
the breaker would flap every `check_interval` around the single threshold.

---

## 9. Recovery sanity check

Replay the baseline request from §5 — it should succeed again and the
Prometheus gauge should be back at `0.0`. Pipeline survived a breaker trip
without manual intervention.

---

## 10. Optional: Priority 5 — peer forwarder

Priority 5 wraps the peer-forwarder **receive buffer** with the breaker so a
flood of inbound peer-forwarder traffic cannot bypass it. Exercising this
path requires the HTTP receive service (`PeerForwarderHttpService`), which is
only used when at least **two** nodes are forwarding to each other —
single-node setups use the in-process `LocalPeerForwarder` and never touch
HTTP.

The simplest reproduction is to run two Data Prepper instances on the same
host with `static` peer discovery pointing at each other, and use a
processor that requires peer forwarding (e.g. `aggregate`, `service_map`,
`anomaly_detector`).

### 10a. Second-instance pipelines

Copy `$DP_HOME` to `$DP_HOME_2` and edit:

```bash
cp -r "$DP_HOME" "$DP_HOME_2"
```

In **`$DP_HOME/config/data-prepper-config.yaml`** add:

```yaml
# ...existing circuit_breakers block...
peer_forwarder:
  discovery_mode: static
  static_endpoints:
    - 127.0.0.1
    - 127.0.0.2     # alias for the second instance
  server_port: 21895
  ssl: false
```

Bind the second instance to a different alias / port — easiest is to run it
listening on `127.0.0.2`:

```bash
# Make 127.0.0.2 reachable (Linux):
sudo ip addr add 127.0.0.2/8 dev lo 2>/dev/null || true
```

In `$DP_HOME_2/config/data-prepper-config.yaml`, use the **same** peer
forwarder block but pick a different `server_port` (e.g. `5900`) so the
admin endpoints don't clash. Make the same change to the OTel source ports
and Prometheus port to avoid collisions (or simply skip running source
pipelines on instance 2 and let it act purely as a peer-forwarder receiver).

### 10b. Pipeline that triggers peer forwarding

Add an `aggregate` processor:

```yaml
traces-pipeline:
  source:
    otel_trace_source:
      ssl: false
  processor:
    - otel_traces:
    - aggregate:
        identification_keys: ["traceId"]
        action:
          remove_duplicates:
  sink:
    - opensearch:
        hosts: [ "http://localhost:9200" ]
        insecure: true
        index: otel-traces
```

### 10c. Run both instances and re-flood

Start instance 2 first (so it can receive forwards), then instance 1:

```bash
cd "$DP_HOME_2" && JAVA_OPTS="-Xms128m -Xmx128m" bin/data-prepper &
cd "$DP_HOME"   && JAVA_OPTS="-Xms128m -Xmx128m" bin/data-prepper
```

Re-run the §6 `telemetrygen` flood against instance 1's `:21890`. While the
breaker is open on instance 2 (the receiver) you will see, in **instance 2**'s
log:

```
... DEBUG ...PeerForwarderHttpService - Rejecting peer forwarder request: circuit breaker is open.
```

That log line — added in this change — is your proof for Priority 5. The
inbound peer-forwarder POST returns HTTP **503** to instance 1 before any
deserialization happens.

> If you don't want to set up a second instance, the unit test
> `PeerForwarderHttpServiceTest#doPost_circuitBreakerOpen_rejectsRequestBeforeParsing`
> covers the same code path deterministically.

---

## 11. Cleanup

```bash
# Stop Data Prepper(s) (Ctrl-C each foreground process), then:
docker rm -f cb-test-os
unset DP_HOME DP_HOME_2 DP_VERSION
# Optional, if you added the loopback alias for §10:
sudo ip addr del 127.0.0.2/8 dev lo 2>/dev/null || true
```

---

## Assertion matrix (what to check off)

| Verifying | Signal | Where | Pass criterion |
|---|---|---|---|
| Breaker opens | `Circuit breaker tripped and open ...` | DP stdout (INFO) | appears under §6 load |
| State observable | `core_circuitBreakers_heap_open` gauge | `http://localhost:4900/metrics/prometheus` | flips to `1.0` |
| Heap reported | `core_circuitBreakers_heap_memoryUsage` gauge | same | climbs past `8.388608e+07` (80 MB) |
| **P1** HTTP decorator | gRPC `UNAVAILABLE` / HTTP 503 | `telemetrygen` stderr | non-zero count |
| **P2** gRPC pre-parse | `BufferWriteException: Circuit breaker is open.` in OTel service | DP stdout (DEBUG) | appears while breaker is open |
| **P4** hysteresis | gap between heap dropping below 80 MB and `Circuit breaker closed` | DP stdout (INFO) | close threshold is 50 MB, not 80 MB |
| **P5** peer fwd | `Rejecting peer forwarder request: circuit breaker is open.` | DP stdout (DEBUG) | appears in the §10 variant |
| Recovery | `Circuit breaker closed ...` after load stops | DP stdout (INFO) | appears |

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| No `Circuit breaker tripped` log | Root logger is `warn` and you didn't write §3c. | Recreate `log4j2-rolling.properties` per §3c and restart DP. |
| `core_circuitBreakers_heap_open` never appears in Prometheus | `circuit_breakers` block missing from `data-prepper-config.yaml`. | Recreate §3a. |
| Breaker won't trip even at `--rate 10000` | Heap not actually constrained (env not picked up). | Confirm `jps -v` shows `-Xmx128m` for `DataPrepperExecute`; some shells eat `JAVA_OPTS`. |
| `telemetrygen` errors out immediately with `connection refused` | OTel source not bound yet. | Wait for `Pipeline [traces-pipeline] - Submitting request to initiate` style log; retry. |
| OpenSearch sink errors with `Couldn't connect to "http://localhost:9200"` | Container not up or security still on. | `curl http://localhost:9200` should return JSON; re-run §2. |
| Breaker closes immediately as soon as load stops | `close_usage` not honoured (config typo). | Re-check `data-prepper-config.yaml` — the startup log must show two **different** byte counts. |
| `Circuit breaker tripped` appears even at idle | `usage` too low for baseline footprint. | Raise to e.g. `100mb`; or raise heap to `-Xmx192m`. |

---

## Why this playbook looks the way it does

- **Foreground processes, not docker-compose.** A one-shot compose stack
  would hide the very logs we're trying to read. Manual debugging benefits
  from explicit terminals.
- **Three OTel sources in one pipeline file.** Priorities 1 and 2 live in
  *each* gRPC service (`OTelTrace/Logs/Metrics`), so we float a tiny
  pipeline per signal instead of trusting one to generalise.
- **`MemoryMXBean` lag, as called out in the problem statement.** Expect a
  few hundred ms of slop between the moment the heap actually exceeds 80 MB
  and the `tripped and open` log — that's why `check_interval` is 500 ms.
- **gRPC clients retry on `UNAVAILABLE` with backoff.** A clean
  `telemetrygen` run does **not** prove the breaker stayed closed; always
  cross-check the server-side log and the Prometheus gauge.

