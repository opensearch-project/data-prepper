# Manual Test: Heap Circuit Breaker

This playbook walks you through bootstrapping a single-node OpenSearch cluster
together with a locally-built Data Prepper instance so you can **manually
verify that the heap circuit breaker opens, rejects requests at the layers
we hardened, and recovers** when load drops.

It exercises:

| ID | What it protects | Verified here |
|---|---|---|
| P1 | Armeria HTTP decorator rejects requests before any body processing | ✅ (§7B) |
| P2 | gRPC service rejects before protobuf-to-domain parsing | ✅ (§7C, not hit in every run) |
| P3 | Hysteresis: separate open / close thresholds (`usage` vs `close_usage`) | ✅ (§8) |
| P4 | Peer-forwarder server rejects inbound batches before reading the body | ✅ (§10, needs three nodes) |

> **Why a small heap?** We pin Data Prepper to `-Xmx256m` so the breaker is
> reachable in seconds of synthetic load. The goal is to prove the behaviour
> end-to-end, not to benchmark. Do not go lower: with three OTel pipelines
> and OpenSearch sinks the idle heap already sits around 90–190 MB, and at
> `-Xmx128m` the breaker trips without any load and Data Prepper can die with
> `OutOfMemoryError`.

> **Automated runner.** `docs/manual-tests/heap-circuit-breaker.sh` runs
> every check of this playbook unattended, with Data Prepper and OpenSearch in
> Docker containers:
>
> ```bash
> ./docs/manual-tests/heap-circuit-breaker.sh                                                   # build first, then trace source, one node
> ./docs/manual-tests/heap-circuit-breaker.sh --skip-build                                      # trace source, one node
> ./docs/manual-tests/heap-circuit-breaker.sh --skip-build --source logs                        # logs source
> ./docs/manual-tests/heap-circuit-breaker.sh --skip-build --source metrics                     # metrics source
> ./docs/manual-tests/heap-circuit-breaker.sh --skip-build --tls                                # OTel source and admin server over TLS
> ./docs/manual-tests/heap-circuit-breaker.sh --skip-build --nodes 3                            # peer forwarder (§10), scenario isolated
> ./docs/manual-tests/heap-circuit-breaker.sh --skip-build --nodes 3 --scenario isolated --tls  # peer forwarder over TLS
> ./docs/manual-tests/heap-circuit-breaker.sh --skip-build --nodes 3 --scenario load-wave       # peer forwarder, scenario load-wave
> ./docs/manual-tests/heap-circuit-breaker.sh --skip-build --keep-running                       # leave the containers running
> ./docs/manual-tests/heap-circuit-breaker.sh --cleanup                                         # remove containers and network, then exit
> ./docs/manual-tests/heap-circuit-breaker.sh --help                                            # all flags and tunables (env vars)
> ```
>
> `--tls` runs the OTel source, the peer forwarder and the admin server over
> TLS with a generated test CA (OpenSearch stays plain HTTP). The manual steps
> below show what the runner does.

---

## Topology

```
┌──────────────┐  OTLP/gRPC  ┌──────────────────┐   HTTP   ┌──────────────┐
│ telemetrygen │ ──────────> │   Data Prepper   │ ───────> │  OpenSearch  │
│  (load gen)  │   :21890    │    -Xmx256m      │  :9200   │  (Docker)    │
└──────────────┘   :21891    │ /metrics:4900    │          │   :9200      │
                   :21892    └──────────────────┘          └──────────────┘
```

---

## 0. Prerequisites

Install once:

- **Docker** (single-node OpenSearch will run in it).
- **A JDK supported by Data Prepper**, see the
  [developer guide](../developer_guide.md#java-versions) for building and the
  [Data Prepper documentation](https://docs.opensearch.org/latest/data-prepper/getting-started/)
  for running. `bin/data-prepper` runs whatever `java` is first on `PATH`, so
  put a supported JDK first on `PATH` for both the build and the run. The
  automated runner needs no local JDK to run Data Prepper; it uses a Docker
  image.
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

(The task builds both architectures. On Apple Silicon / aarch64 Linux use the
`-linux-arm64` directory instead of `-linux-x64`.)

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

# Heap circuit breaker tuned for the -Xmx256m heap below.
# - usage (200 MB)        ≈ 78% of heap  → trips under load, not at idle
# - close_usage (150 MB)  ≈ 59% of heap  → forces hysteresis (P3)
# - reset (2s)            → minimum dwell once tripped
# - check_interval (500ms)→ matches the upstream default
circuit_breakers:
  heap:
    usage: 200mb
    close_usage: 150mb
    reset: 2s
    check_interval: 500ms
YAML
```

### 3b. `pipelines.yaml` — one pipeline per OTel signal

This exercises P1 on **all three** OTel sources and P2 on the logs and
metrics sources (`otlp_traces` has no P2 check).

```bash
cat > "$DP_HOME/pipelines/pipelines.yaml" <<'YAML'
traces-pipeline:
  source:
    otlp_traces:
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
    otlp_logs:
      ssl: false
  sink:
    - opensearch:
        hosts: [ "http://localhost:9200" ]
        insecure: true
        index: otel-logs

metrics-pipeline:
  source:
    otlp_metrics:
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

# P4 peer-forwarder rejection log line is DEBUG.
logger.peerforwarder.name = org.opensearch.dataprepper.core.peerforwarder
logger.peerforwarder.level = debug

# P2 does not log. The ERROR / WARN lines of the OTel sources that §7C uses to
# tell it apart from the buffer check are visible at the root level already.
PROPS
```

---

## 4. Start Data Prepper (pinned heap)

Run **in the foreground** in its own terminal so you can watch the logs:

```bash
cd "$DP_HOME"
JAVA_OPTS="-Xms256m -Xmx256m" bin/data-prepper
```

You should see, during startup:

```
... INFO  ...HeapCircuitBreaker - Circuit breaker heap open threshold is 200.0 MiB (209715200 bytes), close threshold is 150.0 MiB (157286400 bytes).
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

# The OpenSearch sink flushes after bulk_size or its flush timeout (~60s).
sleep 60
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
... INFO  ...HeapCircuitBreaker - Circuit breaker tripped and open. 224.4 MiB (235250680 bytes) used > 200.0 MiB (209715200 bytes) configured
```

> If you don't see it, raise `--rate` or `--workers`, or lower `usage` in the
> config. With `-Xmx256m` the breaker should trip within the first few seconds.

---

## 7. Observe (three quick checks while load is running)

### A. Prometheus state gauge

In a fourth terminal:

```bash
watch -n 0.5 'curl -s http://localhost:4900/metrics/prometheus \
  | grep -E "^core_circuitBreakers_heap_(open|memoryUsage)"'
```

Expect `core_circuitBreakers_heap_open` to flip to `1.0` and stay there
while the load runs.

### B. Client-side `RESOURCE_EXHAUSTED` (P1 proof)

The Armeria decorator (P1) rejects requests before any body is
parsed. gRPC clients get status `RESOURCE_EXHAUSTED` with a `RetryInfo`
detail; OTLP/HTTP clients get HTTP **429** with a `Retry-After` header. Both
are retryable for OTLP exporters, and the retry delay comes from the
source's `retry_info` setting. When the exporter gives up, `telemetrygen`
prints the error on stderr:

```
... rpc error: code = ResourceExhausted desc = Circuit breaker is open. Request rejected before reading the body.
```

Errors with that message = the decorator is rejecting **before** protobuf
parsing. That is what saves the 1–4 MB of allocations per request
the problem statement warns about.

### C. Server-side rejection logs (P2 proof)

The P2 defence-in-depth check in `OTelLogsGrpcService` /
`OTelMetricsGrpcService` catches requests that slipped past the HTTP
decorator during the open/close race window. It does **not** log: it answers
gRPC `RESOURCE_EXHAUSTED` with the message `Circuit breaker is open.` and
increments the source's `requestTimeouts` counter. Because it only covers a
race window, it is not hit in every run.

Do not confuse it with this log line, which you will also see while the
breaker is open:

```
... ERROR ...OTelTraceGrpcService - Failed to write the request of size 59326 due to:
java.util.concurrent.TimeoutException: Circuit breaker is open. Unable to write to buffer.
	at org.opensearch.dataprepper.core.parser.CircuitBreakingBuffer.checkBreaker(...)
```

That comes from the pre-existing `CircuitBreakingBuffer`, not from the
P2 check. To spot P2 on the metrics source, compare
`metrics_pipeline_otlp_metrics_requestTimeouts_total` against the
number of `OTelMetricsGrpcService - Failed to write the request` lines: any
excess is P2, as long as there are no WARN lines `... request already timed out.`
`heap-circuit-breaker.sh --source metrics` automates that comparison (also for `--source logs`).

---

## 8. Verify hysteresis (P3)

Stop the load (Ctrl-C the `telemetrygen` from §6) and watch the DP terminal:

1. Heap usage starts dropping.
2. The breaker **does not** close immediately when usage crosses below 200 MB.
3. It stays open until usage falls to **150 MB** (`close_usage`) or below, then logs:

```
... INFO  ...HeapCircuitBreaker - Circuit breaker closed. 109.8 MiB (115160680 bytes) used <= 150.0 MiB (157286400 bytes) configured close threshold
```

That gap between "usage < 200mb" and "Circuit breaker closed" is the
oscillation-prevention band P3 buys you. Without `close_usage`,
the breaker would flap around the single threshold, about once per
`reset` + `check_interval`.

---

## 9. Recovery sanity check

Replay the baseline request from §5 — it should succeed again and the
Prometheus gauge should be back at `0.0`. Pipeline survived a breaker trip
without manual intervention.

---

## 10. P4 — peer forwarder (three nodes)

P4 makes the peer forwarder server answer inbound peer-forwarder
requests with HTTP 429 while the breaker is open, **before** the request body
is read: `PeerForwarderHttpServerProvider` registers the same
`CircuitBreakerDecoratingHttpService` as P1. A flood of inbound peer-forwarder
traffic can no longer bypass the breaker into the receive buffers. Exercising this path requires
the HTTP receive service (`PeerForwarderHttpService`), which is only used
when at least **two** nodes forward to each other and a processor needs peer
forwarding (`aggregate`, `otel_traces`, `service_map`,
`otel_apm_service_map`, `trace_peer_forwarder`). Single-node setups use the
in-process `LocalPeerForwarder` and never touch HTTP.

### Why this needs containers

Several Data Prepper instances cannot share one host for this test:

- The peer forwarder server listens on **all** interfaces on its `port`
  (default 4994), so a loopback alias such as `127.0.0.2` does not separate
  two instances.
- A node always forwards to the `port` from **its own** configuration, so all
  nodes must use the same port.

`heap-circuit-breaker.sh --nodes 3` therefore starts three nodes in
Docker containers with their own IPs on a private network, plus OpenSearch,
and connects them with `discovery_mode: static`:

```bash
# Default scenario: only the breaker causes 429 responses (asserted).
./docs/manual-tests/heap-circuit-breaker.sh --skip-build --nodes 3

# Flood on dp3 on top of steady traffic (observational).
./docs/manual-tests/heap-circuit-breaker.sh --skip-build --nodes 3 --scenario load-wave
```

The `isolated` scenario sends low traffic to dp1 only. It first measures
dp3's heap under that traffic and restarts dp3 with breaker thresholds just
above it, so dp3's breaker opens and closes by itself. It then asserts:

| Check | Pass criterion |
|---|---|
| dp3's breaker trips | at least one `Circuit breaker tripped and open` on dp3 |
| P4 is the only failure source | P4 rejections logged by dp3 = failed forwarding requests counted by dp1 |
| Nothing is lost | no `Dropping N records` on dp1 |
| Test is isolated | no full batching queue on dp1, no inbound write failures on any node |
| Health check follows the breaker | dp3's gRPC health check answers RESOURCE_EXHAUSTED while open, OK while closed |

Samples (every 0.5 s) and timed events are written to
`/tmp/cb-test-trace-n3-<scenario>/` for plotting.

The peer forwarder settings used by the script (`buffer_size: 2048`,
`forwarding_batch_size: 500`, `forwarding_batch_timeout: 500ms`) differ from
the defaults on purpose. With the defaults (512 / 1500), any forwarded batch
larger than 512 records is rejected by the receiver and then dropped by the
sender, independent of the circuit breaker.

> If you don't want to run containers, the unit test
> `PeerForwarderHttpServerProviderTest#get_with_open_circuit_breaker_rejects_requests_with_429_before_the_service`
> covers the same code path deterministically.

---

## 11. Cleanup

```bash
# Stop Data Prepper(s) (Ctrl-C each foreground process), then:
docker rm -f cb-test-os
unset DP_HOME DP_VERSION
# The automated runner cleans up after itself; after --keep-running use:
./docs/manual-tests/heap-circuit-breaker.sh --cleanup
```

---

## Assertion matrix (what to check off)

| Verifying | Signal | Where | Pass criterion |
|---|---|---|---|
| Breaker opens | `Circuit breaker tripped and open ...` | DP stdout (INFO) | appears under §6 load |
| State observable | `core_circuitBreakers_heap_open` gauge | `http://localhost:4900/metrics/prometheus` | flips to `1.0` |
| Heap reported | `core_circuitBreakers_heap_memoryUsage` gauge | same | climbs past `2.097152E8` (200 MB) |
| **P1** HTTP decorator | gRPC `RESOURCE_EXHAUSTED` / HTTP 429 | `telemetrygen` stderr | non-zero count |
| **P2** gRPC pre-parse | `requestTimeouts` counter of the source grows beyond the `Failed to write the request` log lines; client sees `RESOURCE_EXHAUSTED: Circuit breaker is open.` | `/metrics/prometheus` + DP stdout | race window; not guaranteed every run (unit tests cover it deterministically) |
| **P3** hysteresis | gap between heap dropping below 200 MB and `Circuit breaker closed` | DP stdout (INFO) | close threshold is 150 MB, not 200 MB |
| **P4** peer fwd | `Rejecting peer forwarder request: circuit breaker is open.` | DP stdout (DEBUG) | asserted by `heap-circuit-breaker.sh --nodes 3` (§10) |
| Recovery | `Circuit breaker closed ...` after load stops | DP stdout (INFO) | appears |

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| No `Circuit breaker tripped` log | Root logger is `warn` and you didn't write §3c. | Recreate `log4j2-rolling.properties` per §3c and restart DP. |
| `core_circuitBreakers_heap_open` never appears in Prometheus | `circuit_breakers` block missing from `data-prepper-config.yaml`. | Recreate §3a. |
| Breaker won't trip even at `--rate 10000` | Heap not actually constrained (env not picked up). | Confirm `jps -v` shows `-Xmx256m` for `DataPrepperExecute`; some shells eat `JAVA_OPTS`. |
| `telemetrygen` errors out immediately with `connection refused` | OTel source not bound yet. | Wait for `Pipeline [traces-pipeline] - Submitting request to initiate` style log; retry. |
| OpenSearch sink errors with `Couldn't connect to "http://localhost:9200"` | Container not up or security still on. | `curl http://localhost:9200` should return JSON; re-run §2. |
| Breaker closes immediately as soon as load stops | `close_usage` not honoured (config typo). | Re-check `data-prepper-config.yaml` — the startup log must show two **different** byte counts. |
| `Circuit breaker tripped` appears even at idle | `usage` too low for baseline footprint. | Raise `usage`/`close_usage` together with the heap (e.g. `-Xmx384m` with `300mb`/`225mb`). |
| DP dies with `OutOfMemoryError: Java heap space` | Heap too small (e.g. `-Xmx128m`), made worse by sink retry loops while OpenSearch rejects requests. | Use at least `-Xmx256m`; fix the OpenSearch side first. |
| Sink logs `Failed to initialize OpenSearch sink, retrying: Forbidden access` and the OTel ports never open | Host disk above OpenSearch's flood-stage watermark (~95%); OpenSearch blocks index creation (`index_create_block_exception`). | Free disk space, or for this throwaway container only: `curl -XPUT localhost:9200/_cluster/settings -H 'Content-Type: application/json' -d '{"persistent":{"cluster.routing.allocation.disk.threshold_enabled":false,"cluster.blocks.create_index":null}}'`. |
| `_count` stays `0` right after sending data | The OpenSearch sink buffers documents until `bulk_size` or its flush timeout (~60 s). | Wait up to a minute and re-run the `_count` query. |

---

## Why this playbook looks the way it does

- **Foreground processes, not docker-compose.** A one-shot compose stack
  would hide the very logs we're trying to read. Manual debugging benefits
  from explicit terminals.
- **Three OTel sources in one pipeline file.** P1 is installed on the server
  of *each* OTel source, and P2 lives in the logs and metrics gRPC services
  only, so we float a tiny pipeline per signal instead of trusting one to
  generalise.
- **`MemoryMXBean` lag, as called out in the problem statement.** Expect a
  few hundred ms of slop between the moment the heap actually exceeds 200 MB
  and the `tripped and open` log — that's why `check_interval` is 500 ms.
- **gRPC clients retry on `RESOURCE_EXHAUSTED` with the server's retry delay.** A clean
  `telemetrygen` run does **not** prove the breaker stayed closed; always
  cross-check the server-side log and the Prometheus gauge.

