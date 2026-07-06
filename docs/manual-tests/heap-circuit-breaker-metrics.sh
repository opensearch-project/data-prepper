#!/usr/bin/env bash

# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

# =============================================================================
# heap-circuit-breaker.sh
#
# Automated runner for docs/manual-tests/heap-circuit-breaker.md.
#
# Boots a single-node OpenSearch (Docker) + a locally-built Data Prepper,
# floods it with telemetrygen traces, and asserts that the heap circuit
# breaker opens, that the layers we hardened (P1/P2/P4) actually engage,
# and that the breaker closes again when load stops.
#
# Usage:
#   ./heap-circuit-breaker.sh [--skip-build] [--keep-running] [--cleanup]
#                             [--with-peer-forwarder] [--help]
#
# Flags:
#   --skip-build           Don't run ./gradlew assemble (assume DP is already built).
#   --keep-running         Leave DP + OpenSearch up after the run for manual poking.
#   --cleanup              Tear down any prior run (container + DP processes) and exit.
#   --with-peer-forwarder  Print pointers for the P5 variant (not automated).
#   -h, --help             Show this help.
#
# Tunables (env vars):
#   LOAD_DURATION   Seconds the high-rate load runs.   Default: 30
#   LOAD_RATE       telemetrygen --rate.                Default: 5000
#   LOAD_WORKERS    telemetrygen --workers.             Default: 100
#   DP_HEAP         JVM heap flags.                     Default: "-Xms256m -Xmx256m"
#   JAVA_HOME       JDK 11+ to run DP and Gradle with.  Default: a developer-local JDK 21
#                   (override by exporting JAVA_HOME before running).
#
# Exit code: 0 if every assertion PASSED, 1 if any FAILED, >1 on setup error.
# =============================================================================

set -euo pipefail

# ---------- Constants ----------
readonly CONTAINER_NAME="cb-test-os"
readonly OS_IMAGE="opensearchproject/opensearch:2"
readonly OS_PORT=9200
readonly DP_ADMIN_PORT=4900
readonly OTEL_TRACE_PORT=21890
readonly DP_LOG="/tmp/cb-test-dp.log"
readonly DP_PID="/tmp/cb-test-dp.pid"
readonly TELEM_LOG="/tmp/cb-test-telemetrygen.log"
readonly TRIP_TELEM_LOG="/tmp/cb-test-telemetrygen-trip.log"

# Thresholds — must match what we write into data-prepper-config.yaml below.
readonly OPEN_BYTES=209715200   # 200 * 1024 * 1024
readonly CLOSE_BYTES=157286400  # 150 * 1024 * 1024

LOAD_DURATION="${LOAD_DURATION:-30}"
LOAD_RATE="${LOAD_RATE:-5000}"
LOAD_WORKERS="${LOAD_WORKERS:-100}"
DP_HEAP="${DP_HEAP:--Xms256m -Xmx256m}"

# ---------- Repo root + arch ----------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
readonly SCRIPT_DIR REPO_ROOT

case "$(uname -m)" in
    x86_64)            ARCH=x64 ;;
    aarch64|arm64)     ARCH=arm64 ;;
    *) echo "Unsupported architecture: $(uname -m)" >&2; exit 2 ;;
esac
readonly ARCH
# Gradle task names from :release:archives:linux are: installLinuxx64Dist / installLinuxarm64Dist
# (capitalize() in Gradle only capitalizes the first character of "linuxx64").
readonly INSTALL_TASK="installLinux${ARCH}Dist"

# ---------- Java ----------
# Data Prepper needs Java 11+. JAVA_HOME from the environment wins; otherwise
# we fall back to a developer-local JDK so the script works out of the box on
# this machine. Override by exporting JAVA_HOME=/path/to/jdk before running.
#
# Why prepend to PATH (not just export JAVA_HOME)?
#   bin/data-prepper does `if type -p java; then _java=java` *before* it falls
#   back to $JAVA_HOME/bin/java — so the only reliable way to make it pick our
#   JDK is to make `java` on PATH resolve to it.
JAVA_HOME="${JAVA_HOME:-/home/tlongo/programming/languages/jdks/amazon-corretto-21.0.3.9.1-linux-x64}"
export JAVA_HOME
export PATH="$JAVA_HOME/bin:$PATH"
readonly MIN_JAVA_MAJOR=11

# ---------- Flags ----------
SKIP_BUILD=false
KEEP_RUNNING=false
CLEANUP_ONLY=false
WITH_PEER_FORWARDER=false

usage() { sed -n '3,28p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; }

while [[ $# -gt 0 ]]; do
    case "$1" in
        --skip-build)           SKIP_BUILD=true ;;
        --keep-running)         KEEP_RUNNING=true ;;
        --cleanup)              CLEANUP_ONLY=true ;;
        --with-peer-forwarder)  WITH_PEER_FORWARDER=true ;;
        -h|--help)              usage; exit 0 ;;
        *) echo "Unknown flag: $1 (see --help)" >&2; exit 2 ;;
    esac
    shift
done

# ---------- Pretty output ----------
if [[ -t 1 ]]; then
    C_RESET=$'\e[0m'; C_DIM=$'\e[2m'
    C_RED=$'\e[31m'; C_GRN=$'\e[32m'; C_YEL=$'\e[33m'; C_CYN=$'\e[36m'; C_BLD=$'\e[1m'
else
    C_RESET=""; C_DIM=""; C_RED=""; C_GRN=""; C_YEL=""; C_CYN=""; C_BLD=""
fi

FAILURES=0
WARNINGS=0
log()     { printf '%s[%s]%s %s\n' "$C_CYN" "$(date +%H:%M:%S)" "$C_RESET" "$*"; }
section() { printf '\n%s=== %s ===%s\n' "$C_BLD$C_YEL" "$*" "$C_RESET"; }
pass()    { printf '%s✅ PASS%s %s\n' "$C_GRN" "$C_RESET" "$*"; }
fail()    { printf '%s❌ FAIL%s %s\n' "$C_RED" "$C_RESET" "$*"; FAILURES=$((FAILURES + 1)); }
warn()    { printf '%s⚠  WARN%s %s\n' "$C_YEL" "$C_RESET" "$*"; WARNINGS=$((WARNINGS + 1)); }
die()     { printf '%sERROR%s %s\n' "$C_RED" "$C_RESET" "$*" >&2; exit 2; }

# ---------- Cleanup trap ----------
cleanup() {
    local rc=$?
    set +e
    trap - EXIT INT TERM
    if [[ -f "$DP_PID" ]]; then
        local pid
        pid="$(cat "$DP_PID" 2>/dev/null || true)"
        if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null; then
            log "Stopping Data Prepper (pid=$pid)..."
            kill "$pid" 2>/dev/null
            for _ in 1 2 3 4 5; do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
            kill -9 "$pid" 2>/dev/null || true
        fi
        rm -f "$DP_PID"
    fi
    # Belt-and-braces — kill any orphaned DP.
    pkill -f "org.opensearch.dataprepper.DataPrepperExecute" 2>/dev/null || true

    if ! $KEEP_RUNNING; then
        log "Removing OpenSearch container..."
        docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
    else
        log "Leaving OpenSearch container running ($CONTAINER_NAME). Logs: $DP_LOG"
        log "Cleanup later with: $0 --cleanup"
    fi
    exit "$rc"
}
trap cleanup EXIT INT TERM

# =============================================================================
# Phases
# =============================================================================

phase_cleanup_only() {
    section "Cleanup only"
    pkill -f "org.opensearch.dataprepper.DataPrepperExecute" 2>/dev/null && log "killed running Data Prepper(s)" || log "no running Data Prepper"
    docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 && log "removed container $CONTAINER_NAME" || log "no container to remove"
    rm -f "$DP_PID" "$DP_LOG" "$TELEM_LOG" "$TRIP_TELEM_LOG"
    log "Done."
}

phase_check_prereqs() {
    section "Checking prerequisites"
    local missing=0
    for cmd in docker curl jq awk grep sed; do
        if ! command -v "$cmd" >/dev/null 2>&1; then
            printf '%s missing: %s%s\n' "$C_RED" "$cmd" "$C_RESET" >&2
            missing=$((missing + 1))
        fi
    done
    if ! command -v telemetrygen >/dev/null 2>&1; then
        printf '%s missing: telemetrygen%s\n' "$C_RED" "$C_RESET" >&2
        printf '   install: %sgo install github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen@latest%s\n' "$C_DIM" "$C_RESET" >&2
        printf '   then:    %sexport PATH="$PATH:$(go env GOPATH)/bin"%s\n' "$C_DIM" "$C_RESET" >&2
        missing=$((missing + 1))
    fi

    # Java: JAVA_HOME has been set above. Verify it points at a JDK ≥ 11.
    if [[ ! -x "$JAVA_HOME/bin/java" ]]; then
        printf '%s missing: %s/bin/java (set JAVA_HOME to a JDK %d+)%s\n' \
            "$C_RED" "$JAVA_HOME" "$MIN_JAVA_MAJOR" "$C_RESET" >&2
        missing=$((missing + 1))
    else
        local jver jmajor
        jver="$("$JAVA_HOME/bin/java" -version 2>&1 | awk -F\" '/version/ {print $2; exit}')"
        if [[ "$jver" =~ ^1\.([0-9]+) ]]; then
            jmajor="${BASH_REMATCH[1]}"          # "1.8.0_xxx" → 8
        else
            jmajor="${jver%%.*}"                  # "21.0.x" → 21
        fi
        if (( jmajor < MIN_JAVA_MAJOR )); then
            printf '%s Java %s at %s is too old (need %d+).%s\n' \
                "$C_RED" "$jver" "$JAVA_HOME" "$MIN_JAVA_MAJOR" "$C_RESET" >&2
            printf '   fix: %sexport JAVA_HOME=/path/to/jdk-%d+%s\n' \
                "$C_DIM" "$MIN_JAVA_MAJOR" "$C_RESET" >&2
            missing=$((missing + 1))
        else
            log "Using Java $jver from $JAVA_HOME"
        fi
    fi

    (( missing == 0 )) || die "$missing prerequisite(s) missing"
    docker info >/dev/null 2>&1 || die "Docker daemon not reachable (try: sudo systemctl start docker)"
    log "All prerequisites present."
}

phase_build() {
    section "Building Data Prepper (${INSTALL_TASK}) — slow on first run"
    ( cd "$REPO_ROOT" && ./gradlew ":release:archives:linux:${INSTALL_TASK}" )
}

phase_locate_dp_home() {
    local version
    version="$(grep '^version=' "$REPO_ROOT/gradle.properties" | cut -d= -f2)"
    DP_HOME="$REPO_ROOT/release/archives/linux/build/install/opensearch-data-prepper-${version}-linux-${ARCH}"
    readonly DP_HOME
    [[ -d "$DP_HOME" ]] || die "DP_HOME not found: $DP_HOME (run without --skip-build first)"
    log "DP_HOME=$DP_HOME"
}

phase_start_opensearch() {
    section "Starting single-node OpenSearch (security disabled)"
    docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
    docker run -d --name "$CONTAINER_NAME" \
        -p "${OS_PORT}:9200" -p 9600:9600 \
        -e "discovery.type=single-node" \
        -e "DISABLE_SECURITY_PLUGIN=true" \
        -e "OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m" \
        "$OS_IMAGE" >/dev/null

    log "Waiting for OpenSearch on :$OS_PORT ..."
    local i
    for i in $(seq 1 60); do
        if curl -fs "http://localhost:${OS_PORT}/_cluster/health" >/dev/null 2>&1; then
            log "OpenSearch is up: $(curl -s http://localhost:${OS_PORT} | jq -r '.version.number')"
            return 0
        fi
        sleep 2
    done
    docker logs --tail 50 "$CONTAINER_NAME" >&2 || true
    die "OpenSearch did not start within 120s"
}

phase_write_configs() {
    section "Writing configs into $DP_HOME/{config,pipelines}/"
    mkdir -p "$DP_HOME/config" "$DP_HOME/pipelines"

    cat > "$DP_HOME/config/data-prepper-config.yaml" <<'YAML'
ssl: false
metric_registries: [Prometheus]

# Tuned for -Xmx256m: usage ~76%, close_usage ~57% → forces hysteresis (P4).
# 200mb / 150mb sits comfortably above the ~100mb idle footprint of three
# OTel pipelines + OpenSearch sinks, so the breaker doesn't trip at startup.
circuit_breakers:
  heap:
    usage: 200mb
    close_usage: 150mb
    reset: 2s
    check_interval: 500ms
YAML

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

# Make breaker open/close (INFO) visible.
logger.breaker.name = org.opensearch.dataprepper.core.breaker
logger.breaker.level = info

# P5 peer-forwarder rejection line is DEBUG.
logger.peerforwarder.name = org.opensearch.dataprepper.core.peerforwarder
logger.peerforwarder.level = debug

# P2 OTel gRPC pre-parse rejection path.
logger.oteltrace.name = org.opensearch.dataprepper.plugins.source.oteltrace
logger.oteltrace.level = debug
logger.otellogs.name = org.opensearch.dataprepper.plugins.source.otellogs
logger.otellogs.level = debug
logger.otelmetrics.name = org.opensearch.dataprepper.plugins.source.otelmetrics
logger.otelmetrics.level = debug
PROPS
    log "Configs written."
}

phase_start_dataprepper() {
    section "Starting Data Prepper (heap=$DP_HEAP, log=$DP_LOG)"
    rm -f "$DP_LOG" "$DP_PID"
    pkill -f "org.opensearch.dataprepper.DataPrepperExecute" 2>/dev/null || true

    (
        cd "$DP_HOME"
        # data-prepper-x64.sh reads JAVA_OPTS and exec's java with our flags.
        JAVA_OPTS="$DP_HEAP" nohup bin/data-prepper >"$DP_LOG" 2>&1 &
        echo $! > "$DP_PID"
    )

    log "Data Prepper pid=$(cat "$DP_PID"). Waiting for admin endpoint on :$DP_ADMIN_PORT ..."
    local i
    for i in $(seq 1 90); do
        if curl -fs "http://localhost:${DP_ADMIN_PORT}/metrics/prometheus" >/dev/null 2>&1; then
            log "Data Prepper is ready."
            break
        fi
        if ! kill -0 "$(cat "$DP_PID")" 2>/dev/null; then
            tail -50 "$DP_LOG" >&2 || true
            die "Data Prepper died during startup (see $DP_LOG)"
        fi
        sleep 1
    done

    if ! curl -fs "http://localhost:${DP_ADMIN_PORT}/metrics/prometheus" >/dev/null 2>&1; then
        tail -50 "$DP_LOG" >&2 || true
        die "Data Prepper admin endpoint never came up"
    fi

    # Proof point: startup log shows both thresholds at the configured bytes.
    if grep -q "Circuit breaker heap open threshold is ${OPEN_BYTES} bytes, close threshold is ${CLOSE_BYTES} bytes" "$DP_LOG"; then
        pass "Startup log shows usage=${OPEN_BYTES}B / close_usage=${CLOSE_BYTES}B (hysteresis configured)."
    else
        fail "Startup log missing or has wrong breaker thresholds — see $DP_LOG."
    fi
}

# Read a single Prometheus gauge value (first sample line for the metric).
# Echoes the numeric value or an empty string. Tolerates curl/HTTP/pipe
# failures (the admin endpoint can momentarily choke under the heavy flood)
# so callers can rely on a string return without `set -e` killing the run.
prom_value() {
    local metric="$1" body
    body="$(curl -fs --max-time 3 "http://localhost:${DP_ADMIN_PORT}/metrics/prometheus" 2>/dev/null || true)"
    [[ -z "$body" ]] && return 0
    printf '%s\n' "$body" | awk -v m="^${metric}( |$|{)" '$0 ~ m { print $NF; exit }' || true
}

# True iff awk treats $1 as numerically zero.
is_zero() { awk -v v="${1:-1}" 'BEGIN { exit (v + 0 == 0) ? 0 : 1 }'; }

# Wait up to $1 seconds for $2 (literal substring) to appear in $DP_LOG.
wait_for_log() {
    local timeout="$1" needle="$2" i
    for i in $(seq 1 "$timeout"); do
        if grep -qF "$needle" "$DP_LOG" 2>/dev/null; then
            return 0
        fi
        sleep 1
    done
    return 1
}

phase_baseline() {
    section "Baseline: breaker must be closed before we trip it"
    rm -f "$TELEM_LOG"
    # Brief warm-up. We intentionally do NOT assert the OpenSearch document
    # count here — that depends on sink bulk_size / flush_timeout defaults
    # and is unrelated to whether the breaker behaves correctly.
    timeout --foreground --kill-after=5s 25s telemetrygen traces \
        --otlp-endpoint "localhost:${OTEL_TRACE_PORT}" --otlp-insecure \
        --duration 5s --rate 10 --workers 1 >"$TELEM_LOG" 2>&1 || true

    local v; v="$(prom_value core_circuitBreakers_heap_open)"
    if is_zero "${v:-1}"; then
        pass "core_circuitBreakers_heap_open=$v (closed) — pipelines warm, ready to trip."
    else
        fail "core_circuitBreakers_heap_open=$v (expected 0) — baseline already over threshold; raise 'usage' in data-prepper-config.yaml."
    fi
}

phase_trip() {
    section "Tripping the breaker (workers=$LOAD_WORKERS rate=$LOAD_RATE duration=${LOAD_DURATION}s)"
    rm -f "$TRIP_TELEM_LOG"

    # Snapshot the DP log size so subsequent greps only see events that occurred
    # *during* this phase — avoids confusion with any pre-existing breaker activity.
    local pre_size
    pre_size="$(stat -c%s "$DP_LOG" 2>/dev/null || echo 0)"

    # Wrap telemetrygen in `timeout` — under sustained backpressure the gRPC
    # client can block on channel drain well past --duration, stalling the test.
    timeout --foreground --kill-after=10s "$((LOAD_DURATION + 30))s" \
        telemetrygen traces \
            --otlp-endpoint "localhost:${OTEL_TRACE_PORT}" --otlp-insecure \
            --workers "$LOAD_WORKERS" --rate "$LOAD_RATE" --duration "${LOAD_DURATION}s" \
            >"$TRIP_TELEM_LOG" 2>&1 &
    local telem_pid=$!

    # Assertion 1: server logs "tripped and open" within 30s.
    if wait_for_log 30 "Circuit breaker tripped and open"; then
        local trip_line tripped_bytes
        trip_line="$(tail -c "+$((pre_size + 1))" "$DP_LOG" | grep -m1 "Circuit breaker tripped and open" || true)"
        # If nothing post-marker (rare), fall back to the first match anywhere.
        [[ -z "$trip_line" ]] && trip_line="$(grep -m1 "Circuit breaker tripped and open" "$DP_LOG")"
        pass "Found 'Circuit breaker tripped and open' in DP log:"
        printf '        %s%s%s\n' "$C_DIM" "$trip_line" "$C_RESET"

        # Definitive proof from the log line itself — no Prometheus race.
        tripped_bytes="$(echo "$trip_line" | sed -nE 's/.*tripped and open\. ([0-9]+) used.*/\1/p')"
        if [[ -n "$tripped_bytes" ]] && (( tripped_bytes > OPEN_BYTES )); then
            pass "Breaker tripped at $tripped_bytes bytes (> open threshold $OPEN_BYTES)."
        else
            fail "Trip log bytes='${tripped_bytes:-?}' not above open threshold $OPEN_BYTES."
        fi
    else
        fail "Breaker did not trip in 30s (try raising LOAD_RATE/LOAD_WORKERS)."
    fi

    # Assertion 2: Prometheus gauge currently reports open.
    local v; v="$(prom_value core_circuitBreakers_heap_open)"
    if [[ -n "$v" ]] && ! is_zero "$v"; then
        pass "core_circuitBreakers_heap_open=$v (open)."
    else
        fail "core_circuitBreakers_heap_open=${v:-<missing>} (expected 1)."
    fi

    # Let the load finish so the next phase can observe close behaviour.
    wait "$telem_pid" 2>/dev/null || true

    # Assertion 3 (P2): defence-in-depth check inside OTel gRPC services.
    # This is the DETERMINISTIC server-side proof that the breaker rejected
    # requests — every gRPC request that found the breaker open between trip
    # and close logs a "Circuit breaker is open" TimeoutException here.
    local p2
    p2="$(tail -c "+$((pre_size + 1))" "$DP_LOG" | grep -cE 'Circuit breaker is open' 2>/dev/null || true)"
    p2="${p2:-0}"
    if (( p2 > 0 )); then
        pass "Found $p2 server-side 'Circuit breaker is open' log line(s) — P2 (gRPC pre-parse) engaged."
    else
        fail "No 'Circuit breaker is open' rejection logged by OTel services (P2 may not be wired)."
    fi

    # Soft check (P1 visibility): client-side rejection in telemetrygen stderr.
    # The OTLP exporter retries silently with exponential backoff for up to
    # several minutes, so a single trip→close cycle (Priority 4 hysteresis is
    # ~2s) may complete entirely within the retry window and surface no
    # individual errors. The deterministic P1 proof lives in the server-side
    # log + the breaker-tripped log above; this is informational only.
    local rej
    rej="$(grep -cE 'Circuit breaker is open|code = (Unavailable|ResourceExhausted)|HTTP/.* 503|rpc error|export timeout' "$TRIP_TELEM_LOG" 2>/dev/null || true)"
    rej="${rej:-0}"
    if (( rej > 0 )); then
        pass "telemetrygen surfaced $rej rejection/error line(s) — P1 visible client-side."
    else
        warn "telemetrygen surfaced 0 errors (likely OTLP retries absorbed them; server-side P2 above is the deterministic proof). Trip log: $TRIP_TELEM_LOG"
    fi
}

phase_hysteresis() {
    section "Verifying hysteresis (P4) — breaker must settle below close threshold (${CLOSE_BYTES}B)"
    log "Load stopped. Waiting up to 90s for breaker to settle closed (sink + GC catch up)..."

    # The breaker oscillates during sustained load; we want the *final* settled
    # state. Wait until the Prometheus gauge stays at 0 for 3 consecutive
    # seconds — that proves the breaker is no longer flapping.
    local stable=0 i v=""
    for i in $(seq 1 90); do
        v="$(prom_value core_circuitBreakers_heap_open)"
        if is_zero "${v:-1}"; then
            stable=$((stable + 1))
            (( stable >= 3 )) && break
        else
            stable=0
        fi
        sleep 1
    done

    if (( stable < 3 )); then
        fail "Breaker did not settle closed within 90s (last gauge='$v'); see $DP_LOG."
        return
    fi
    pass "Breaker settled closed (gauge=$v stable for ${stable}s)."

    # Find the MOST RECENT "Circuit breaker closed" event and assert its
    # bytes value respects the close threshold — proves the hysteresis band.
    local last_close used_bytes
    last_close="$(grep "Circuit breaker closed" "$DP_LOG" | tail -1)"
    if [[ -z "$last_close" ]]; then
        fail "No 'Circuit breaker closed' log line found at all."
        return
    fi
    printf '        %s%s%s\n' "$C_DIM" "$last_close" "$C_RESET"

    used_bytes="$(echo "$last_close" | sed -nE 's/.*Circuit breaker closed\. ([0-9]+) used memory.*/\1/p')"
    if [[ -n "$used_bytes" ]] && (( used_bytes <= CLOSE_BYTES )); then
        pass "Most-recent close at $used_bytes bytes (≤ close threshold $CLOSE_BYTES)."
    else
        fail "Most-recent close at '${used_bytes:-?}' bytes (expected ≤ $CLOSE_BYTES)."
    fi
}

phase_recovery() {
    section "Recovery sanity check — low-rate flow must not re-trip the breaker"
    rm -f "$TELEM_LOG"
    timeout --foreground --kill-after=5s 25s telemetrygen traces \
        --otlp-endpoint "localhost:${OTEL_TRACE_PORT}" --otlp-insecure \
        --duration 10s --rate 20 --workers 2 >"$TELEM_LOG" 2>&1 || true

    # Sample the gauge twice with a gap — guards against catching a single
    # transient half-second of openness during sink flush.
    sleep 2
    local v1 v2
    v1="$(prom_value core_circuitBreakers_heap_open)"
    sleep 3
    v2="$(prom_value core_circuitBreakers_heap_open)"
    if is_zero "${v1:-1}" && is_zero "${v2:-1}"; then
        pass "Breaker remained closed under post-recovery baseline (gauge=$v1 → $v2)."
    else
        fail "Breaker reopened under recovery baseline (gauge=$v1 → $v2)."
    fi
}

phase_p5_pointer() {
    section "Priority 5 — peer forwarder (not automated)"
    cat <<EOM
The P5 variant needs:
  - sudo (loopback alias: ip addr add 127.0.0.2/8 dev lo)
  - a second Data Prepper instance with juggled ports
  - a pipeline using a peer-forwarded processor (e.g. aggregate)

See §10 of docs/manual-tests/heap-circuit-breaker.md.

Deterministic equivalent (no infra):
  ./gradlew :data-prepper-core:test \\
      --tests 'org.opensearch.dataprepper.core.peerforwarder.server.PeerForwarderHttpServiceTest.doPost_circuitBreakerOpen_rejectsRequestBeforeParsing'
EOM
}

# =============================================================================
# Main
# =============================================================================

main() {
    if $CLEANUP_ONLY; then
        phase_cleanup_only
        trap - EXIT INT TERM
        exit 0
    fi

    phase_check_prereqs
    $SKIP_BUILD || phase_build
    phase_locate_dp_home
    phase_start_opensearch
    phase_write_configs
    phase_start_dataprepper
    phase_baseline
    phase_trip
    phase_hysteresis
    phase_recovery
    $WITH_PEER_FORWARDER && phase_p5_pointer || true

    section "Summary"
    if (( FAILURES == 0 )); then
        if (( WARNINGS == 0 )); then
            printf '%s🎉 All assertions PASSED%s\n' "$C_GRN$C_BLD" "$C_RESET"
        else
            printf '%s🎉 All assertions PASSED%s (%d warning(s) — see above)\n' \
                "$C_GRN$C_BLD" "$C_RESET" "$WARNINGS"
        fi
        exit 0
    else
        printf '%s❌ %d assertion(s) FAILED%s%s — see %s for full DP output\n' \
            "$C_RED$C_BLD" "$FAILURES" "$C_RESET" \
            "$( (( WARNINGS > 0 )) && printf ' (+%d warning(s))' "$WARNINGS")" \
            "$DP_LOG"
        exit 1
    fi
}

main "$@"




