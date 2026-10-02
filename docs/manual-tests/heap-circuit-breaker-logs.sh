#!/usr/bin/env bash

# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

# =============================================================================
# heap-circuit-breaker-logs.sh
#
# Logs-source-focused variant of heap-circuit-breaker.sh.
#
# Boots a single-node OpenSearch (Docker) + a locally-built Data Prepper with
# a LOGS-ONLY pipeline, floods the gRPC entry point with telemetrygen, and
# asserts that the heap circuit breaker engages on BOTH the gRPC and HTTP
# entry points of the OTel logs source.
#
# The logs source registers two services on the same Armeria ServerBuilder:
#   1. the gRPC LogsService (hit by telemetrygen / OTLP-gRPC clients)
#   2. an optional additional HTTP LogsService (hit by OTLP-HTTP/JSON clients),
#      configured by the `http_path` source setting. This script sets
#      http_path: /v1/logs in pipelines.yaml so the HTTP service is registered.
#
# A single server-level CircuitBreakerDecoratingHttpService wraps both. The
# deterministic proof that the HTTP path is gated is a parallel curl loop
# against the OTLP-HTTP endpoint that catches HTTP 503 responses while the
# gRPC flood keeps the breaker open.
#
# Usage:
#   ./heap-circuit-breaker-logs.sh [--skip-build] [--keep-running] [--cleanup] [--help]
#
# Flags:
#   --skip-build     Don't run ./gradlew assemble (assume DP is already built).
#   --keep-running   Leave DP + OpenSearch up after the run for manual poking.
#   --cleanup        Tear down any prior run (container + DP processes) and exit.
#   -h, --help       Show this help.
#
# Tunables (env vars):
#   LOAD_DURATION       Seconds the high-rate gRPC load runs.   Default: 30
#   LOAD_RATE           telemetrygen --rate.                    Default: 5000
#   LOAD_WORKERS        telemetrygen --workers.                 Default: 100
#   HTTP_PROBE_INTERVAL Seconds between HTTP probes.            Default: 0.2
#   DP_HEAP             JVM heap flags.                         Default: "-Xms256m -Xmx256m"
#   JAVA_HOME           JDK 11+ to run DP and Gradle with.      Default: developer-local JDK 21
#
# Exit code: 0 if every assertion PASSED, 1 if any FAILED, >1 on setup error.
# =============================================================================

set -euo pipefail

# ---------- Constants ----------
readonly CONTAINER_NAME="cb-test-logs-os"
readonly OS_IMAGE="opensearchproject/opensearch:2"
readonly OS_PORT=9200
readonly DP_ADMIN_PORT=4900
readonly OTEL_LOGS_PORT=21892
readonly DP_LOG="/tmp/cb-test-logs-dp.log"
readonly DP_PID="/tmp/cb-test-logs-dp.pid"
readonly TELEM_LOG="/tmp/cb-test-logs-telemetrygen.log"
readonly TRIP_TELEM_LOG="/tmp/cb-test-logs-telemetrygen-trip.log"
readonly HTTP_PROBE_LOG="/tmp/cb-test-logs-http-probes.log"
readonly HTTP_PROBE_STOP="/tmp/cb-test-logs-http-probe.stop"

# OTLP-HTTP/JSON entry point exposed by the logs source's additional HTTP
# service (see ArmeriaHttpService#@Post(""), path is set by http_path in YAML).
# Must match the http_path written below in phase_write_configs.
readonly OTLP_HTTP_PATH="/v1/logs"
# Minimal valid ExportLogsServiceRequest. The circuit-breaker decorator runs
# BEFORE body parsing, so payload content is irrelevant — we just need a
# well-formed POST that the inner HTTP service would otherwise accept.
readonly OTLP_PROBE_JSON='{}'

# Thresholds — must match what we write into data-prepper-config.yaml below.
readonly OPEN_BYTES=209715200   # 200 * 1024 * 1024
readonly CLOSE_BYTES=157286400  # 150 * 1024 * 1024

LOAD_DURATION="${LOAD_DURATION:-30}"
LOAD_RATE="${LOAD_RATE:-5000}"
LOAD_WORKERS="${LOAD_WORKERS:-100}"
HTTP_PROBE_INTERVAL="${HTTP_PROBE_INTERVAL:-0.2}"
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
readonly INSTALL_TASK="installLinux${ARCH}Dist"

# ---------- Java ----------
JAVA_HOME="${JAVA_HOME:-/home/tlongo/programming/languages/jdks/amazon-corretto-21.0.3.9.1-linux-x64}"
export JAVA_HOME
export PATH="$JAVA_HOME/bin:$PATH"
readonly MIN_JAVA_MAJOR=11

# ---------- Flags ----------
SKIP_BUILD=false
KEEP_RUNNING=false
CLEANUP_ONLY=false

usage() { sed -n '3,41p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; }

while [[ $# -gt 0 ]]; do
    case "$1" in
        --skip-build)   SKIP_BUILD=true ;;
        --keep-running) KEEP_RUNNING=true ;;
        --cleanup)      CLEANUP_ONLY=true ;;
        -h|--help)      usage; exit 0 ;;
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
    # Stop any background HTTP probe loop.
    touch "$HTTP_PROBE_STOP" 2>/dev/null || true
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
# Helpers
# =============================================================================

# Read a single Prometheus gauge value (first sample line for the metric).
prom_value() {
    local metric="$1" body
    body="$(curl -fs --max-time 3 "http://localhost:${DP_ADMIN_PORT}/metrics/prometheus" 2>/dev/null || true)"
    [[ -z "$body" ]] && return 0
    printf '%s\n' "$body" | awk -v m="^${metric}( |$|{)" '$0 ~ m { print $NF; exit }' || true
}

is_zero() { awk -v v="${1:-1}" 'BEGIN { exit (v + 0 == 0) ? 0 : 1 }'; }

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

# Send a single OTLP-HTTP/JSON logs probe. Echoes the HTTP status code (or
# "000" on connection failure / timeout). Body is discarded.
http_probe_once() {
    curl -s -o /dev/null -w "%{http_code}" \
        --max-time 3 \
        -X POST \
        -H "Content-Type: application/json" \
        -d "$OTLP_PROBE_JSON" \
        "http://localhost:${OTEL_LOGS_PORT}${OTLP_HTTP_PATH}" 2>/dev/null || echo "000"
}

# Run http_probe_once in a tight loop, one status code per line, until the
# stop-marker file appears. Designed to run in the background concurrently
# with the gRPC flood so it samples HTTP responses across open/closed windows.
http_probe_loop() {
    local stop_marker="$1" out="$2"
    : > "$out"
    while [[ ! -f "$stop_marker" ]]; do
        printf '%s\n' "$(http_probe_once)" >> "$out"
        sleep "$HTTP_PROBE_INTERVAL"
    done
}

# Tally (\n-separated) status codes in a probe-log file.
# Echoes "<n_200> <n_503> <n_other>".
http_probe_tally() {
    local file="$1" n_200 n_503 n_other
    n_200=$(grep -c '^200$' "$file" 2>/dev/null || true); n_200="${n_200:-0}"
    n_503=$(grep -c '^503$' "$file" 2>/dev/null || true); n_503="${n_503:-0}"
    n_other=$(grep -cvE '^(200|503)$' "$file" 2>/dev/null || true); n_other="${n_other:-0}"
    printf '%s %s %s' "$n_200" "$n_503" "$n_other"
}

# =============================================================================
# Phases
# =============================================================================

phase_cleanup_only() {
    section "Cleanup only"
    pkill -f "org.opensearch.dataprepper.DataPrepperExecute" 2>/dev/null && log "killed running Data Prepper(s)" || log "no running Data Prepper"
    docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 && log "removed container $CONTAINER_NAME" || log "no container to remove"
    rm -f "$DP_PID" "$DP_LOG" "$TELEM_LOG" "$TRIP_TELEM_LOG" "$HTTP_PROBE_LOG" "$HTTP_PROBE_STOP"
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

    if [[ ! -x "$JAVA_HOME/bin/java" ]]; then
        printf '%s missing: %s/bin/java (set JAVA_HOME to a JDK %d+)%s\n' \
            "$C_RED" "$JAVA_HOME" "$MIN_JAVA_MAJOR" "$C_RESET" >&2
        missing=$((missing + 1))
    else
        local jver jmajor
        jver="$("$JAVA_HOME/bin/java" -version 2>&1 | awk -F\" '/version/ {print $2; exit}')"
        if [[ "$jver" =~ ^1\.([0-9]+) ]]; then
            jmajor="${BASH_REMATCH[1]}"
        else
            jmajor="${jver%%.*}"
        fi
        if (( jmajor < MIN_JAVA_MAJOR )); then
            printf '%s Java %s at %s is too old (need %d+).%s\n' \
                "$C_RED" "$jver" "$JAVA_HOME" "$MIN_JAVA_MAJOR" "$C_RESET" >&2
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
    section "Writing LOGS-ONLY configs into $DP_HOME/{config,pipelines}/"
    mkdir -p "$DP_HOME/config" "$DP_HOME/pipelines"

    cat > "$DP_HOME/config/data-prepper-config.yaml" <<'YAML'
ssl: false
metric_registries: [Prometheus]

# Tuned for -Xmx256m with a single logs pipeline.
# 200mb / 150mb gives a comfortable hysteresis band well above the idle
# footprint (~60mb for one logs pipeline + OpenSearch sink) so the breaker
# doesn't trip at startup, but well below -Xmx256m so a sustained flood will
# trip it within a few seconds.
circuit_breakers:
  heap:
    usage: 200mb
    close_usage: 150mb
    reset: 2s
    check_interval: 500ms
YAML

    # Logs-only pipeline. We deliberately do NOT register metrics_pipeline or
    # traces_pipeline because we want all heap pressure (and all breaker activity)
    # to be attributable to the OTel logs source.
    #
    # http_path enables the additional HTTP service alongside the gRPC service.
    # Without this, the logs source only registers the gRPC entry point and our
    # HTTP probe loop below would always 404. The path here MUST match
    # OTLP_HTTP_PATH at the top of this script.
    cat > "$DP_HOME/pipelines/pipelines.yaml" <<'YAML'
logs-pipeline:
  source:
    otel_logs_source:
      ssl: false
      http_path: /v1/logs
  sink:
    - opensearch:
        hosts: [ "http://localhost:9200" ]
        insecure: true
        index: otel-logs
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

logger.otellogs.name = org.opensearch.dataprepper.plugins.source.otellogs
logger.otellogs.level = info
PROPS
    log "Configs written (logs-only pipeline with http_path=$OTLP_HTTP_PATH)."
}

phase_start_dataprepper() {
    section "Starting Data Prepper (heap=$DP_HEAP, log=$DP_LOG)"
    rm -f "$DP_LOG" "$DP_PID"
    pkill -f "org.opensearch.dataprepper.DataPrepperExecute" 2>/dev/null || true

    (
        cd "$DP_HOME"
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

    if grep -q "Circuit breaker heap open threshold is ${OPEN_BYTES} bytes, close threshold is ${CLOSE_BYTES} bytes" "$DP_LOG"; then
        pass "Startup log shows usage=${OPEN_BYTES}B / close_usage=${CLOSE_BYTES}B (hysteresis configured)."
    else
        fail "Startup log missing or has wrong breaker thresholds — see $DP_LOG."
    fi

    # Proof point specific to this script: confirm the logs source wired the
    # circuit-breaker HTTP decorator (OTelLogsSource.configureCircuitBreaker
    # logs this on startup when a CircuitBreaker bean is present).
    if grep -q "Installing circuit-breaker HTTP decorator for otel_logs_source" "$DP_LOG"; then
        pass "Logs source installed the circuit-breaker HTTP decorator at startup."
    else
        warn "Did not see 'Installing circuit-breaker HTTP decorator for otel_logs_source' in DP log — the HTTP-path proof below may fail."
    fi

    # Sanity check: the additional HTTP service was registered (logs source
    # only registers it when http_path is set).
    if grep -q "Configuring HTTP service under ${OTLP_HTTP_PATH}" "$DP_LOG"; then
        pass "Additional HTTP service registered under ${OTLP_HTTP_PATH}."
    else
        warn "Did not see 'Configuring HTTP service under ${OTLP_HTTP_PATH}' in DP log — http_path may have been silently ignored."
    fi
}

phase_baseline() {
    section "Baseline: breaker must be closed; gRPC and HTTP paths must succeed"
    rm -f "$TELEM_LOG"

    # gRPC warm-up via telemetrygen logs.
    timeout --foreground --kill-after=5s 25s telemetrygen logs \
        --otlp-endpoint "localhost:${OTEL_LOGS_PORT}" --otlp-insecure \
        --duration 5s --rate 10 --workers 1 >"$TELEM_LOG" 2>&1 || true

    local v; v="$(prom_value core_circuitBreakers_heap_open)"
    if is_zero "${v:-1}"; then
        pass "core_circuitBreakers_heap_open=$v (closed) — pipeline warm, ready to trip."
    else
        fail "core_circuitBreakers_heap_open=$v (expected 0) — baseline already over threshold; raise 'usage' in data-prepper-config.yaml."
    fi

    # HTTP probe: the additional HTTP service must accept an OTLP-HTTP/JSON
    # request when the breaker is closed (sanity check that the path is wired
    # and the decorator delegates through).
    local status; status="$(http_probe_once)"
    if [[ "$status" == "200" ]]; then
        pass "HTTP path returned 200 for OTLP-HTTP/JSON probe — additional HTTP service reachable, decorator is delegating."
    else
        fail "HTTP path returned '$status' for OTLP-HTTP/JSON probe at baseline (expected 200). Path: $OTLP_HTTP_PATH"
    fi
}

phase_trip() {
    section "Tripping breaker via gRPC flood; probing HTTP path in parallel"
    rm -f "$TRIP_TELEM_LOG" "$HTTP_PROBE_LOG" "$HTTP_PROBE_STOP"

    local pre_size
    pre_size="$(stat -c%s "$DP_LOG" 2>/dev/null || echo 0)"

    # Background: continuous HTTP probe loop. This is the deterministic proof
    # that the server-level decorator on the logs source's ServerBuilder
    # rejects requests on the HTTP entry point when the breaker is open.
    log "Starting HTTP probe loop (interval=${HTTP_PROBE_INTERVAL}s) → $HTTP_PROBE_LOG"
    http_probe_loop "$HTTP_PROBE_STOP" "$HTTP_PROBE_LOG" &
    local probe_pid=$!

    # Background: gRPC flood with telemetrygen logs.
    log "Starting telemetrygen logs gRPC flood (workers=$LOAD_WORKERS rate=$LOAD_RATE duration=${LOAD_DURATION}s)"
    timeout --foreground --kill-after=10s "$((LOAD_DURATION + 30))s" \
        telemetrygen logs \
            --otlp-endpoint "localhost:${OTEL_LOGS_PORT}" --otlp-insecure \
            --workers "$LOAD_WORKERS" --rate "$LOAD_RATE" --duration "${LOAD_DURATION}s" \
            >"$TRIP_TELEM_LOG" 2>&1 &
    local telem_pid=$!

    # Assertion 1: server logs "tripped and open" within 30s.
    if wait_for_log 30 "Circuit breaker tripped and open"; then
        local trip_line tripped_bytes
        trip_line="$(tail -c "+$((pre_size + 1))" "$DP_LOG" | grep -m1 "Circuit breaker tripped and open" || true)"
        [[ -z "$trip_line" ]] && trip_line="$(grep -m1 "Circuit breaker tripped and open" "$DP_LOG")"
        pass "Found 'Circuit breaker tripped and open' in DP log:"
        printf '        %s%s%s\n' "$C_DIM" "$trip_line" "$C_RESET"

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
        warn "core_circuitBreakers_heap_open=${v:-<missing>} at the moment we sampled (expected 1). The breaker oscillates under sustained load — HTTP probe tally below is the deterministic proof."
    fi

    # Let the gRPC flood finish so we cover the full open window.
    wait "$telem_pid" 2>/dev/null || true

    # Stop the HTTP probe loop and reap it.
    touch "$HTTP_PROBE_STOP"
    wait "$probe_pid" 2>/dev/null || true

    # Assertion 3 (P1 HTTP path): the parallel HTTP probe loop MUST have seen
    # at least one 503 during the flood. The HTTP service is gated by the same
    # server-level decorator that gates the gRPC service; if we got zero 503s,
    # the decorator isn't running on the HTTP path.
    local n_200 n_503 n_other
    read -r n_200 n_503 n_other <<<"$(http_probe_tally "$HTTP_PROBE_LOG")"
    log "HTTP probes during flood: 200=$n_200, 503=$n_503, other=$n_other (file: $HTTP_PROBE_LOG)"
    if (( n_503 > 0 )); then
        pass "HTTP path returned 503 $n_503 time(s) during the open window — P1 HTTP decorator engaged on the logs source's additional HTTP service."
    else
        fail "HTTP path returned 0 instances of 503 across $((n_200 + n_503 + n_other)) probe(s) — the HTTP decorator is NOT rejecting requests on the logs source's HTTP entry point."
    fi
    if (( n_other > 0 )); then
        warn "HTTP probes saw $n_other non-{200,503} response(s); inspect $HTTP_PROBE_LOG for details (could be connection resets under load — usually not fatal)."
    fi

    # Soft check: telemetrygen client-side rejections. OTLP-gRPC retries
    # absorb most errors silently, so this is informational only. The
    # deterministic gRPC-path proof is the trip log above (which only fires
    # when requests are being driven into the breaker).
    local rej
    rej="$(grep -cE 'Circuit breaker is open|code = (Unavailable|ResourceExhausted)|HTTP/.* 503|rpc error|export timeout' "$TRIP_TELEM_LOG" 2>/dev/null || true)"
    rej="${rej:-0}"
    if (( rej > 0 )); then
        pass "telemetrygen surfaced $rej rejection/error line(s) client-side — gRPC path also seeing rejections."
    else
        warn "telemetrygen surfaced 0 client-side errors (OTLP retries absorbed them). The HTTP probe tally above is the deterministic P1 proof."
    fi
}

phase_hysteresis() {
    section "Verifying hysteresis (P4) — breaker must settle below close threshold (${CLOSE_BYTES}B)"
    log "Load stopped. Waiting up to 90s for breaker to settle closed (sink + GC catch up)..."

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
    section "Recovery sanity check — low-rate gRPC + HTTP probe must succeed after settle"
    rm -f "$TELEM_LOG"
    timeout --foreground --kill-after=5s 25s telemetrygen logs \
        --otlp-endpoint "localhost:${OTEL_LOGS_PORT}" --otlp-insecure \
        --duration 10s --rate 20 --workers 2 >"$TELEM_LOG" 2>&1 || true

    sleep 2
    local v1 v2
    v1="$(prom_value core_circuitBreakers_heap_open)"
    sleep 3
    v2="$(prom_value core_circuitBreakers_heap_open)"
    if is_zero "${v1:-1}" && is_zero "${v2:-1}"; then
        pass "Breaker remained closed under post-recovery gRPC baseline (gauge=$v1 → $v2)."
    else
        fail "Breaker reopened under recovery gRPC baseline (gauge=$v1 → $v2)."
    fi

    # The HTTP path must also recover — sending an OTLP-HTTP/JSON probe should
    # now return 200 again (the decorator delegates because the breaker is closed).
    local status; status="$(http_probe_once)"
    if [[ "$status" == "200" ]]; then
        pass "HTTP path returned 200 for OTLP-HTTP/JSON probe post-recovery — decorator is delegating again."
    else
        fail "HTTP path returned '$status' for OTLP-HTTP/JSON probe post-recovery (expected 200)."
    fi
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

