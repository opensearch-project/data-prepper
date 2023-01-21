/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.oteltrace;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Duration;

public class OtelTraceRawProcessorConfig {
    static final long DEFAULT_TG_FLUSH_INTERVAL_SEC = 180L;
    static final Duration DEFAULT_TRACE_ID_TTL = Duration.ofSeconds(15L);
    static final long MAX_TRACE_ID_CACHE_SIZE = 1_000_000L;
    @JsonProperty("trace_flush_interval")
    private long traceFlushInterval = DEFAULT_TG_FLUSH_INTERVAL_SEC;

    @JsonProperty("trace_id_ttl")
    private Duration traceIdTimeToLive = DEFAULT_TRACE_ID_TTL;

    @JsonProperty("max_trace_id_cache_size")
    private long maxTraceIdCacheSize = MAX_TRACE_ID_CACHE_SIZE;

    public long getTraceFlushIntervalSeconds() {
        return traceFlushInterval;
    }

    public Duration getTraceIdTimeToLive() {
        return traceIdTimeToLive;
    }

    public long getMaxTraceIdCacheSize() {
        return maxTraceIdCacheSize;
    }
}
