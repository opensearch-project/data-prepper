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

    @JsonProperty("trace_group_cache_ttl")
    private Duration traceGroupCacheTimeToLive = DEFAULT_TRACE_ID_TTL;

    @JsonProperty("trace_group_cache_max_size")
    private long traceGroupCacheMaxSize = MAX_TRACE_ID_CACHE_SIZE;

    public long getTraceFlushIntervalSeconds() {
        return traceFlushInterval;
    }

    public Duration getTraceGroupCacheTimeToLive() {
        return traceGroupCacheTimeToLive;
    }

    public long getTraceGroupCacheMaxSize() {
        return traceGroupCacheMaxSize;
    }
}
