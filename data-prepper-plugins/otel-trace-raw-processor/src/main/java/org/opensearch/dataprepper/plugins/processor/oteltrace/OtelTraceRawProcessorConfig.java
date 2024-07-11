/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.oteltrace;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;

import java.time.Duration;

public class OtelTraceRawProcessorConfig {
    static final long DEFAULT_TG_FLUSH_INTERVAL_SEC = 180L;
    static final Duration DEFAULT_TRACE_ID_TTL = Duration.ofSeconds(15L);
    static final long MAX_TRACE_ID_CACHE_SIZE = 1_000_000L;
    @JsonProperty("trace_flush_interval")
    @JsonPropertyDescription("Represents the time interval in seconds to flush all the descendant spans without any " +
            "root span. Default is 180.")
    private long traceFlushInterval = DEFAULT_TG_FLUSH_INTERVAL_SEC;

    @JsonProperty("trace_group_cache_ttl")
    @JsonPropertyDescription("Represents the time-to-live to cache a trace group details. Default is 15 seconds.")
    private Duration traceGroupCacheTimeToLive = DEFAULT_TRACE_ID_TTL;

    @JsonProperty("trace_group_cache_max_size")
    @JsonPropertyDescription("Represents the maximum size of the cache to store the trace group details from root spans. " +
            "Default is 1000000.")
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
