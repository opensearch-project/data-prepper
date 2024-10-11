/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.oteltrace;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.time.Duration;

@JsonPropertyOrder
@JsonClassDescription("The <code>otel_traces</code> processor completes trace-group-related fields in all incoming " +
        "span records by state caching the root span information for each <code>traceId</code>.")
public class OtelTraceRawProcessorConfig {
    static final long DEFAULT_TG_FLUSH_INTERVAL_SEC = 180L;
    static final Duration DEFAULT_TRACE_ID_TTL = Duration.ofSeconds(15L);
    static final long MAX_TRACE_ID_CACHE_SIZE = 1_000_000L;

    @JsonProperty("trace_flush_interval")
    @JsonPropertyDescription("Represents the time interval in seconds to flush all the descendant spans without any " +
            "root span. Default is <code>180</code>.")
    private long traceFlushInterval = DEFAULT_TG_FLUSH_INTERVAL_SEC;

    @JsonProperty("trace_group_cache_ttl")
    @JsonPropertyDescription("Represents the time-to-live to cache a trace group details. " +
            "The value may be an ISO 8601 notation such as <code>PT1M30S</code> or a duration and unit such as <code>45s</code>. " +
            "Default is 15 seconds.")
    private Duration traceGroupCacheTimeToLive = DEFAULT_TRACE_ID_TTL;

    @JsonProperty("trace_group_cache_max_size")
    @JsonPropertyDescription("Represents the maximum size of the cache to store the trace group details from root spans. " +
            "Default is <code>1000000</code>.")
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
