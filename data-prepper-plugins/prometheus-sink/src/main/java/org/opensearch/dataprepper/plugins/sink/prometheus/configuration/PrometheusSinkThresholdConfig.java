/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.prometheus.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Size;
import org.hibernate.validator.constraints.time.DurationMax;
import org.hibernate.validator.constraints.time.DurationMin;
import org.opensearch.dataprepper.model.types.ByteCount;

import java.time.Duration;

/**
 * The threshold config holds the different configurations for
 * buffer restrictions, retransmission restrictions and timeout
 * restrictions.
 */
public class PrometheusSinkThresholdConfig {
    public static final int DEFAULT_MAX_EVENTS = 1000;
    public static final String DEFAULT_MAX_REQUEST_SIZE = "1mb";
    public static final long DEFAULT_FLUSH_INTERVAL_SECONDS = 10;

    @JsonProperty("max_events")
    @Size(min = 1, max = 10000, message = "max_events should be between 1 to 10000")
    private int maxEvents = DEFAULT_MAX_EVENTS;

    @JsonProperty("max_request_size")
    private String maxRequestSize = DEFAULT_MAX_REQUEST_SIZE;

    @JsonProperty("flush_interval")
    @DurationMin(seconds = 1)
    @DurationMax(seconds = 60)
    private Duration flushInterval = Duration.ofSeconds(DEFAULT_FLUSH_INTERVAL_SECONDS);

    public int getMaxEvents() {
        return maxEvents;
    }

    public long getMaxRequestSizeBytes() {
        return ByteCount.parse(maxRequestSize).getBytes();
    }

    public long getFlushInterval() {
        return flushInterval.getSeconds();
    }

    public long getFlushIntervalMs() {
        return flushInterval.toMillis();
    }
}
