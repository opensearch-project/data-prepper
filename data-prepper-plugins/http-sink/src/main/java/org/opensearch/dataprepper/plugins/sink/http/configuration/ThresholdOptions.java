/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.http.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import org.hibernate.validator.constraints.time.DurationMax;
import org.hibernate.validator.constraints.time.DurationMin;
import org.opensearch.dataprepper.model.types.ByteCount;

import java.time.Duration;

/**
 * An implementation class of http index configuration Options
 */
public class ThresholdOptions {

    private static final String DEFAULT_MAX_REQUEST_SIZE = "50mb";
    private static final int DEFAULT_MAX_EVENTS = 100;
    private static final Duration DEFAULT_FLUSH_TIMEOUT = Duration.ofSeconds(10);

    @JsonProperty("max_events")
    @Size(min = 1, max = 10000000, message = "event_count size should be between 0 and 10000000")
    @NotNull
    private int maxEvents = DEFAULT_MAX_EVENTS;

    @JsonProperty("max_request_size")
    private String maxRequestSize = DEFAULT_MAX_REQUEST_SIZE;

    @JsonProperty("flush_timeout")
    @DurationMin(seconds = 1)
    @DurationMax(seconds = 3600)
    @NotNull
    private Duration flushTimeout = DEFAULT_FLUSH_TIMEOUT;

    /**
     * Read event collection duration configuration.
     * @return  event collect time out.
     */
    public Duration getFlushTimeOut() {
        return flushTimeout;
    }

    /**
     * Read byte capacity configuration.
     * @return maximum byte count.
     */
    public ByteCount getMaxRequestSize() {
        return ByteCount.parse(maxRequestSize);
    }

    /**
     * Read the event count configuration.
     * @return event count.
     */
    public int getMaxEvents() {
        return maxEvents;
    }
}
