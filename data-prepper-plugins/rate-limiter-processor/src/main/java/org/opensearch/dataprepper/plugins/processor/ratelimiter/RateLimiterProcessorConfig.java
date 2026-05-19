/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.ratelimiter;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.constraints.NotNull;

@JsonPropertyOrder
@JsonClassDescription("The <code>rate_limiter</code> processor controls the number of events processed per second. " +
        "By default, <code>rate_limiter</code> drops events that exceed the configured number allowed per second.")
public class RateLimiterProcessorConfig {

    @JsonPropertyDescription("The number of events allowed per second.")
    @JsonProperty("events_per_second")
    @NotNull
    private int eventsPerSecond;

    @JsonPropertyDescription("Indicates what action the <code>rate_limiter</code> takes when the number of events received is greater than the number of events allowed per second. " +
            "Default value is drop, which drops the excess events received in that second.")
    @JsonProperty(value = "when_exceeds", defaultValue = "drop")
    private RateLimiterMode whenExceeds = RateLimiterMode.DROP;

    @JsonPropertyDescription("The duration in seconds to track per-second event counts. " +
            "Counters older than this are discarded to free resources. Default is 60.")
    @JsonProperty(value = "counter_retention_seconds", defaultValue = "60")
    private int counterRetentionSeconds = 60;

    public int getEventsPerSecond() {
        return eventsPerSecond;
    }

    public RateLimiterMode getWhenExceeds() {
        return whenExceeds;
    }

    public int getCounterRetentionSeconds() {
        return counterRetentionSeconds;
    }
}
