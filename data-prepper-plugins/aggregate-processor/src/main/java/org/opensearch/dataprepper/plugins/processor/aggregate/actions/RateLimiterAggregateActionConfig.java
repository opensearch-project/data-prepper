/*
 * Copyright OpenSearch Contributors
 * SPDX-Limense-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import java.util.Set;
import java.util.HashSet;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

public class RateLimiterAggregateActionConfig {
    public static final Set<String> validRateLimiterModes = new HashSet<>(Set.of(RateLimiterMode.BLOCK.toString(), RateLimiterMode.DROP.toString()));

    @JsonProperty("events_per_second")
    @NotNull
    int eventsPerSecond;
    
    @JsonProperty("when_exceeds")
    String whenExceedsMode = RateLimiterMode.BLOCK.toString();
    
    public int getEventsPerSecond() {
        return eventsPerSecond;
    }

    public String getWhenExceeds() {
        if (!validRateLimiterModes.contains(whenExceedsMode)) {
            throw new IllegalArgumentException("Unknown rate limiter mode " + whenExceedsMode);
        }
        return whenExceedsMode;
    }
}
