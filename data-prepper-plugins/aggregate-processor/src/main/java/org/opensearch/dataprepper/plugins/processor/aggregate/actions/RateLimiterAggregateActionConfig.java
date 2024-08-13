/*
 * Copyright OpenSearch Contributors
 * SPDX-Limense-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import java.util.Set;
import java.util.HashSet;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import jakarta.validation.constraints.NotNull;

public class RateLimiterAggregateActionConfig {
    public static final Set<String> validRateLimiterModes = new HashSet<>(Set.of(RateLimiterMode.BLOCK.toString(), RateLimiterMode.DROP.toString()));

    @JsonPropertyDescription("The number of events allowed per second.")
    @JsonProperty("events_per_second")
    @NotNull
    int eventsPerSecond;
    
    @JsonPropertyDescription("Indicates what action the rate_limiter takes when the number of events received is greater than the number of events allowed per second. Default value is block, which blocks the processor from running after the maximum number of events allowed per second is reached until the next second. Alternatively, the drop option drops the excess events received in that second. Default is block")
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
