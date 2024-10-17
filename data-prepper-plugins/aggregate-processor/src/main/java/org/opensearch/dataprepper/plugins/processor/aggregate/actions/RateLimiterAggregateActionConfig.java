/*
 * Copyright OpenSearch Contributors
 * SPDX-Limense-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.constraints.NotNull;

@JsonPropertyOrder
@JsonClassDescription("The <code>rate_limiter</code> action controls the number of events aggregated per second. " +
        "By default, <code>rate_limiter</code> blocks the <code>aggregate</code> processor from running if it receives more events than the configured number allowed. " +
        "You can overwrite the number events that triggers the <code>rate_limited</code> by using the <code>when_exceeds</code> configuration option.")
public class RateLimiterAggregateActionConfig {
    @JsonPropertyDescription("The number of events allowed per second.")
    @JsonProperty("events_per_second")
    @NotNull
    int eventsPerSecond;
    
    @JsonPropertyDescription("Indicates what action the <code>rate_limiter</code> takes when the number of events received is greater than the number of events allowed per second. " +
            "Default value is block, which blocks the processor from running after the maximum number of events allowed per second is reached until the next second. Alternatively, the drop option drops the excess events received in that second. Default is block")
    @JsonProperty(value = "when_exceeds", defaultValue = "block")
    RateLimiterMode whenExceedsMode = RateLimiterMode.BLOCK;
    
    public int getEventsPerSecond() {
        return eventsPerSecond;
    }

    public RateLimiterMode getWhenExceeds() {
        return whenExceedsMode;
    }
}
