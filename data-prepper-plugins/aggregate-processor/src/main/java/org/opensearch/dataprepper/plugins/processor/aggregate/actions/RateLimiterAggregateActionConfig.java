/*
 * Copyright OpenSearch Contributors
 * SPDX-Limense-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

public class RateLimiterAggregateActionConfig {
    @JsonProperty("events_per_second")
    @NotNull
    int eventsPerSecond;
    
    @JsonProperty("drop_when_exceeds")
    boolean dropWhenExceeds = false;
    
    public int getEventsPerSecond() {
        return eventsPerSecond;
    }

    public boolean getDropWhenExceeds() {
        return dropWhenExceeds;
    }
}
