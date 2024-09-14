/*
 * Copyright OpenSearch Contributors
 * SPDX-Limense-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import jakarta.validation.constraints.NotNull;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import jakarta.validation.constraints.AssertTrue;

public class PercentSamplerAggregateActionConfig {
    @JsonPropertyDescription("The percentage of events to be processed during a one second interval. Must be greater than 0.0 and less than 100.0")
    @JsonProperty("percent")
    @NotNull
    private double percent;
    
    @AssertTrue(message = "Percent value must be greater than 0.0 and less than 100.0")
    boolean isPercentValid() {
        return percent > 0.0 && percent < 100.0;
    }

    public double getPercent() {
        return percent;
    }
}
