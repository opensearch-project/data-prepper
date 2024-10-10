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
import jakarta.validation.constraints.AssertTrue;

@JsonPropertyOrder
@JsonClassDescription("The <code>percent_sampler</code> action controls the number of events aggregated based " +
        "on a percentage of events. The action drops any events not included in the percentage.")
public class PercentSamplerAggregateActionConfig {
    @JsonPropertyDescription("The percentage of events to be processed during a one second interval. Must be greater than 0.0 and less than 100.0.")
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
