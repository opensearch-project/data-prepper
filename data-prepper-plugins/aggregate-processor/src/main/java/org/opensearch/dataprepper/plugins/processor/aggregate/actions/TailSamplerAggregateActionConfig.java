/*
 * Copyright OpenSearch Contributors
 * SPDX-Limense-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.AssertTrue;

import java.time.Duration;

public class TailSamplerAggregateActionConfig {
    @JsonPropertyDescription("period to wait before considering that a trace event is complete")
    @JsonProperty("wait_period")
    @NotNull
    private Duration waitPeriod;
    
    @JsonPropertyDescription("percent value to use for sampling non error events.")
    @JsonProperty("percent")
    @NotNull
    private Integer percent;
    
    @JsonPropertyDescription("condition that determines if an event is error event or not")
    @JsonProperty("condition")
    private String condition;

    @AssertTrue(message = "Percent value must be greater than 0.0 and less than 100.0")
    boolean isPercentValid() {
        return percent > 0.0 && percent < 100.0;
    }

    public Integer getPercent() {
        return percent;
    }
    
    @AssertTrue(message = "Wait period value must be greater than 0 and less than 60")
    boolean isWaitPeriodValid() {
        return waitPeriod.getSeconds() > 0 && waitPeriod.getSeconds() <= 60;
    }

    public Duration getWaitPeriod() {
        return waitPeriod;
    }

    public String getCondition() {
        return condition;
    }
}
