/*
 * Copyright OpenSearch Contributors
 * SPDX-Limense-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.AssertTrue;

import java.time.Duration;

public class TailSamplerAggregateActionConfig {
    @JsonProperty("wait_period")
    @NotNull
    private Duration waitPeriod;
    
    @JsonProperty("percent")
    @NotNull
    private double percent;
    
    @JsonProperty("error_condition")
    private String errorCondition;

    @AssertTrue(message = "Percent value must be greater than 0.0 and less than 100.0")
    boolean isPercentValid() {
        return percent > 0.0 && percent < 100.0;
    }

    public double getPercent() {
        return percent;
    }
    
    @AssertTrue(message = "Wait period value must be greater than 0 and less than 600")
    boolean isWaitPeriodValid() {
        return waitPeriod.getSeconds() > 0 && waitPeriod.getSeconds() <= 600;
    }

    public Duration getWaitPeriod() {
        return waitPeriod;
    }

    public String getErrorCondition() {
        return errorCondition;
    }
}
