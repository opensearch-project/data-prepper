/*
 * Copyright OpenSearch Contributors
 * SPDX-Limense-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

public class PercentSamplerAggregateActionConfig {
    @JsonProperty("percent")
    @NotNull
    double percent;
    
    public double getPercent() {
        if (percent <= 0.0 || percent >= 100.0) {
            throw new IllegalArgumentException("Invalid percent value. percent value must be greater than 0.0 and less than 100.0");
        }
        return percent;
    }
}
