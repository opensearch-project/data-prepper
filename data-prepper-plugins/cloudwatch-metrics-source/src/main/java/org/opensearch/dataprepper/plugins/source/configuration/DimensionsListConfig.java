/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

/**
 *  Configuration  class to get the List of Dimensions for the Metrics
 */
public class DimensionsListConfig {
    @JsonProperty("dimension")
    @NotNull
    private DimensionConfig dimensionConfig;

    /**
     * Get the List of Dimensions for the Metrics
     * @return metrics list
     */
    public DimensionConfig getDimensionConfig() {
        return dimensionConfig;
    }
}
