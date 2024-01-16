/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

/**
 *  Configuration  class to get the Dimensions of the Metric
 */
public class DimensionConfig {
    @JsonProperty("name")
    @NotNull
    private String name;
    @JsonProperty("value")
    @NotNull
    private String value;

    /**
     * Get the Dimension Name
     * @return Dimension Name
     */
    public String getName() {
        return name;
    }

    /**
     * Get the Dimension Value
     * @return Dimension Value
     */
    public String getValue() {
        return value;
    }
}
