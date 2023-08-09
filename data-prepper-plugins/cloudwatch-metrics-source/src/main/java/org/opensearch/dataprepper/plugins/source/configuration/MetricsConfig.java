/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

import java.util.List;

/**
 *  Configuration class to get the Metric info
 */
public class MetricsConfig {
    @JsonProperty("name")
    @NotNull
    private String name;
    @JsonProperty("id")
    @NotNull
    private String id;
    @JsonProperty("period")
    @NotNull
    private Integer period;
    @JsonProperty("stat")
    @NotNull
    private String stat;
    @JsonProperty("unit")
    @NotNull
    private String unit;
    @JsonProperty("dimensions")
    @NotNull
    private List<DimensionsListConfig> dimensionsListConfigs;

    public String getName() {
        return name;
    }
    public String getId() {
        return id;
    }
    public Integer getPeriod() {
        return period;
    }
    public String getStat() {
        return stat;
    }
    public String getUnit() {
        return unit;
    }
    public List<DimensionsListConfig> getDimensionsListConfigs() {
        return dimensionsListConfigs;
    }
}
