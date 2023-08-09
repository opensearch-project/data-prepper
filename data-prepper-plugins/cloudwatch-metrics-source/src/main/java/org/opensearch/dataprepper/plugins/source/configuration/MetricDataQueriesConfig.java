/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

/**
 *  Configuration class to get the Metric Data Queries
 */
public class MetricDataQueriesConfig {
    @JsonProperty("metric")
    @NotNull
    MetricsConfig metricsConfig;

    /**
     * Get Metric Data Queries Configuration
     * @return Data Queries Configuration
     */
    public MetricsConfig getMetricsConfig() {
        return metricsConfig;
    }
}
