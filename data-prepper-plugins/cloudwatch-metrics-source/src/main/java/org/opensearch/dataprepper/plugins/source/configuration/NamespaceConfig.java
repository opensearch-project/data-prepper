/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

import java.util.List;

/**
 *  Configuration class to get the Namespace info
 */
public class NamespaceConfig {

    @JsonProperty("name")
    @NotNull
    private String name;
    @JsonProperty("start_time")
    @NotNull
    private String startTime;
    @JsonProperty("end_time")
    @NotNull
    private String endTime;
    @JsonProperty("metricDataQueries")
    @NotNull
    private List<MetricDataQueriesConfig> metricDataQueriesConfig;
    @JsonProperty("metric_names")
    @NotNull
    private List<String> metricNames;

    /**
     * Get Metric Name
     * @return Metric Name
     */
    public String getName() {
        return name;
    }
    /**
     * Get Start Time
     * @return Time
     */
    public String getStartTime() {
        return startTime;
    }

    /**
     * Get End Time
     * @return Time
     */
    public String getEndTime() {
        return endTime;
    }

    /**
     * Get List of Metric Data Queries Configuration
     * @return Metric Data Queries Configuration
     */
    public List<MetricDataQueriesConfig> getMetricDataQueriesConfig() {
        return metricDataQueriesConfig;
    }

    /**
     * Get List of metrics
     * @return List of metrics
     */
    public List<String> getMetricNames() {
        return metricNames;
    }
}