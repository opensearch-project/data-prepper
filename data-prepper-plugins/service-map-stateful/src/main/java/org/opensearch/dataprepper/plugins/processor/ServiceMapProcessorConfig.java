/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.constraints.NotEmpty;

@JsonPropertyOrder
@JsonClassDescription("The <code>service_map</code> processor uses OpenTelemetry data to create a distributed service map for " +
        "visualization in OpenSearch Dashboards.")
public class ServiceMapProcessorConfig {
    private static final String WINDOW_DURATION = "window_duration";
    static final int DEFAULT_WINDOW_DURATION = 180;
    static final String DEFAULT_DB_PATH = "data/service-map/";
    static final String DB_PATH = "db_path";

    @JsonProperty(WINDOW_DURATION)
    @JsonPropertyDescription("Represents the fixed time window, in seconds, " +
            "during which service map relationships are evaluated. Default value is <code>180</code>.")
    private int windowDuration = DEFAULT_WINDOW_DURATION;

    @NotEmpty
    @JsonProperty(DB_PATH)
    @JsonPropertyDescription("Represents folder path for database storing transient data off-heap " +
            "when processing service-map data. Default value is <code>data/service-map/</code>")
    private String dbPath = DEFAULT_DB_PATH;

    public int getWindowDuration() {
        return windowDuration;
    }

    public String getDbPath() {
        return dbPath;
    }
}
