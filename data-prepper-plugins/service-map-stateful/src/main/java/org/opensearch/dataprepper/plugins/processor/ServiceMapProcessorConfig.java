/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder
@JsonClassDescription("The <code>service_map</code> processor uses OpenTelemetry data to create a distributed service map for " +
        "visualization in OpenSearch Dashboards.")
public class ServiceMapProcessorConfig {
    private static final String WINDOW_DURATION = "window_duration";
    static final int DEFAULT_WINDOW_DURATION = 180;
    static final String DEFAULT_DB_PATH = "data/service-map/";

    @JsonProperty(WINDOW_DURATION)
    @JsonPropertyDescription("Represents the fixed time window, in seconds, " +
            "during which service map relationships are evaluated. Default value is <code>180</code>.")
    private int windowDuration = DEFAULT_WINDOW_DURATION;

    public int getWindowDuration() {
        return windowDuration;
    }
}
