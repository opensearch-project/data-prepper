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

import java.util.Collections;
import java.util.List;

@JsonPropertyOrder
@JsonClassDescription("The <code>otel_apm_service_map</code> processor uses OpenTelemetry data to create APM service map " +
        "relationships for visualization, generating ServiceDetails and ServiceRemoteDetails events.")
public class OtelApmServiceMapProcessorConfig {
    private static final String WINDOW_DURATION = "window_duration";
    static final int DEFAULT_WINDOW_DURATION = 60;
    static final String DEFAULT_DB_PATH = "data/otel-apm-service-map/";
    static final String DB_PATH = "db_path";
    private static final String GROUP_BY_ATTRIBUTES = "group_by_attributes";

    @JsonProperty(value = WINDOW_DURATION, defaultValue = "" + DEFAULT_WINDOW_DURATION)
    @JsonPropertyDescription("Represents the fixed time window, in seconds, " +
            "during which APM service map relationships are evaluated.")
    private int windowDuration = DEFAULT_WINDOW_DURATION;

    @NotEmpty
    @JsonProperty(value = DB_PATH, defaultValue = DEFAULT_DB_PATH)
    @JsonPropertyDescription("Represents folder path for creating database files storing transient data off heap memory" +
            "when processing APM service-map data.")
    private String dbPath = DEFAULT_DB_PATH;

    @JsonProperty(value = GROUP_BY_ATTRIBUTES)
    @JsonPropertyDescription("List of OTEL resource attribute names that should be copied into Service.groupByAttributes " +
            "when present on the span's resource attributes. Only applied to primary Service objects, not dependency services.")
    private List<String> groupByAttributes = Collections.emptyList();

    public int getWindowDuration() {
        return windowDuration;
    }

    public String getDbPath() {
        return dbPath;
    }

    public List<String> getGroupByAttributes() {
        return groupByAttributes != null ? Collections.unmodifiableList(groupByAttributes) : Collections.emptyList();
    }
}
