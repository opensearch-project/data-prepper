/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.otel_apm_service_map;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.constraints.NotEmpty;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

@JsonPropertyOrder
@JsonClassDescription("The <code>otel_apm_service_map</code> processor uses OpenTelemetry data to create APM service map " +
        "relationships for visualization, generating ServiceDetails and ServiceRemoteDetails events.")
public class OTelApmServiceMapProcessorConfig {
    static final int DEFAULT_WINDOW_DURATION_SECONDS = 60;
    static final String DEFAULT_DB_PATH = "data/otel-apm-service-map/";

    @JsonProperty("window_duration")
    @JsonPropertyDescription("Represents the fixed time window during which APM service map relationships are evaluated. " +
            "Supports ISO-8601 duration format (e.g., PT60S, PT1M) or simple integer values (interpreted as seconds).")
    private Duration windowDuration = Duration.ofSeconds(DEFAULT_WINDOW_DURATION_SECONDS);

    @NotEmpty
    @JsonProperty(value = "db_path", defaultValue = DEFAULT_DB_PATH)
    @JsonPropertyDescription("Represents folder path for creating database files storing transient data off heap memory" +
            "when processing APM service-map data.")
    private String dbPath = DEFAULT_DB_PATH;

    @JsonProperty("group_by_attributes")
    @JsonPropertyDescription("List of OTEL resource attribute names that should be copied into Service.groupByAttributes " +
            "when present on the span's resource attributes. Only applied to primary Service objects, not dependency services.")
    private List<String> groupByAttributes = Collections.emptyList();

    public Duration getWindowDuration() {
        return windowDuration;
    }

    public String getDbPath() {
        return dbPath;
    }

    public List<String> getGroupByAttributes() {
        return groupByAttributes != null ? Collections.unmodifiableList(groupByAttributes) : Collections.emptyList();
    }
}
