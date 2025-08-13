/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.livecapture;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

/**
 * Configuration class for live capture functionality.
 * Maps to the 'live_capture' section in the Data Prepper configuration.
 */
public class LiveCaptureConfiguration {

    private final boolean defaultEnabled;
    private final double defaultRate;
    private final List<Object> sinkConfigurations;

    public LiveCaptureConfiguration() {
        this.defaultEnabled = false;
        this.defaultRate = 1.0;
        this.sinkConfigurations = Collections.emptyList();
    }

    @JsonCreator
    public LiveCaptureConfiguration(
            @JsonProperty("default_enabled") final Boolean defaultEnabled,
            @JsonProperty("default_rate") final Double defaultRate,
            @JsonProperty("sink") final List<Object> sinkConfigurations) {

        this.defaultEnabled = defaultEnabled != null ? defaultEnabled : false;

        final double rate = defaultRate != null ? defaultRate : 1.0;
        if (rate <= 0) {
            throw new IllegalArgumentException("default_rate must be positive");
        }
        this.defaultRate = rate;
        this.sinkConfigurations = sinkConfigurations != null ? sinkConfigurations : Collections.emptyList();
        // Validate exactly 1 sink is configured for now
        if (this.sinkConfigurations.size() > 1) {
            throw new IllegalArgumentException("Only one sink configuration is currently supported. Found: " + this.sinkConfigurations.size());
        }
    }

    public boolean isDefaultEnabled() {
        return defaultEnabled;
    }

    public double getDefaultRate() {
        return defaultRate;
    }

    public List<Object> getSinkConfigurations() {
        return sinkConfigurations;
    }
    
    /**
     * Gets the first (and currently only allowed) sink configuration.
     * 
     * @return the sink configuration, or null if none configured
     */
    public Object getFirstSinkConfiguration() {
        return sinkConfigurations.isEmpty() ? null : sinkConfigurations.get(0);
    }

}