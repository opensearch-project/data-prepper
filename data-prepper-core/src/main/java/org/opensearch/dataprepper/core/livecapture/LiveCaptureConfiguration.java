/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.livecapture;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration class for live capture functionality.
 * Maps to the 'live_capture' section in the Data Prepper configuration.
 */
public class LiveCaptureConfiguration {


    private final boolean defaultEnabled;
    private final double defaultRate;
    private final Object liveCaptureOutputSinkConfig;

    public LiveCaptureConfiguration() {
        this.defaultEnabled = false;
        this.defaultRate = 1.0;
        this.liveCaptureOutputSinkConfig = null;
    }

    @JsonCreator
    public LiveCaptureConfiguration(
            @JsonProperty("default_enabled") final Boolean defaultEnabled,
            @JsonProperty("default_rate") final Double defaultRate,
            @JsonProperty("live_capture_out") final Object liveCaptureOutputSinkConfig) {

        this.defaultEnabled = defaultEnabled != null ? defaultEnabled : false;
        double rate = defaultRate != null ? defaultRate : 1.0;
        if (rate <= 0) {
            throw new IllegalArgumentException("default_rate must be positive");
        }
        this.defaultRate = rate;
        this.liveCaptureOutputSinkConfig = liveCaptureOutputSinkConfig;

    }

    public boolean isDefaultEnabled() {
        return defaultEnabled;
    }

    public double getDefaultRate() {
        return defaultRate;
    }

    public Object getLiveCaptureOutputSinkConfig() {
        return liveCaptureOutputSinkConfig;
    }

}