/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.loggenerator;

import com.amazon.dataprepper.model.configuration.PluginModel;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

import java.time.Duration;

public class LogGeneratorSourceConfig {
    static int DEFAULT_INTERVAL_SECONDS = 5;
    static int DEFAULT_LOG_COUNT = 0;

    @JsonProperty("interval")
    private Duration interval = Duration.ofSeconds(DEFAULT_INTERVAL_SECONDS);

    @JsonProperty("total_log_count")
    private int count = DEFAULT_LOG_COUNT;

    @JsonProperty("log_type")
    @NotNull
    private PluginModel logType;

    public Duration getInterval() {
        return interval;
    }

    public int getCount() {
        return count;
    }

    public PluginModel getLogType() {
        return logType;
    }
}
