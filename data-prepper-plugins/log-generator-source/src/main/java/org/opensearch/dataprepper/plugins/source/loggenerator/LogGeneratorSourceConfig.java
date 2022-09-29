/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.loggenerator;

import org.opensearch.dataprepper.model.configuration.PluginModel;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

import java.time.Duration;

public class LogGeneratorSourceConfig {
    static int DEFAULT_INTERVAL_SECONDS = 5;
    static int INFINITE_LOG_COUNT = 0;

    @JsonProperty("interval")
    private Duration interval = Duration.ofSeconds(DEFAULT_INTERVAL_SECONDS);

    @JsonProperty("total_log_count")
    private int count = INFINITE_LOG_COUNT; // default of 0 => will generate logs until call to stop()

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
