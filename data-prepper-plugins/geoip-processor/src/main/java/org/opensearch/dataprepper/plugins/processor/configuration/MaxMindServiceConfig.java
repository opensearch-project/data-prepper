/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.plugins.processor.loadtype.LoadTypeOptions;

import java.time.Duration;
import java.util.List;

public class MaxMindServiceConfig {

    private static final Duration DEFAULT_CACHE_REFRESH_SCHEDULE = Duration.parse("P15D");

    @JsonProperty("database_path")
    @NotNull
    List<DatabasePathURLConfig> databasePath;

    @JsonProperty("load_type")
    @NotNull
    private LoadTypeOptions loadType;

    @JsonProperty("cache_size")
    private Integer cacheSize;

    @JsonProperty("cache_refresh_schedule")
    @NotNull
    private Duration cacheRefreshSchedule = DEFAULT_CACHE_REFRESH_SCHEDULE;

    /**
     * Get the list of Configured Database path options
     * @return List of DatabasePathURLConfig
     */
    public List<DatabasePathURLConfig> getDatabasePath() {
        return databasePath;
    }

    /**
     * Get the Configured load type either Cache or in memory
     * @return String
     */
    public LoadTypeOptions getLoadType() {
        return loadType;
    }

    /**
     * Get the Configured Cache size
     * @return Integer
     */
    public Integer getCacheSize() {
        return cacheSize;
    }

    /**
     * Get the Configured Cache refresh scheduled Duration
     * @return Duration
     */
    public Duration getCacheRefreshSchedule() {
        return cacheRefreshSchedule;
    }
}