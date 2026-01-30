/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Enhanced leader progress state that tracks last partition creation time
 * per dimension type, enabling configurable partition creation intervals.
 */
public class ConfigurableIntervalLeaderProgressState extends DimensionalTimeSliceLeaderProgressState {

    /**
     * Map tracking the last partition creation time for each dimension type.
     * This allows each dimension type to have independent partition creation schedules.
     */
    @JsonProperty("lastPartitionTimePerDimensionType")
    private Map<String, Instant> lastPartitionTimePerDimensionType;

    /**
     * Default constructor for Jackson deserialization.
     */
    public ConfigurableIntervalLeaderProgressState() {
        super(Instant.now(), 0);
        this.lastPartitionTimePerDimensionType = new HashMap<>();
    }

    /**
     * Constructor with initial values.
     *
     * @param lastPollTime   the last poll time for general progress tracking
     * @param remainingHours the number of remaining hours for historical data processing
     */
    public ConfigurableIntervalLeaderProgressState(Instant lastPollTime, int remainingHours) {
        super(lastPollTime, remainingHours);
        this.lastPartitionTimePerDimensionType = new HashMap<>();
    }

    /**
     * Gets the map of last partition creation times per dimension type.
     *
     * @return Map of dimension type to last partition creation time
     */
    public Map<String, Instant> getLastPartitionTimePerDimensionType() {
        if (lastPartitionTimePerDimensionType == null) {
            lastPartitionTimePerDimensionType = new HashMap<>();
        }
        return lastPartitionTimePerDimensionType;
    }

    /**
     * Sets the map of last partition creation times per dimension type.
     *
     * @param lastPartitionTimePerDimensionType Map of dimension type to last partition creation time
     */
    public void setLastPartitionTimePerDimensionType(Map<String, Instant> lastPartitionTimePerDimensionType) {
        this.lastPartitionTimePerDimensionType = lastPartitionTimePerDimensionType;
    }

    /**
     * Gets the last partition creation time for a specific dimension type.
     *
     * @param dimensionType the dimension type
     * @return the last partition creation time, or the global last poll time if not tracked
     */
    public Instant getLastPartitionTimeForDimension(String dimensionType) {
        return getLastPartitionTimePerDimensionType().getOrDefault(dimensionType, getLastPollTime());
    }

    /**
     * Sets the last partition creation time for a specific dimension type.
     *
     * @param dimensionType the dimension type
     * @param partitionTime the last partition creation time
     */
    public void setLastPartitionTimeForDimension(String dimensionType, Instant partitionTime) {
        getLastPartitionTimePerDimensionType().put(dimensionType, partitionTime);
    }

    @Override
    public String toString() {
        return "ConfigurableIntervalLeaderProgressState{" +
                "lastPartitionTimePerDimensionType=" + lastPartitionTimePerDimensionType +
                ", lastPollTime=" + getLastPollTime() +
                ", remainingHours=" + getRemainingHours() +
                '}';
    }
}
