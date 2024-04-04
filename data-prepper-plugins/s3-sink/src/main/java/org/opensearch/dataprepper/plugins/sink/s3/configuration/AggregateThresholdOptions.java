/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.model.types.ByteCount;


/**
 * An implementation class of s3 index configuration Options
 */
public class AggregateThresholdOptions {

    @JsonProperty("maximum_size")
    @NotNull
    private ByteCount maximumSize;

    /**
     * Controls how aggressive the force flush is when the maximum_size is reached.
     * Groups will be flushed until the total size is less than maximum_size * flush_capacity_ratio
     */
    @JsonProperty("flush_capacity_ratio")
    @Min(value = 0, message = "flush_capacity_ratio must be between 0.0 and 1.0")
    @Max(value = 1, message = "flush_capacity_ratio must be between 0.0 and 1.0")
    private double flushCapacityRatio = 0.5;

    /**
     * Read byte capacity configuration.
     * @return maximum byte count.
     */
    public ByteCount getMaximumSize() {
        return maximumSize;
    }

    public double getFlushCapacityRatio() { return flushCapacityRatio; }
}