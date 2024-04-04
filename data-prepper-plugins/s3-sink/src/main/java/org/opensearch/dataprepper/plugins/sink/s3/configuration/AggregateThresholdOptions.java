/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.opensearch.dataprepper.model.types.ByteCount;


/**
 * An implementation class of s3 index configuration Options
 */
public class AggregateThresholdOptions {

    private static final String DEFAULT_BYTE_CAPACITY = "50mb";

    @JsonProperty("maximum_size")
    private ByteCount maximumSize = ByteCount.parse(DEFAULT_BYTE_CAPACITY);

    /**
     * Controls how aggressive the force flush is when the maximum_size is reached.
     * Groups will be flushed until the total size is less than maximum_size * flush_capacity_ratio
     */
    @JsonProperty("flush_capacity_ratio")
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