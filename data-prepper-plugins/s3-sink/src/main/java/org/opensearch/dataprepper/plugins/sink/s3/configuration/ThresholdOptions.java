/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.configuration;

import java.time.Duration;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import org.hibernate.validator.constraints.time.DurationMax;
import org.hibernate.validator.constraints.time.DurationMin;
import org.opensearch.dataprepper.model.types.ByteCount;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * An implementation class of s3 index configuration Options
 */
public class ThresholdOptions {

    private static final String DEFAULT_BYTE_CAPACITY = "50mb";

    @JsonProperty("event_count")
    @Min(value = 0, message = "event_count size should be between 0 and 10000000")
    @Max(value = 10000000, message = "event_count size should be between 0 and 10000000")
    private int eventCount;

    @JsonProperty("maximum_size")
    private ByteCount maximumSize = ByteCount.parse(DEFAULT_BYTE_CAPACITY);

    @JsonProperty("event_collect_timeout")
    @DurationMin(seconds = 1)
    @DurationMax(seconds = 3600)
    @NotNull
    private Duration eventCollectTimeOut;

    /**
     * Read event collection duration configuration.
     * @return  event collect time out.
     */
    public Duration getEventCollectTimeOut() {
        return eventCollectTimeOut;
    }

    /**
     * Read byte capacity configuration.
     * @return maximum byte count.
     */
    public ByteCount getMaximumSize() {
        return maximumSize;
    }

    /**
     * Read the event count configuration.
     * @return event count.
     */
    public int getEventCount() {
        return eventCount;
    }
}