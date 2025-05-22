/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Size;
import org.hibernate.validator.constraints.time.DurationMax;
import org.hibernate.validator.constraints.time.DurationMin;
import org.opensearch.dataprepper.model.types.ByteCount;

import java.time.Duration;

/**
 * The threshold config holds the different configurations for
 * buffer restrictions, retransmission restrictions and timeout
 * restrictions.
 */
public class ThresholdConfig {
    public static final int DEFAULT_BATCH_SIZE = 25;
    public static final String DEFAULT_EVENT_SIZE = "256kb";
    public static  final String DEFAULT_SIZE_OF_REQUEST = "1mb";
    public static final long DEFAULT_FLUSH_INTERVAL = 60;

    @JsonProperty(value = "batch_size", defaultValue="25")
    @Min(1)
    @Max(10000)
    private int batchSize = DEFAULT_BATCH_SIZE;

    @JsonProperty(value = "max_event_size", defaultValue="256k")
    @Size(min = 1, max = 256, message = "max_event_size amount should be between 1 to 256 kilobytes")
    private String maxEventSize = DEFAULT_EVENT_SIZE;

    @JsonProperty(value = "max_request_size", defaultValue="1mb")
    private String maxRequestSize = DEFAULT_SIZE_OF_REQUEST;

    @JsonProperty("flush_interval")
    @DurationMin(seconds = 60)
    @DurationMax(seconds = 3600)
    private Duration flushInterval = Duration.ofSeconds(DEFAULT_FLUSH_INTERVAL);

    public int getBatchSize() {
        return batchSize;
    }

    public long getMaxEventSizeBytes() {
        return ByteCount.parse(maxEventSize).getBytes();
    }

    public long getMaxRequestSizeBytes() {
        return ByteCount.parse(maxRequestSize).getBytes();
    }

    public long getFlushInterval() {
        return flushInterval.getSeconds();
    }

}
