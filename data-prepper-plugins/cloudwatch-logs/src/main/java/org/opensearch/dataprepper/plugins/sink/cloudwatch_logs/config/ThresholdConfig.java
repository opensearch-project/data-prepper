/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Size;
import org.hibernate.validator.constraints.time.DurationMax;
import org.hibernate.validator.constraints.time.DurationMin;
import org.opensearch.dataprepper.model.types.ByteCount;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;

/**
 * The threshold config holds the different configurations for
 * buffer restrictions, retransmission restrictions and timeout
 * restrictions.
 */
public class ThresholdConfig {
    public static final int DEFAULT_BATCH_SIZE = 25;
    public static final String DEFAULT_EVENT_SIZE = "256kb";
    public static  final String DEFAULT_SIZE_OF_REQUEST = "1mb";
    public static final int DEFAULT_RETRY_COUNT = 5;
    public static final long DEFAULT_LOG_SEND_INTERVAL_TIME = 60;
    public static final long DEFAULT_BACKOFF_TIME = 500;

    @JsonProperty("batch_size")
    @Size(min = 1, max = 10000, message = "batch_size amount should be between 1 to 10000")
    private int batchSize = DEFAULT_BATCH_SIZE;

    @JsonProperty("max_event_size")
    @Size(min = 1, max = 256, message = "max_event_size amount should be between 1 to 256 kilobytes")
    private String maxEventSize = DEFAULT_EVENT_SIZE;

    @JsonProperty("max_request_size")
    private String maxRequestSize = DEFAULT_SIZE_OF_REQUEST;

    @JsonProperty("retry_count")
    @Size(min = 1, max = 15, message = "retry_count amount should be between 1 and 15")
    private int retryCount = DEFAULT_RETRY_COUNT;

    @JsonProperty("log_send_interval")
    @DurationMin(seconds = 60)
    @DurationMax(seconds = 3600)
    private Duration logSendInterval = Duration.ofSeconds(DEFAULT_LOG_SEND_INTERVAL_TIME);

    @JsonProperty("back_off_time")
    @DurationMin(millis = 500)
    @DurationMax(millis = 1000)
    private Duration backOffTime = Duration.ofMillis(DEFAULT_BACKOFF_TIME);

    public int getBatchSize() {
        return batchSize;
    }

    public long getMaxEventSizeBytes() {
        return ByteCount.parse(maxEventSize).getBytes();
    }

    public long getMaxRequestSizeBytes() {
        return ByteCount.parse(maxRequestSize).getBytes();
    }

    public int getRetryCount() {
        return retryCount;
    }

    public long getLogSendInterval() {
        return logSendInterval.getSeconds();
    }

    public long getBackOffTime() {
        return (backOffTime.get(ChronoUnit.NANOS) / 1000000) + (backOffTime.getSeconds() * 1000);
    }
}