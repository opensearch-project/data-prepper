/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Size;

/**
 * The threshold config holds the different configurations for
 * buffer restrictions, retransmission restrictions and timeout
 * restrictions.
 */
public class ThresholdConfig {
    public static final int DEFAULT_BATCH_SIZE = 100;
    public static final int DEFAULT_EVENT_SIZE = 50;
    public static final int DEFAULT_SIZE_OF_REQUEST = 524288;
    public static final int DEFAULT_RETRY_COUNT = 5;
    public static final int DEFAULT_LOG_SEND_INTERVAL_TIME = 60;
    public static final int DEFAULT_BACKOFF_TIME = 500;

    @JsonProperty("batch_size")
    @Size(min = 1, max = 10000, message = "batch_size amount should be between 1 to 10000")
    private int batchSize = DEFAULT_BATCH_SIZE;

    @JsonProperty("max_event_size")
    @Size(min = 1, max = 256, message = "max_event_size amount should be between 1 to 256 kilobytes")
    private int maxEventSize = DEFAULT_EVENT_SIZE;

    @JsonProperty("max_request_size")
    @Size(min = 1, max = 1048576, message = "max_batch_request_size amount should be between 1 and 1048576 bytes")
    private int maxRequestSize = DEFAULT_SIZE_OF_REQUEST;

    @JsonProperty("retry_count")
    @Size(min = 1, max = 15, message = "retry_count amount should be between 1 and 15")
    private int retryCount = DEFAULT_RETRY_COUNT;

    @JsonProperty("log_send_interval")
    @Size(min = 5, max = 300, message = "log_send_interval amount should be between 5 and 300 seconds")
    private int logSendInterval = DEFAULT_LOG_SEND_INTERVAL_TIME;

    @JsonProperty("back_off_time")
    @Size(min = 500, max = 1000, message = "back_off_time amount should be between 500 and 1000 milliseconds")
    private int backOffTime = DEFAULT_BACKOFF_TIME;

    public int getBatchSize() {
        return batchSize;
    }

    public int getMaxEventSize() {
        return maxEventSize;
    }

    public int getMaxRequestSize() {
        return maxRequestSize;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public int getLogSendInterval() {
        return logSendInterval;
    }

    public int getBackOffTime() {
        return backOffTime;
    }
}
