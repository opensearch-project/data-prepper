package org.opensearch.dataprepper.plugins.sink.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Size;

/**
 * The threshold config holds the different configurations for
 * buffer restrictions, retransmission restrictions and timeout
 * restrictions.
 */
public class ThresholdConfig {
    public static final int DEFAULT_BATCH_SIZE = 10;
    public static final int DEFAULT_EVENT_SIZE = 50;
    public static final int DEFAULT_NUMBER_OF_EVENTS = 10;
    public static final int DEFAULT_RETRY_COUNT = 5;
    public static final int DEFAULT_BACKOFF_TIME = 1000;

    @JsonProperty("batch_size")
    @Size(min = 1, max = 10000, message = "batch_size amount should be between 1 to 10000")
    private int batchSize = DEFAULT_BATCH_SIZE;

    @JsonProperty("max_event_size")
    @Size(min = 1, max = 256, message = "max_event_size amount should be between 1 to 256 bytes")
    private int maxEventSize = DEFAULT_EVENT_SIZE;

    @JsonProperty("max_batch_request_size")
    @Size(min = 1, max = 1048576, message = "max_batch_request_size amount should be between 1 and 1048576 bytes")
    private int maxEvents = DEFAULT_NUMBER_OF_EVENTS;

    @JsonProperty("retry_count")
    @Size(min = 1, max = 15, message = "retry_count amount should be between 1 and 15")
    private int retryCount = DEFAULT_RETRY_COUNT;

    @JsonProperty("backoff_time")
    @Size(min = 0, max = 20000, message = "backoff_time amount should be between 0 and 20000 milliseconds")
    private int backOffTime = DEFAULT_BACKOFF_TIME;

    public int getBatchSize() {
        return batchSize;
    }

    public int getMaxEventSize() {
        return maxEventSize;
    }

    public int getMaxEvents() {
        return maxEvents;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public int getBackOffTime() {
        return backOffTime;
    }
}
