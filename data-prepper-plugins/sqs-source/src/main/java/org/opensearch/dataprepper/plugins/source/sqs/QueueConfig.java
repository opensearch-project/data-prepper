package org.opensearch.dataprepper.plugins.source.sqs;

import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;

import org.hibernate.validator.constraints.time.DurationMax;
import org.hibernate.validator.constraints.time.DurationMin;

public class QueueConfig {

    private static final int DEFAULT_MAXIMUM_MESSAGES = 10;
    private static final Boolean DEFAULT_VISIBILITY_DUPLICATE_PROTECTION = false;
    private static final Duration DEFAULT_VISIBILITY_TIMEOUT_SECONDS = Duration.ofSeconds(30);
    private static final Duration DEFAULT_VISIBILITY_DUPLICATE_PROTECTION_TIMEOUT = Duration.ofHours(2);
    private static final Duration DEFAULT_WAIT_TIME_SECONDS = Duration.ofSeconds(20);
    private static final Duration DEFAULT_POLL_DELAY_SECONDS = Duration.ofSeconds(0);
    static final int DEFAULT_NUMBER_OF_WORKERS = 1;
    private static final int DEFAULT_BATCH_SIZE = 10;

    @JsonProperty("url")
    @NotNull
    private String url;

    @JsonProperty("workers") 
    @Valid
    private int numWorkers = DEFAULT_NUMBER_OF_WORKERS;

    @JsonProperty("maximum_messages")
    @Min(1)
    @Max(10)
    private int maximumMessages = DEFAULT_MAXIMUM_MESSAGES;

    @JsonProperty("batch_size")
    @Max(10)
    private Integer batchSize = DEFAULT_BATCH_SIZE;

    @JsonProperty("polling_frequency")
    private Duration pollingFrequency = Duration.ZERO;

    @JsonProperty("poll_delay")
    @DurationMin(seconds = 0)
    private Duration pollDelay = DEFAULT_POLL_DELAY_SECONDS;

    @JsonProperty("visibility_timeout")
    @DurationMin(seconds = 0)
    @DurationMax(seconds = 43200)
    private Duration visibilityTimeout = DEFAULT_VISIBILITY_TIMEOUT_SECONDS;

    @JsonProperty("visibility_duplication_protection")
    private Boolean visibilityDuplicateProtection = DEFAULT_VISIBILITY_DUPLICATE_PROTECTION;

    @JsonProperty("visibility_duplicate_protection_timeout")
    @DurationMin(seconds = 30)
    @DurationMax(hours = 24)
    private Duration visibilityDuplicateProtectionTimeout = DEFAULT_VISIBILITY_DUPLICATE_PROTECTION_TIMEOUT;

    @JsonProperty("wait_time")
    @DurationMin(seconds = 0)
    @DurationMax(seconds = 20)
    private Duration waitTime = DEFAULT_WAIT_TIME_SECONDS;

    public String getUrl() {
        return url;
    }

    public Duration getPollingFrequency() {
        return pollingFrequency;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public int getMaximumMessages() {
        return maximumMessages;
    }
    
    public int getNumWorkers() {
        return numWorkers;
    }

    public Duration getVisibilityTimeout() {
        return visibilityTimeout;
    }

    public Boolean getVisibilityDuplicateProtection() {
        return visibilityDuplicateProtection;
    }

    public Duration getVisibilityDuplicateProtectionTimeout() {
        return visibilityDuplicateProtectionTimeout;
    }

    public Duration getWaitTime() {
        return waitTime;
    }

    public Duration getPollDelay() {
        return pollDelay;
    }
}

