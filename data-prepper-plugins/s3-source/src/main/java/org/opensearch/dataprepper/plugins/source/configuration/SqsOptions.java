/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.hibernate.validator.constraints.time.DurationMax;
import org.hibernate.validator.constraints.time.DurationMin;

import java.time.Duration;

public class SqsOptions {
    private static final int DEFAULT_MAXIMUM_MESSAGES = 10;
    private static final Duration DEFAULT_VISIBILITY_TIMEOUT_SECONDS = Duration.ofSeconds(30);
    private static final Duration DEFAULT_WAIT_TIME_SECONDS = Duration.ofSeconds(20);
    private static final Duration DEFAULT_POLL_DELAY_SECONDS = Duration.ofSeconds(0);
    private static final Duration DEFAULT_MAX_BACKOFF = Duration.ofSeconds(600);
    private static final Duration DEFAULT_BASE_DELAY = Duration.ofSeconds(30);
    private static final int DEFAULT_MAX_RETRIES = 5;

    @JsonProperty("queue_url")
    @NotBlank(message = "SQS URL cannot be null or empty")
    private String sqsUrl;

    @JsonProperty("maximum_messages")
    @Min(1)
    @Max(10)
    private int maximumMessages = DEFAULT_MAXIMUM_MESSAGES;

    @JsonProperty("visibility_timeout")
    @DurationMin(seconds = 0)
    @DurationMax(seconds = 43200)
    private Duration visibilityTimeout = DEFAULT_VISIBILITY_TIMEOUT_SECONDS;

    @JsonProperty("wait_time")
    @DurationMin(seconds = 0)
    @DurationMax(seconds = 20)
    private Duration waitTime = DEFAULT_WAIT_TIME_SECONDS;

    @JsonProperty("poll_delay")
    @DurationMin(seconds = 0)
    private Duration pollDelay = DEFAULT_POLL_DELAY_SECONDS;

    @JsonProperty("max_backoff")
    @DurationMin(seconds = 0)
    private Duration maxBackOff = DEFAULT_MAX_BACKOFF;

    @JsonProperty("base_delay")
    @DurationMin(seconds = 0)
    private Duration baseDelay = DEFAULT_BASE_DELAY;

    @JsonProperty("max_retries")
    private int maxRetries = DEFAULT_MAX_RETRIES;

    public String getSqsUrl() {
        return sqsUrl;
    }

    public int getMaximumMessages() {
        return maximumMessages;
    }

    public Duration getVisibilityTimeout() {
        return visibilityTimeout;
    }

    public Duration getWaitTime() {
        return waitTime;
    }

    public Duration getPollDelay() {
        return pollDelay;
    }

    public Duration getMaxBackOff() {
        return maxBackOff;
    }

    public Duration getBaseDelay() {
        return baseDelay;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

}
