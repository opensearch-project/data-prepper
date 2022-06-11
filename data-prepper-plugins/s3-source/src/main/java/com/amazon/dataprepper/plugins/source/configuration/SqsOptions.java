/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

import java.time.Duration;

public class SqsOptions {
    private static final int DEFAULT_MAXIMUM_MESSAGES = 10;
    private static final Duration DEFAULT_VISIBILITY_TIMEOUT_SECONDS = Duration.ofSeconds(30);
    private static final Duration DEFAULT_WAIT_TIME_SECONDS = Duration.ofSeconds(20);
    private static final Duration DEFAULT_POLL_DELAY_SECONDS = Duration.ofSeconds(0);

    @JsonProperty("queue_url")
    @NotBlank(message = "SQS URL cannot be null or empty")
    private String sqsUrl;

    @JsonProperty("maximum_messages")
    private int maximumMessages = DEFAULT_MAXIMUM_MESSAGES;

    @JsonProperty("visibility_timeout")
    @Min(0)
    @Max(43200)
    private Duration visibilityTimeout = DEFAULT_VISIBILITY_TIMEOUT_SECONDS;

    @JsonProperty("wait_time")
    @Min(0)
    @Max(20)
    private Duration waitTime = DEFAULT_WAIT_TIME_SECONDS;

    @JsonProperty("poll_delay")
    @Min(0)
    private Duration pollDelay = DEFAULT_POLL_DELAY_SECONDS;

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
}
