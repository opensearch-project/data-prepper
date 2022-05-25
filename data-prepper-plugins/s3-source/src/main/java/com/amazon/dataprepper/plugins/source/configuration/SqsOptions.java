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
    private final int DEFAULT_MAXIMUM_MESSAGES = 10;
    private final int DEFAULT_VISIBILITY_TIMEOUT_SECONDS = 30;
    private final int DEFAULT_WAIT_TIME_SECONDS = 30;
    private final int DEFAULT_POLL_DELAY_SECONDS = 0;
    private final int DEFAULT_THREAD_COUNT = 1;

    @JsonProperty("queue_url")
    @NotBlank(message = "SQS URL cannot be null or empty")
    private String sqsUrl;

    @JsonProperty("maximum_messages")
    private int maximumMessages = DEFAULT_MAXIMUM_MESSAGES;

    @JsonProperty("visibility_timeout")
    @Min(0)
    @Max(43200)
    private Duration visibilityTimeout = Duration.ofSeconds(DEFAULT_VISIBILITY_TIMEOUT_SECONDS);

    @JsonProperty("wait_time")
    private Duration waitTime = Duration.ofSeconds(DEFAULT_WAIT_TIME_SECONDS);

    @JsonProperty("poll_delay")
    private Duration pollDelay = Duration.ofSeconds(DEFAULT_POLL_DELAY_SECONDS);

    @JsonProperty("thread_count")
    private int threadCount = DEFAULT_THREAD_COUNT;

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

    public int getThreadCount() {
        return threadCount;
    }
}
