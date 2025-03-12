/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.sqs;

import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import org.opensearch.dataprepper.plugins.source.sqs.common.OnErrorOption;
import org.hibernate.validator.constraints.time.DurationMax;
import org.hibernate.validator.constraints.time.DurationMin;
import org.opensearch.dataprepper.model.configuration.PluginModel;

public class QueueConfig {

    private static final Integer DEFAULT_MAXIMUM_MESSAGES = null;
    private static final boolean DEFAULT_VISIBILITY_DUPLICATE_PROTECTION = false;
    private static final Duration DEFAULT_VISIBILITY_TIMEOUT_SECONDS = null;
    private static final Duration DEFAULT_VISIBILITY_DUPLICATE_PROTECTION_TIMEOUT = Duration.ofHours(2);
    private static final Duration DEFAULT_WAIT_TIME_SECONDS = null;
    private static final Duration DEFAULT_POLL_DELAY_SECONDS = Duration.ofSeconds(0);
    static final int DEFAULT_NUMBER_OF_WORKERS = 1;

    @JsonProperty("url")
    @NotNull
    private String url;

    @JsonProperty("workers") 
    @Valid
    private int numWorkers = DEFAULT_NUMBER_OF_WORKERS;

    @JsonProperty("maximum_messages")
    @Min(1)
    @Max(10)
    private Integer maximumMessages = DEFAULT_MAXIMUM_MESSAGES;

    @JsonProperty("poll_delay")
    @DurationMin(seconds = 0)
    private Duration pollDelay = DEFAULT_POLL_DELAY_SECONDS;

    @JsonProperty("visibility_timeout")
    @DurationMin(seconds = 0)
    @DurationMax(seconds = 43200)
    private Duration visibilityTimeout = DEFAULT_VISIBILITY_TIMEOUT_SECONDS;

    @JsonProperty("visibility_duplication_protection")
    @NotNull
    private boolean visibilityDuplicateProtection = DEFAULT_VISIBILITY_DUPLICATE_PROTECTION;

    @JsonProperty("visibility_duplicate_protection_timeout")
    @DurationMin(seconds = 30)
    @DurationMax(hours = 24)
    private Duration visibilityDuplicateProtectionTimeout = DEFAULT_VISIBILITY_DUPLICATE_PROTECTION_TIMEOUT;

    @JsonProperty("wait_time")
    @DurationMin(seconds = 0)
    @DurationMax(seconds = 20)
    private Duration waitTime = DEFAULT_WAIT_TIME_SECONDS;

    @JsonProperty("codec")
    private PluginModel codec = null;

    @JsonProperty("on_error")
    private OnErrorOption onErrorOption = OnErrorOption.RETAIN_MESSAGES;

    public String getUrl() {
        return url;
    }

    public Integer getMaximumMessages() {
        return maximumMessages;
    }
    
    public int getNumWorkers() {
        return numWorkers;
    }

    public Duration getVisibilityTimeout() {return visibilityTimeout; }

    public boolean getVisibilityDuplicateProtection() {
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

    public PluginModel getCodec() {
        return codec;
    }

    public OnErrorOption getOnErrorOption() {
        return onErrorOption;
    }
}

