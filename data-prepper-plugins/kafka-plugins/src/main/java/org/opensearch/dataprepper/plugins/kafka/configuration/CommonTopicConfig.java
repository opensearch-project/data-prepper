/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.time.Duration;

/**
 * This class has topic configurations which are common to all Kafka plugins - source, buffer, and sink.
 * <p>
 * Be sure to only add to this configuration if the setting is applicable for all three types.
 */
public abstract class CommonTopicConfig implements TopicConfig {
    static final Duration DEFAULT_SESSION_TIMEOUT = Duration.ofSeconds(45);
    static final String DEFAULT_AUTO_OFFSET_RESET = "earliest";
    static final Duration DEFAULT_THREAD_WAITING_TIME = Duration.ofSeconds(5);
    static final Duration DEFAULT_RETRY_BACKOFF = Duration.ofSeconds(10);
    static final Duration DEFAULT_RECONNECT_BACKOFF = Duration.ofSeconds(10);
    static final Duration DEFAULT_MAX_POLL_INTERVAL = Duration.ofSeconds(300);
    static final Integer DEFAULT_CONSUMER_MAX_POLL_RECORDS = 500;
    static final Integer DEFAULT_NUM_OF_WORKERS = 2;
    static final Duration DEFAULT_HEART_BEAT_INTERVAL_DURATION = Duration.ofSeconds(5);


    @JsonProperty("name")
    @NotNull
    @Valid
    private String name;

    @JsonProperty("group_id")
    @Valid
    @Size(min = 1, max = 255, message = "size of group id should be between 1 and 255")
    private String groupId;

    @JsonProperty("workers")
    @Valid
    @Size(min = 1, max = 200, message = "Number of worker threads should lies between 1 and 200")
    private Integer workers = DEFAULT_NUM_OF_WORKERS;

    @JsonProperty("session_timeout")
    @Valid
    @Size(min = 1)
    private Duration sessionTimeOut = DEFAULT_SESSION_TIMEOUT;

    @JsonProperty("auto_offset_reset")
    private String autoOffsetReset = DEFAULT_AUTO_OFFSET_RESET;

    @JsonProperty("thread_waiting_time")
    private Duration threadWaitingTime = DEFAULT_THREAD_WAITING_TIME;

    @JsonProperty("retry_backoff")
    private Duration retryBackoff = DEFAULT_RETRY_BACKOFF;

    @JsonProperty("reconnect_backoff")
    private Duration reconnectBackoff = DEFAULT_RECONNECT_BACKOFF;

    @JsonProperty("max_poll_interval")
    private Duration maxPollInterval = DEFAULT_MAX_POLL_INTERVAL;

    @JsonProperty("consumer_max_poll_records")
    private Integer consumerMaxPollRecords = DEFAULT_CONSUMER_MAX_POLL_RECORDS;

    @JsonProperty("heart_beat_interval")
    @Valid
    @Size(min = 1)
    private Duration heartBeatInterval= DEFAULT_HEART_BEAT_INTERVAL_DURATION;


    @Override
    public String getGroupId() {
        return groupId;
    }

    @Override
    public Duration getSessionTimeOut() {
        return sessionTimeOut;
    }

    @Override
    public String getAutoOffsetReset() {
        return autoOffsetReset;
    }

    @Override
    public Duration getThreadWaitingTime() {
        return threadWaitingTime;
    }

    @Override
    public Duration getRetryBackoff() {
        return retryBackoff;
    }

    @Override
    public Duration getReconnectBackoff() {
        return reconnectBackoff;
    }

    @Override
    public Duration getMaxPollInterval() {
        return maxPollInterval;
    }

    @Override
    public Integer getConsumerMaxPollRecords() {
        return consumerMaxPollRecords;
    }

    @Override
    public Integer getWorkers() {
        return workers;
    }

    @Override
    public Duration getHeartBeatInterval() {
        return heartBeatInterval;
    }

    @Override
    public String getName() {
        return name;
    }
}
