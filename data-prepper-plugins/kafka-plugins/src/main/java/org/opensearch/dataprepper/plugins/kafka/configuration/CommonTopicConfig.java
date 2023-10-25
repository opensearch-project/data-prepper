/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import org.opensearch.dataprepper.model.types.ByteCount;

import java.time.Duration;

/**
 * This class has topic configurations which are common to all Kafka plugins - source, buffer, and sink.
 * <p>
 * Be sure to only add to this configuration if the setting is applicable for all three types.
 */
public class CommonTopicConfig {
    static final boolean DEFAULT_AUTO_COMMIT = false;
    static final Duration DEFAULT_SESSION_TIMEOUT = Duration.ofSeconds(45);
    static final String DEFAULT_AUTO_OFFSET_RESET = "earliest";
    static final Duration DEFAULT_THREAD_WAITING_TIME = Duration.ofSeconds(5);
    static final Duration DEFAULT_MAX_RECORD_FETCH_TIME = Duration.ofSeconds(4);
    static final String DEFAULT_FETCH_MAX_BYTES = "50mb";
    static final Integer DEFAULT_FETCH_MAX_WAIT = 500;
    static final String DEFAULT_FETCH_MIN_BYTES = "1b";
    static final Duration DEFAULT_RETRY_BACKOFF = Duration.ofSeconds(10);
    static final Duration DEFAULT_RECONNECT_BACKOFF = Duration.ofSeconds(10);
    static final String DEFAULT_MAX_PARTITION_FETCH_BYTES = "1mb";
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

    @JsonProperty("auto_commit")
    private Boolean autoCommit = DEFAULT_AUTO_COMMIT;

    @JsonProperty("session_timeout")
    @Valid
    @Size(min = 1)
    private Duration sessionTimeOut = DEFAULT_SESSION_TIMEOUT;

    @JsonProperty("auto_offset_reset")
    private String autoOffsetReset = DEFAULT_AUTO_OFFSET_RESET;

    @JsonProperty("thread_waiting_time")
    private Duration threadWaitingTime = DEFAULT_THREAD_WAITING_TIME;

    @JsonProperty("max_partition_fetch_bytes")
    private String maxPartitionFetchBytes = DEFAULT_MAX_PARTITION_FETCH_BYTES;

    @JsonProperty("fetch_max_bytes")
    private String fetchMaxBytes = DEFAULT_FETCH_MAX_BYTES;

    @JsonProperty("fetch_max_wait")
    @Valid
    @Size(min = 1)
    private Integer fetchMaxWait = DEFAULT_FETCH_MAX_WAIT;

    @JsonProperty("fetch_min_bytes")
    private String fetchMinBytes = DEFAULT_FETCH_MIN_BYTES;

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


    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public Boolean getAutoCommit() {
        return autoCommit;
    }

    public Duration getSessionTimeOut() {
        return sessionTimeOut;
    }


    public String getAutoOffsetReset() {
        return autoOffsetReset;
    }

    public void setAutoOffsetReset(String autoOffsetReset) {
        this.autoOffsetReset = autoOffsetReset;
    }


    public Duration getThreadWaitingTime() {
        return threadWaitingTime;
    }

    public void setThreadWaitingTime(Duration threadWaitingTime) {
        this.threadWaitingTime = threadWaitingTime;
    }


    public long getMaxPartitionFetchBytes() {
        return ByteCount.parse(maxPartitionFetchBytes).getBytes();
    }


    public long getFetchMaxBytes() {
        long value = ByteCount.parse(fetchMaxBytes).getBytes();
        if (value < 1 || value > 50*1024*1024) {
            throw new RuntimeException("Invalid Fetch Max Bytes");
        }
        return value;
    }

    public void setAutoCommit(Boolean autoCommit) {
        this.autoCommit = autoCommit;
    }


    public Integer getFetchMaxWait() {
        return fetchMaxWait;
    }


    public long getFetchMinBytes() {
        long value = ByteCount.parse(fetchMinBytes).getBytes();
        if (value < 1) {
            throw new RuntimeException("Invalid Fetch Min Bytes");
        }
        return value;
    }


    public Duration getRetryBackoff() {
        return retryBackoff;
    }


    public Duration getReconnectBackoff() {
        return reconnectBackoff;
    }

    public void setRetryBackoff(Duration retryBackoff) {
        this.retryBackoff = retryBackoff;
    }


    public Duration getMaxPollInterval() {
        return maxPollInterval;
    }

    public void setMaxPollInterval(Duration maxPollInterval) {
        this.maxPollInterval = maxPollInterval;
    }


    public Integer getConsumerMaxPollRecords() {
        return consumerMaxPollRecords;
    }

    public void setConsumerMaxPollRecords(Integer consumerMaxPollRecords) {
        this.consumerMaxPollRecords = consumerMaxPollRecords;
    }


    public Integer getWorkers() {
        return workers;
    }

    public void setWorkers(Integer workers) {
        this.workers = workers;
    }


    public Duration getHeartBeatInterval() {
        return heartBeatInterval;
    }

    public void setHeartBeatInterval(Duration heartBeatInterval) {
        this.heartBeatInterval = heartBeatInterval;
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
