/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;

import java.time.Duration;
/**
 * * A helper class that helps to read consumer configuration values from
 * pipelines.yaml
 */
public class TopicConfig {
    private static final String AUTO_COMMIT = "false";
    private static final Duration DEFAULT_COMMIT_INTERVAL = Duration.ofSeconds(5);
    private static final Duration DEFAULT_SESSION_TIMEOUT = Duration.ofSeconds(45);
    private static final int MAX_RETRY_ATTEMPT = Integer.MAX_VALUE;
    static final String DEFAULT_AUTO_OFFSET_RESET = "latest";
    static final Duration THREAD_WAITING_TIME = Duration.ofSeconds(5);
    private static final Duration MAX_RECORD_FETCH_TIME = Duration.ofSeconds(4);
    private static final Duration BUFFER_DEFAULT_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration MAX_RETRY_DELAY = Duration.ofSeconds(1);
    private static final Integer FETCH_MAX_BYTES = 52428800;
    private static final Integer FETCH_MAX_WAIT = 500;
    private static final Integer FETCH_MIN_BYTES = 1;
    private static final Duration RETRY_BACKOFF = Duration.ofSeconds(100);
    private static final Duration MAX_POLL_INTERVAL = Duration.ofSeconds(300000);
    private static final Integer CONSUMER_MAX_POLL_RECORDS = 500;
    static final Integer DEFAULT_NUM_OF_WORKERS = 2;
    static final Duration HEART_BEAT_INTERVAL_DURATION = Duration.ofSeconds(5);

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

    @JsonProperty("max_retry_attempts")
    @Valid
    @Size(min = 1, max = Integer.MAX_VALUE, message = " Max retry attempts should lies between 1 and Integer.MAX_VALUE")
    private Integer maxRetryAttempts = MAX_RETRY_ATTEMPT;

    @JsonProperty("max_retry_delay")
    @Valid
    @Size(min = 1)
    private Duration maxRetryDelay = MAX_RETRY_DELAY;

    @JsonProperty("serde_format")
    private MessageFormat serdeFormat= MessageFormat.PLAINTEXT;

    @JsonProperty("auto_commit")
    private Boolean autoCommit = false;

    @JsonProperty("commit_interval")
    @Valid
    @Size(min = 1)
    private Duration commitInterval = DEFAULT_COMMIT_INTERVAL;

    @JsonProperty("session_timeout")
    @Valid
    @Size(min = 1)
    private Duration sessionTimeOut = DEFAULT_SESSION_TIMEOUT;

    @JsonProperty("auto_offset_reset")
    private String autoOffsetReset = DEFAULT_AUTO_OFFSET_RESET;

    @JsonProperty("group_name")
    @Valid
    @Size(min = 1, max = 255, message = "size of group name should be between 1 and 255")
    private String groupName;

    @JsonProperty("thread_waiting_time")
    private Duration threadWaitingTime = THREAD_WAITING_TIME;

    @JsonProperty("max_record_fetch_time")
    private Duration maxRecordFetchTime = MAX_RECORD_FETCH_TIME;

    @JsonProperty("buffer_default_timeout")
    @Valid
    @Size(min = 1)
    private Duration bufferDefaultTimeout = BUFFER_DEFAULT_TIMEOUT;

    @JsonProperty("fetch_max_bytes")
    @Valid
    @Size(min = 1, max = 52428800)
    private Integer fetchMaxBytes = FETCH_MAX_BYTES;

    @JsonProperty("fetch_max_wait")
    @Valid
    @Size(min = 1)
    private Integer fetchMaxWait = FETCH_MAX_WAIT;

    @JsonProperty("fetch_min_bytes")
    @Size(min = 1)
    @Valid
    private Integer fetchMinBytes = FETCH_MIN_BYTES;

    @JsonProperty("key_mode")
    private KafkaKeyMode kafkaKeyMode = KafkaKeyMode.INCLUDE_AS_FIELD;

    @JsonProperty("retry_backoff")
    private Duration retryBackoff = RETRY_BACKOFF;

    @JsonProperty("max_poll_interval")
    private Duration maxPollInterval = MAX_POLL_INTERVAL;

    @JsonProperty("consumer_max_poll_records")
    private Integer consumerMaxPollRecords = CONSUMER_MAX_POLL_RECORDS;

    @JsonProperty("heart_beat_interval")
    @Valid
    @Size(min = 1)
    private Duration heartBeatInterval= HEART_BEAT_INTERVAL_DURATION;

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public void setMaxRetryAttempts(Integer maxRetryAttempts) {
        this.maxRetryAttempts = maxRetryAttempts;
    }

    public MessageFormat getSerdeFormat() {
        return serdeFormat;
    }

    public Boolean getAutoCommit() {
        return autoCommit;
    }

    public Duration getCommitInterval() {
        return commitInterval;
    }

    public void setCommitInterval(Duration commitInterval) {
        this.commitInterval = commitInterval;
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

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public Duration getThreadWaitingTime() {
        return threadWaitingTime;
    }

    public void setThreadWaitingTime(Duration threadWaitingTime) {
        this.threadWaitingTime = threadWaitingTime;
    }

    public Duration getMaxRecordFetchTime() {
        return maxRecordFetchTime;
    }

    public void setMaxRecordFetchTime(Duration maxRecordFetchTime) {
        this.maxRecordFetchTime = maxRecordFetchTime;
    }

    public Duration getBufferDefaultTimeout() {
        return bufferDefaultTimeout;
    }

    public void setBufferDefaultTimeout(Duration bufferDefaultTimeout) {
        this.bufferDefaultTimeout = bufferDefaultTimeout;
    }

    public Integer getFetchMaxBytes() {
        return fetchMaxBytes;
    }

    public void setAutoCommit(Boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    public Integer getFetchMaxWait() {
        return fetchMaxWait;
    }

    public Integer getFetchMinBytes() {
        return fetchMinBytes;
    }

    public Duration getRetryBackoff() {
        return retryBackoff;
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

    public Duration getMaxRetryDelay() {
        return maxRetryDelay;
    }

    public void setMaxRetryDelay(Duration maxRetryDelay) {
        this.maxRetryDelay = maxRetryDelay;
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

    public KafkaKeyMode getKafkaKeyMode() {
        return kafkaKeyMode;
    }

}
