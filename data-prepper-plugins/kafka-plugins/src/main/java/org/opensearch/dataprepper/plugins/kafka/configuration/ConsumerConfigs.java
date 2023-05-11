/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import java.time.Duration;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Size;

/**
 * * A helper class that helps to read user configuration values from
 * pipelines.yaml
 */
public class ConsumerConfigs {

  private static final String AUTO_COMMIT = "false";
  private static final Duration AUTOCOMMIT_INTERVAL = Duration.ofSeconds(5);
  private static final Duration SESSION_TIMEOUT = Duration.ofSeconds(45);
  private static final int MAX_RETRY_ATTEMPT = Integer.MAX_VALUE;
  private static final String AUTO_OFFSET_RESET = "earliest";
  private static final Duration THREAD_WAITING_TIME = Duration.ofSeconds(1);
  private static final Duration MAX_RECORD_FETCH_TIME = Duration.ofSeconds(4);
  private static final Duration BUFFER_DEFAULT_TIMEOUT = Duration.ofSeconds(5);
  private static final Duration MAX_RETRY_DELAY = Duration.ofSeconds(1);
  private static final Long FETCH_MAX_BYTES = 52428800L;
  private static final Long FETCH_MAX_WAIT = 500L;
  private static final Long FETCH_MIN_BYTES = 1L;
  private static final Duration RETRY_BACKOFF = Duration.ofSeconds(100);
  private static final Duration MAX_POLL_INTERVAL = Duration.ofSeconds(300000);
  private static final Long CONSUMER_MAX_POLL_RECORDS = 500L;
  private static final Integer NUM_OF_WORKERS = 10;
  private static final Duration HEART_BEAT_INTERVAL_DURATION = Duration.ofSeconds(3);

  @JsonProperty("group_id")
  private String groupId;

  @JsonProperty("workers")
  @Size(min = 1, max = 200, message = "Number of worker threads should lies between 1 and 200")
  private Integer workers = NUM_OF_WORKERS;

  @JsonProperty("max_retry_attempts")
  @Size(min = 1, max = Integer.MAX_VALUE, message = " Max retry attempts should lies between 1 and Integer.MAX_VALUE")
  private Integer maxRetryAttempts = MAX_RETRY_ATTEMPT;

  @JsonProperty("max_retry_delay")
  @Size(min = 1)
  private Duration maxRetryDelay = MAX_RETRY_DELAY;

  @JsonProperty("autocommit")
  private String autoCommit = AUTO_COMMIT;

  @JsonProperty("autocommit_interval")
  @Size(min = 1)
  private Duration autoCommitInterval = AUTOCOMMIT_INTERVAL;

  @JsonProperty("session_timeout")
  @Size(min = 1)
  private Duration sessionTimeOut = SESSION_TIMEOUT;

  @JsonProperty("auto_offset_reset")
  private String autoOffsetReset = AUTO_OFFSET_RESET;

  @JsonProperty("group_name")
  private String groupName;

  @JsonProperty("thread_waiting_time")
  private Duration threadWaitingTime = THREAD_WAITING_TIME;

  @JsonProperty("max_record_fetch_time")
  private Duration maxRecordFetchTime = MAX_RECORD_FETCH_TIME;

  @JsonProperty("buffer_default_timeout")
  @Size(min = 1)
  private Duration bufferDefaultTimeout = BUFFER_DEFAULT_TIMEOUT;

  @JsonProperty("fetch_max_bytes")
  @Size(min = 1, max = 52428800)
  private Long fetchMaxBytes = FETCH_MAX_BYTES;

  @JsonProperty("fetch_max_wait")
  @Size(min = 1)
  private Long fetchMaxWait = FETCH_MAX_WAIT;

  @JsonProperty("fetch_min_bytes")
  @Size(min = 1)
  private Long fetchMinBytes = FETCH_MIN_BYTES;

  @JsonProperty("retry_backoff")
  private Duration retryBackoff = RETRY_BACKOFF;

  @JsonProperty("max_poll_interval")
  private Duration maxPollInterval = MAX_POLL_INTERVAL;

  @JsonProperty("consumer_max_poll_records")
  private Long consumerMaxPollRecords = CONSUMER_MAX_POLL_RECORDS;

  @JsonProperty("heart_beat_interval")
  @Size(min = 1)
  private Duration heartBeatInterval= HEART_BEAT_INTERVAL_DURATION;

  public String getGroupId() {
    return groupId;
  }

  public void setGroupId(String groupId) {
    this.groupId = groupId;
  }

  public Integer getMaxRetryAttempts() {
    return maxRetryAttempts;
  }

  public void setMaxRetryAttempts(Integer maxRetryAttempts) {
    this.maxRetryAttempts = maxRetryAttempts;
  }

  public String getAutoCommit() {
    return autoCommit;
  }

  public Duration getAutoCommitInterval() {
    return autoCommitInterval;
  }

  public void setAutoCommitInterval(Duration autoCommitInterval) {
    this.autoCommitInterval = autoCommitInterval;
  }

  public Duration getSessionTimeOut() {
    return sessionTimeOut;
  }

  public void setSessionTimeOut(Duration sessionTimeOut) {
    this.sessionTimeOut = sessionTimeOut;
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

  public Long getFetchMaxBytes() {
    return fetchMaxBytes;
  }

  public void setFetchMaxBytes(Long fetchMaxBytes) {
    this.fetchMaxBytes = fetchMaxBytes;
  }

  public void setAutoCommit(String autoCommit) {
    this.autoCommit = autoCommit;
  }

  public Long getFetchMaxWait() {
    return fetchMaxWait;
  }

  public void setFetchMaxWait(Long fetchMaxWait) {
    this.fetchMaxWait = fetchMaxWait;
  }

  public Long getFetchMinBytes() {
    return fetchMinBytes;
  }

  public void setFetchMinBytes(Long fetchMinBytes) {
    this.fetchMinBytes = fetchMinBytes;
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

  public Long getConsumerMaxPollRecords() {
    return consumerMaxPollRecords;
  }

  public void setConsumerMaxPollRecords(Long consumerMaxPollRecords) {
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

}
