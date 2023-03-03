/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import java.time.Duration;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.validation.constraints.NotNull;

public class KafkaSourceConfig {

	@JsonProperty("bootstrap_servers")
	@NotNull
	private List<String> bootStrapServers;

	public List<String> getBootStrapServers() {
		return bootStrapServers;
	}

	@JsonProperty("group_id")
	private String groupId;

	public String getGroupId() {
		return groupId;
	}

	@JsonProperty("topic_name")
	@NotNull
	private String topicName;

	public String getTopicName() {
		return topicName;
	}

	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}

	@JsonProperty("max_retry_attempts")
	private Integer maxRetryAttempts;

	public Integer getMaxRetryAttempts() {
		return maxRetryAttempts;
	}

	public void setMaxRetryAttempts(Integer maxRetryAttempts) {
		this.maxRetryAttempts = maxRetryAttempts;
	}

	@JsonProperty("schema_type")
	@NotNull
	private String schemaType;

	public String getSchemaType() {
		return schemaType;
	}

	public void setSchemaType(String schemaType) {
		this.schemaType = schemaType;
	}

	@JsonProperty("enable_autocommit")
	@NotNull
	private String enableAutoCommit;

	public String getEnableAutoCommit() {
		return enableAutoCommit;
	}

	@JsonProperty("autocommit_interval")
	private Duration autoCommitInterval;

	public Duration getAutoCommitInterval() {
		return autoCommitInterval;
	}

	@JsonProperty("session_timeout")
	private Duration sessionTimeOut;

	public Duration getSessionTimeOut() {
		return sessionTimeOut;
	}

	@JsonProperty("key_deserializer")
	@NotNull
	private String keyDeserializer;

	public String getKeyDeserializer() {
		return keyDeserializer;
	}

	@JsonProperty("value_deserializer")
	@NotNull
	private String valueDeserializer;

	public String getValueDeserializer() {
		return valueDeserializer;
	}

	@JsonProperty("security_protocol")
	private String securityProtocol;

	public String getSecurityProtocol() {
		return securityProtocol;
	}

	@JsonProperty("auto_offset_reset")
	private String autoOffsetReset;

	public String getAutoOffsetReset() {
		return autoOffsetReset;
	}

	public void setAutoOffsetReset(String autoOffsetReset) {
		this.autoOffsetReset = autoOffsetReset;
	}

	@JsonProperty("client_id")
	private String clientId;

	public String getClientId() {
		return clientId;
	}

	@JsonProperty("consumer_count")
	private Integer consumerCount;

	public Integer getConsumerCount() {
		return consumerCount;
	}

	@JsonProperty("consumer_group_name")
	private String consumerGroupName;

	public String getConsumerGroupName() {
		return consumerGroupName;
	}

	@JsonProperty("number_of_partitions")
	private Integer numberOfPartitions;

	public Integer getNumberOfPartitions() {
		return numberOfPartitions;
	}

	@JsonProperty("thread_waiting_time_in_sec")
	private Duration threadWaitingTime;

	public Duration getThreadWaitingTime() {
		return threadWaitingTime;
	}

	@JsonProperty("max_record_fetch_time")
	private Duration maxRecordFetchTime;

	public Duration getMaxRecordFetchTime() {
		return maxRecordFetchTime;
	}

	@JsonProperty("heart_beat_interval")
	private Duration heartBeatInterval;

	public Duration getHeartBeatInterval() {
		return heartBeatInterval;
	}

	@JsonProperty("buffer_default_timeout")
	private Duration bufferDefaultTimeout;

	public Duration getBufferDefaultTimeout() {
		return bufferDefaultTimeout;
	}

	@JsonProperty("record_type")
	private String recordType;

	public String getRecordType() {
		return recordType;
	}

}
