package org.opensearch.dataprepper.plugins.source;

import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.validation.constraints.NotNull;

public class KafkaSourceConfig {
	
	public static final Integer NUM_OF_PARTITIONS = 2;
	public static final Integer CONSUMER_COUNT = 3;
	public static final Long MAX_FETCH_TIME = 4000L;
	public static final String CONSUMER_GROUP_NAME = "kafka-consumer-group";
	public static final Long THREAD_WAITING_MILLI_SEC = 1000L;
	public static final String COMMIT_FAILURE_MSG = "Failed to commit record. Will try again in the future...";
	public static final String CLOSING_CONSUMER_MSG= "Closing the Consumer...";
	
	    @JsonProperty("bootstrap_servers")
	    @NotNull
	    private String bootStrapServers;
		
		public String getBootStrapServers() {
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

		@JsonProperty("enable_autocommit_interval")
	    private String enableAutoCommitInterval;

		
		public String getEnableAutoCommitInterval() {
			return enableAutoCommitInterval;
		}

		@JsonProperty("session_timeout")
	    private String sessionTimeOut;
		
		public String getSessionTimeOut() {
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
		
		@JsonProperty("securityProtocol")
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
		
		
}
