package org.opensearch.dataprepper.plugins.source;

import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.validation.constraints.NotNull;

public class KafkaSourceConfig {
	
	public static final Integer NUM_OF_PARTITIONS = 2;
	public static final Integer CONSUMER_COUNT = 2;
	public static final String TOPIC_NAME = "my-topic";
	public static final String CONSUMER_GROUP_NAME = "test-consumer-group";
	public static final Long THREAD_WAITING_MILLI_SEC = 3000L;
	
	private final String security_inter_broker_protocol="PLAIN";
	private final String sasl_mechanism="PLAIN";
	
	    @JsonProperty("bootstrap_servers")
	    @NotNull
	    private String bootstrapservers;


		public String getBootstrapservers() {
			return bootstrapservers;
		}
		
		@JsonProperty("group_id")
	    private String groupId;


		public String getGroupId() {
			return groupId;
		}
		
		@JsonProperty("enable_autocommit")
		@NotNull
	    private String enableautocommit;


		public String getEnableautocommit() {
			return enableautocommit;
		}
	 
		@JsonProperty("enable_autocommit_interval")
	    private String enableautocommitinterval;


		public String getEnableautocommitinterval() {
			return enableautocommitinterval;
		}
		
		@JsonProperty("session_timeout")
	    private String sessiontimeout;


		public String getSessiontimeout() {
			return sessiontimeout;
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
		
		public String getSecurityInterBrokerProtocol() {
		    return security_inter_broker_protocol;
		} 
		
		public String getSaslMechanism() {
		    return sasl_mechanism;
		}
}
