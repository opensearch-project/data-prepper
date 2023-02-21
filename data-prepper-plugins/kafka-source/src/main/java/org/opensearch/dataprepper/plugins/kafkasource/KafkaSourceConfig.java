package org.opensearch.dataprepper.plugins.kafkasource;

import com.fasterxml.jackson.annotation.JsonProperty;

public class KafkaSourceConfig {
	
	private final String securityinterbrokerprotocol="PLAIN";
	private final String saslmechanism="PLAIN";
	
	    @JsonProperty("bootstrapservers")
	    private String bootstrapservers;


		public String getBootstrapservers() {
			return bootstrapservers;
		}
		
		@JsonProperty("groupId")
	    private String groupId;


		public String getGroupId() {
			return groupId;
		}
		
		@JsonProperty("enableautocommit")
	    private String enableautocommit;


		public String getEnableautocommit() {
			return enableautocommit;
		}
	 
		@JsonProperty("enableautocommitinterval")
	    private String enableautocommitinterval;


		public String getEnableautocommitinterval() {
			return enableautocommitinterval;
		}
		
		@JsonProperty("sessiontimeout")
	    private String sessiontimeout;


		public String getSessiontimeout() {
			return sessiontimeout;
		}
		
		@JsonProperty("keyDeserializer")
	    private String keyDeserializer;


		public String getKeyDeserializer() {
			return keyDeserializer;
		}
		
		@JsonProperty("valueDeserializer")
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
		    return securityinterbrokerprotocol;
		} 
		
		public String getSaslMechanism() {
		    return saslmechanism;
		}
}
