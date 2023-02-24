package org.opensearch.dataprepper.plugins.config;


import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.opensearch.dataprepper.plugins.source.KafkaSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ConsumerConfigs {
	
	private static final Logger logger = LoggerFactory.getLogger(ConsumerConfigs.class);
	
	public Properties getConsumerProperties(KafkaSourceConfig sourceConfig) {
		
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, sourceConfig.getBootstrapservers());
		props.put(ConsumerConfig.GROUP_ID_CONFIG, sourceConfig.getGroupId());
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, sourceConfig.getEnableautocommit());
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, sourceConfig.getEnableautocommitinterval());
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sourceConfig.getSessiontimeout());
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, sourceConfig.getKeyDeserializer());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, sourceConfig.getValueDeserializer());
		

		return props;
		
		
		
	}
	
}
