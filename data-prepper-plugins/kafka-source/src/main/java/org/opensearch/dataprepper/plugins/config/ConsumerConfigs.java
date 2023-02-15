package org.opensearch.dataprepper.plugins.config;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerConfigs {
	
	private static final Logger logger = LoggerFactory.getLogger(ConsumerConfigs.class);
	public static final String BOOT_STRAP_SERVER = "localhost:9092";
	public static final Integer NUM_OF_PARTITIONS = 2;
	public static final Integer CONSUMER_COUNT = 2;
	public static final String TOPIC_NAME = "mytopic";
	
	public static Properties getConfigs(String consumerGroupId) {
			
		Properties props = new Properties();
		
		props.setProperty("bootstrap.servers", BOOT_STRAP_SERVER);
        props.setProperty("group.id", consumerGroupId);
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        
        logger.info("Consumer configurations set successfully...");
        return props;
		
		
	}

}
