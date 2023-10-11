package org.opensearch.dataprepper.plugins.kafka.configuration;

import java.util.List;

public interface KafkaConsumerConfig extends KafkaConnectionConfig {

    String getClientDnsLookup();

    boolean getAcknowledgementsEnabled();
    
    SchemaConfig getSchemaConfig();

    List<TopicConfig> getTopics();
}
