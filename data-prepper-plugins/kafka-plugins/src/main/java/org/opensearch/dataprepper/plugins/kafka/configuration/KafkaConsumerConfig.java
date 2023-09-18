package org.opensearch.dataprepper.plugins.kafka.configuration;

public interface KafkaConsumerConfig extends KafkaConnection{

    AwsConfig getAwsConfig();

    EncryptionConfig getEncryptionConfig();

    String getClientDnsLookup();

    boolean getAcknowledgementsEnabled();
}
