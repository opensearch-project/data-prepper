package org.opensearch.dataprepper.plugins.kafka.configuration;

import org.opensearch.dataprepper.plugins.kafka.util.KafkaClusterAuthConfig;

import java.util.Collection;

public interface KafkaConnectionConfig extends KafkaClusterAuthConfig {
    Collection<String> getBootstrapServers();
    AuthConfig getAuthConfig();
    AwsConfig getAwsConfig();
    EncryptionConfig getEncryptionConfig();
    SchemaConfig getSchemaConfig();
}
