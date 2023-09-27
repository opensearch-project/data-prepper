package org.opensearch.dataprepper.plugins.kafka.configuration;

import java.util.Collection;

public interface KafkaConnectionConfig {
    Collection<String> getBootstrapServers();
    AuthConfig getAuthConfig();
    AwsConfig getAwsConfig();
    EncryptionConfig getEncryptionConfig();
}
