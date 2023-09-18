package org.opensearch.dataprepper.plugins.kafka.configuration;

import java.util.Collection;
import java.util.List;

public interface KafkaConnection {
    Collection<String> getBootStrapServers();
    AuthConfig getAuthConfig();
    SchemaConfig getSchemaConfig();

    List<TopicConfig> getTopics();
}
