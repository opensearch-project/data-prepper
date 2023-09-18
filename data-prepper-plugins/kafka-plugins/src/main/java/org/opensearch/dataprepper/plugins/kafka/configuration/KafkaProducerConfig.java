package org.opensearch.dataprepper.plugins.kafka.configuration;

import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

public interface KafkaProducerConfig extends KafkaConnection {
    Collection<String> getBootStrapServers();

    AuthConfig getAuthConfig();

    SchemaConfig getSchemaConfig();

    String getSerdeFormat();

    List<TopicConfig> getTopics();

    KafkaProducerProperties getKafkaProducerProperties();

    void setDlqConfig(PluginSetting pluginSetting);

    String getPartitionKey();

    Optional<PluginModel> getDlq();
}
