package org.opensearch.dataprepper.plugins.kafka.configuration;

import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;

import java.util.Collection;
import java.util.Optional;

public interface KafkaProducerConfig extends KafkaConnectionConfig {
    Collection<String> getBootstrapServers();

    AuthConfig getAuthConfig();

    SchemaConfig getSchemaConfig();

    String getSerdeFormat();

    TopicConfig getTopic();

    KafkaProducerProperties getKafkaProducerProperties();

    void setDlqConfig(PluginSetting pluginSetting);

    String getPartitionKey();
    Optional<PluginModel> getDlq();
}
