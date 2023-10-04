package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class KafkaBufferConfig implements KafkaProducerConfig, KafkaConsumerConfig {

    @JsonProperty("bootstrap_servers")
    private List<String> bootStrapServers;

    @JsonProperty("topic")
    TopicConfig topic;

    @JsonProperty("schema")
    @Valid
    private SchemaConfig schemaConfig;

    @Valid
    @JsonProperty("authentication")
    private AuthConfig authConfig;

    @JsonProperty("encryption")
    private EncryptionConfig encryptionConfig;

    @JsonProperty("producer_properties")
    private KafkaProducerProperties kafkaProducerProperties;

    @JsonProperty("aws")
    @Valid
    private AwsConfig awsConfig;


    public List<String> getBootstrapServers() {
        if (Objects.nonNull(bootStrapServers)) {
            return bootStrapServers;
        }
        return null;
    }

    @Override
    public AuthConfig getAuthConfig() {
        return authConfig;
    }

    @Override
    public SchemaConfig getSchemaConfig() {
        return schemaConfig;
    }

    @Override
    public String getSerdeFormat() {
        return topic.getSerdeFormat().toString();
    }

    @Override
    public TopicConfig getTopic() {
        return topic;
    }

    @Override
    public List<TopicConfig> getTopics() {
        return Collections.singletonList(topic);
    }

    @Override
    public KafkaProducerProperties getKafkaProducerProperties() {
        return kafkaProducerProperties;
    }

    @Override
    public String getPartitionKey() {
        return "pipeline-buffer";
    }

    @Override
    public Optional<PluginModel> getDlq() {
        // TODO: move DLQ logic to be sink specific (currently, write to DLQ is handled by KafkaCustomConsumer)
        return Optional.empty();
    }
    @Override
    public void setDlqConfig(PluginSetting pluginSetting) {

    }
    @Override
    public AwsConfig getAwsConfig() {
        return awsConfig;
    }

    @Override
    public EncryptionConfig getEncryptionConfig() {
        if (Objects.isNull(encryptionConfig)) {
            return new EncryptionConfig();
        }
        return encryptionConfig;
    }

    @Override
    public String getClientDnsLookup() {
        return null;
    }

    @Override
    public boolean getAcknowledgementsEnabled() {
        return false;
    }
}
