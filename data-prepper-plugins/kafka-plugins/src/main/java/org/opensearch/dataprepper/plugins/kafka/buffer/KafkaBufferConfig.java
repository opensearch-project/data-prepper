/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.buffer;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.plugins.kafka.configuration.AuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.AwsConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConsumerConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaConsumerConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaProducerConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaProducerProperties;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaConfig;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

class KafkaBufferConfig implements KafkaProducerConfig, KafkaConsumerConfig {
    private static final Duration DEFAULT_DRAIN_TIMEOUT = Duration.ofSeconds(30);

    @JsonProperty("bootstrap_servers")
    private List<String> bootstrapServers;

    @JsonProperty("topics")
    @NotNull
    @Size(min = 1, max = 1, message = "Only one topic currently supported for Kafka buffer")
    private List<BufferTopicConfig> topics;

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

    @JsonProperty("drain_timeout")
    private Duration drainTimeout = DEFAULT_DRAIN_TIMEOUT;

    @JsonProperty("custom_metric_prefix")
    private String customMetricPrefix;


    public List<String> getBootstrapServers() {
        if (Objects.nonNull(bootstrapServers)) {
            return bootstrapServers;
        }
        return null;
    }

    @Override
    public AuthConfig getAuthConfig() {
        return authConfig;
    }

    @Override
    @JsonIgnore
    public SchemaConfig getSchemaConfig() {
        return null;
    }

    @Override
    @JsonIgnore
    public String getSerdeFormat() {
        return getTopic().getSerdeFormat().toString();
    }

    @Override
    @JsonIgnore
    public BufferTopicConfig getTopic() {
        return topics.get(0);
    }

    @Override
    public List<? extends TopicConsumerConfig> getTopics() {
        return topics;
    }

    @Override
    public KafkaProducerProperties getKafkaProducerProperties() {
        return kafkaProducerProperties;
    }

    @Override
    @JsonIgnore
    public String getPartitionKey() {
        return "pipeline-buffer";
    }

    @Override
    @JsonIgnore
    public Optional<PluginModel> getDlq() {
        // TODO: move DLQ logic to be sink specific (currently, write to DLQ is handled by KafkaCustomConsumer)
        return Optional.empty();
    }
    @Override
    @JsonIgnore
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
    @JsonIgnore
    public String getClientDnsLookup() {
        return null;
    }

    @Override
    @JsonIgnore
    public boolean getAcknowledgementsEnabled() {
        return true;
    }

    public Duration getDrainTimeout() {
        return drainTimeout;
    }

    @JsonIgnore
    public Optional<String> getCustomMetricPrefix() {
        return Optional.ofNullable(customMetricPrefix);
    }
}
