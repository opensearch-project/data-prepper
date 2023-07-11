/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import org.apache.commons.lang3.ObjectUtils;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.kafka.AuthConfig;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * * A helper class that helps to read user configuration values from
 * pipelines.yaml
 */

public class KafkaSinkConfig {

    public static final String DLQ = "dlq";

    @JsonProperty("bootstrap_servers")
    @NotNull
    @Size(min = 1, message = "Bootstrap servers can't be empty")
    private List<String> bootStrapServers;

    private PluginModel dlq;

    public Optional<PluginModel> getDlq() {
        return Optional.ofNullable(dlq);
    }

    public void setDlqConfig(final PluginSetting pluginSetting) {
        final LinkedHashMap<String, Map<String, Object>> dlq = (LinkedHashMap) pluginSetting.getAttributeFromSettings(DLQ);
        if (dlq != null) {
            if (dlq.size() != 1) {
                throw new RuntimeException("dlq option must declare exactly one dlq configuration");
            }
            final Map.Entry<String, Map<String, Object>> entry = dlq.entrySet().stream()
                    .findFirst()
                    .get();

            this.dlq = new PluginModel(entry.getKey(), entry.getValue());

        }
    }


    @JsonProperty("thread_wait_time")
    private Long threadWaitTime;


    @JsonProperty("topic")
    TopicConfig topic;

    @JsonProperty("authentication")
    private AuthConfig authConfig;

    @JsonProperty("schema")
    @Valid
    private SchemaConfig schemaConfig;

    @JsonProperty("serde_format")
    private String serdeFormat;


    @JsonProperty("partition_key")
    @NotNull
    @NotEmpty
    private String partitionKey;

    @JsonProperty("producer_properties")
    private KafkaProducerProperties kafkaProducerProperties;

    public SchemaConfig getSchemaConfig() {
        return schemaConfig;
    }


    public AuthConfig getAuthConfig() {
        return authConfig;
    }


    public List<String> getBootStrapServers() {
        return bootStrapServers;
    }

    public String getSerdeFormat() {
        if (ObjectUtils.isEmpty(serdeFormat)) {
            serdeFormat = "plaintext";
        }
        return serdeFormat;
    }

    public Long getThreadWaitTime() {
        return threadWaitTime;
    }

    public void setBootStrapServers(List<String> bootStrapServers) {
        this.bootStrapServers = bootStrapServers;
    }

    public void setThreadWaitTime(Long threadWaitTime) {
        this.threadWaitTime = threadWaitTime;
    }

    public void setAuthConfig(AuthConfig authConfig) {
        this.authConfig = authConfig;
    }

    public void setSchemaConfig(SchemaConfig schemaConfig) {
        this.schemaConfig = schemaConfig;
    }

    public TopicConfig getTopic() {
        return topic;
    }

    public void setTopic(TopicConfig topic) {
        this.topic = topic;
    }

    public KafkaProducerProperties getKafkaProducerProperties() {
        return kafkaProducerProperties;
    }

    public String getPartitionKey() {
        return partitionKey;
    }
}
