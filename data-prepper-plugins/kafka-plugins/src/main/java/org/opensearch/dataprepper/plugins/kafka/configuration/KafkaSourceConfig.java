/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import org.opensearch.dataprepper.plugins.kafka.util.KafkaClusterAuthConfig;

import java.util.List;
import java.util.Objects;

/**
 * * A helper class that helps to read user configuration values from
 * pipelines.yaml
 */

public class KafkaSourceConfig implements KafkaConsumerConfig, KafkaClusterAuthConfig {

    @JsonProperty("bootstrap_servers")
    private List<String> bootStrapServers;

    @JsonProperty("topics")
    @NotNull
    @Size(min = 1, max = 10, message = "The number of Topics should be between 1 and 10")
    private List<TopicConfig> topics;

    @JsonProperty("schema")
    @Valid
    private SchemaConfig schemaConfig;

    @Valid
    @JsonProperty("authentication")
    private AuthConfig authConfig;

    @JsonProperty("encryption")
    private EncryptionConfig encryptionConfig;

    @JsonProperty("aws")
    @Valid
    private AwsConfig awsConfig;

    @JsonProperty("acknowledgments")
    private Boolean acknowledgementsEnabled = false;

    @JsonProperty("client_dns_lookup")
    private String clientDnsLookup;

    public String getClientDnsLookup() {
        return clientDnsLookup;
    }

    public boolean getAcknowledgementsEnabled() {
        return acknowledgementsEnabled;
    }

    public List<TopicConfig> getTopics() {
        return topics;
    }

    public void setTopics(List<TopicConfig> topics) {
        this.topics = topics;
    }

    public List<String> getBootstrapServers() {
        if (Objects.nonNull(bootStrapServers)) {
            return bootStrapServers;
        }
        return null;
    }

    public void setBootStrapServers(List<String> bootStrapServers) {
        this.bootStrapServers = bootStrapServers;
    }

    public SchemaConfig getSchemaConfig() {
        return schemaConfig;
    }

    public void setSchemaConfig(SchemaConfig schemaConfig) {
        this.schemaConfig = schemaConfig;
    }

    public AuthConfig getAuthConfig() {
        return authConfig;
    }

    public EncryptionConfig getEncryptionConfig() {
        if (Objects.isNull(encryptionConfig)) {
            return new EncryptionConfig();
        }
        return encryptionConfig;
    }

    public EncryptionConfig getEncryptionConfigRaw() {
        return encryptionConfig;
    }

    public AwsConfig getAwsConfig() {
        return awsConfig;
    }

    public void setAuthConfig(AuthConfig authConfig) {
        this.authConfig = authConfig;
    }

    public void setAwsConfig(AwsConfig awsConfig) {
        this.awsConfig = awsConfig;
    }

    public void setEncryptionConfig(EncryptionConfig encryptionConfig) {
        this.encryptionConfig = encryptionConfig;
    }
}
