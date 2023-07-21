/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.util.List;
import java.util.Objects;
import java.time.Duration;

/**
 * * A helper class that helps to read user configuration values from
 * pipelines.yaml
 */

public class KafkaSourceConfig {
    public static final Duration DEFAULT_ACKNOWLEDGEMENTS_TIMEOUT = Duration.ofSeconds(30);

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
    private EncryptionType encryptionType = EncryptionType.SSL;

    @JsonProperty("aws")
    @Valid
    private AwsConfig awsConfig;

    @JsonProperty("acknowledgments")
    private Boolean acknowledgementsEnabled = false;

    @JsonProperty("acknowledgments_timeout")
    private Duration acknowledgementsTimeout = DEFAULT_ACKNOWLEDGEMENTS_TIMEOUT;

    public Boolean getAcknowledgementsEnabled() {
        return acknowledgementsEnabled;
    }

    public Duration getAcknowledgementsTimeout() {
        return acknowledgementsTimeout;
    }

    public List<TopicConfig> getTopics() {
        return topics;
    }

    public void setTopics(List<TopicConfig> topics) {
        this.topics = topics;
    }

    public String getBootStrapServers() {
        if (Objects.nonNull(bootStrapServers)) {
            return String.join(",", bootStrapServers);
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

    public EncryptionType getEncryptionType() {
        return encryptionType;
    }

    public AwsConfig getAwsConfig() {
        return awsConfig;
    }

    public void setAuthConfig(AuthConfig authConfig) {
        this.authConfig = authConfig;
    }
}
