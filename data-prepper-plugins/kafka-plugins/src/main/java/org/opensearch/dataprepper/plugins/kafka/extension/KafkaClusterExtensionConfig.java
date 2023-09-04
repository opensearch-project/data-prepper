/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.extension;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import org.opensearch.dataprepper.plugins.kafka.configuration.AuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.AwsConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;

import java.util.List;

public class KafkaClusterExtensionConfig {
    @JsonProperty("bootstrap_servers")
    private List<String> bootstrapServers;

    @Valid
    @JsonProperty("authentication")
    private AuthConfig authConfig;

    @JsonProperty("encryption")
    private KafkaSourceConfig.EncryptionConfig encryptionConfig;

    @JsonProperty("aws")
    @Valid
    private AwsConfig awsConfig;

    public List<String> getBootStrapServers() {
        return bootstrapServers;
    }

    public AuthConfig getAuthConfig() {
        return authConfig;
    }

    public AwsConfig getAwsConfig() {
        return awsConfig;
    }

    public KafkaSourceConfig.EncryptionConfig getEncryptionConfig() {
        return encryptionConfig;
    }
}
