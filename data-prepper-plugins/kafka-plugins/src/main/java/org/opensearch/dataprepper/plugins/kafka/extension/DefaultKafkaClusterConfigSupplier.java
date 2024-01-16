/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.extension;

import org.opensearch.dataprepper.plugins.kafka.configuration.AuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.AwsConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionConfig;

import java.util.List;
import java.util.Objects;

public class DefaultKafkaClusterConfigSupplier implements KafkaClusterConfigSupplier {
    private final KafkaClusterConfig kafkaClusterConfig;
    public DefaultKafkaClusterConfigSupplier(KafkaClusterConfig kafkaClusterConfig) {
        this.kafkaClusterConfig = kafkaClusterConfig;
    }

    @Override
    public List<String> getBootStrapServers() {
        return Objects.nonNull(kafkaClusterConfig) ? kafkaClusterConfig.getBootStrapServers() : null;
    }

    @Override
    public AuthConfig getAuthConfig() {
        return Objects.nonNull(kafkaClusterConfig) ? kafkaClusterConfig.getAuthConfig() : null;
    }

    @Override
    public AwsConfig getAwsConfig() {
        return Objects.nonNull(kafkaClusterConfig) ? kafkaClusterConfig.getAwsConfig() : null;
    }

    @Override
    public EncryptionConfig getEncryptionConfig() {
        return Objects.nonNull(kafkaClusterConfig) ? kafkaClusterConfig.getEncryptionConfig() : null;
    }
}
