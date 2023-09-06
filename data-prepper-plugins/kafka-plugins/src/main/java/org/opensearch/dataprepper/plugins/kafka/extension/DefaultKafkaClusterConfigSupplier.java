/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.extension;

import org.opensearch.dataprepper.plugins.kafka.configuration.AuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.AwsConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionConfig;

import java.util.List;

public class DefaultKafkaClusterConfigSupplier implements KafkaClusterConfigSupplier {
    private final KafkaClusterConfig kafkaClusterConfig;
    public DefaultKafkaClusterConfigSupplier(KafkaClusterConfig kafkaClusterConfig) {
        this.kafkaClusterConfig = kafkaClusterConfig;
    }

    @Override
    public List<String> getBootStrapServers() {
        return kafkaClusterConfig.getBootStrapServers();
    }

    @Override
    public AuthConfig getAuthConfig() {
        return kafkaClusterConfig.getAuthConfig();
    }

    @Override
    public AwsConfig getAwsConfig() {
        return kafkaClusterConfig.getAwsConfig();
    }

    @Override
    public EncryptionConfig getEncryptionConfig() {
        return kafkaClusterConfig.getEncryptionConfig();
    }
}
