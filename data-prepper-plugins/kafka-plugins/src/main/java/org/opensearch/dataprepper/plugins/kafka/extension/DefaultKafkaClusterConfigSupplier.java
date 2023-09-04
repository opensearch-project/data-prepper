/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.extension;

import org.opensearch.dataprepper.plugins.kafka.configuration.AuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.AwsConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;

import java.util.List;

public class DefaultKafkaClusterConfigSupplier implements KafkaClusterConfigSupplier {
    private KafkaClusterExtensionConfig kafkaClusterExtensionConfig;
    public DefaultKafkaClusterConfigSupplier(KafkaClusterExtensionConfig kafkaClusterExtensionConfig) {
        this.kafkaClusterExtensionConfig = kafkaClusterExtensionConfig;
    }

    @Override
    public List<String> getBootStrapServers() {
        return kafkaClusterExtensionConfig.getBootStrapServers();
    }

    @Override
    public AuthConfig getAuthConfig() {
        return kafkaClusterExtensionConfig.getAuthConfig();
    }

    @Override
    public AwsConfig getAwsConfig() {
        return kafkaClusterExtensionConfig.getAwsConfig();
    }

    @Override
    public KafkaSourceConfig.EncryptionConfig getEncryptionConfig() {
        return kafkaClusterExtensionConfig.getEncryptionConfig();
    }
}
