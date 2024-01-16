/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafkaconnect.extension;

public class DefaultKafkaConnectConfigSupplier implements KafkaConnectConfigSupplier {
    private final KafkaConnectConfig kafkaConnectConfig;
    public DefaultKafkaConnectConfigSupplier(KafkaConnectConfig kafkaConnectConfig) {
        this.kafkaConnectConfig = kafkaConnectConfig;
    }

    @Override
    public KafkaConnectConfig getConfig() {
        return this.kafkaConnectConfig;
    }
}
