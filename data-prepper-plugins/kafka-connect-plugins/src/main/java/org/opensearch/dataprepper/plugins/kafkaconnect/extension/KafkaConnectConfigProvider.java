/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafkaconnect.extension;

import org.opensearch.dataprepper.model.plugin.ExtensionProvider;

import java.util.Optional;

public class KafkaConnectConfigProvider implements ExtensionProvider<KafkaConnectConfigSupplier> {
    private final KafkaConnectConfigSupplier kafkaConnectConfigSupplier;
    public KafkaConnectConfigProvider(KafkaConnectConfigSupplier kafkaConnectConfigSupplier) {
        this.kafkaConnectConfigSupplier = kafkaConnectConfigSupplier;
    }

    @Override
    public Optional<KafkaConnectConfigSupplier> provideInstance(Context context) {
        return Optional.of(this.kafkaConnectConfigSupplier);
    }

    @Override
    public Class<KafkaConnectConfigSupplier> supportedClass() {
        return KafkaConnectConfigSupplier.class;
    }
}
