/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.extension;

import org.opensearch.dataprepper.model.plugin.ExtensionProvider;

import java.util.Optional;

public class KafkaClusterConfigProvider implements ExtensionProvider<KafkaClusterConfigSupplier> {
    private final KafkaClusterConfigSupplier kafkaClusterConfigSupplier;

    public KafkaClusterConfigProvider(KafkaClusterConfigSupplier kafkaClusterConfigSupplier) {
        this.kafkaClusterConfigSupplier = kafkaClusterConfigSupplier;
    }

    @Override
    public Optional<KafkaClusterConfigSupplier> provideInstance(Context context) {
        return Optional.of(this.kafkaClusterConfigSupplier);
    }

    @Override
    public Class<KafkaClusterConfigSupplier> supportedClass() {
        return KafkaClusterConfigSupplier.class;
    }
}
