/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafkaconnect.extension;

import org.opensearch.dataprepper.model.annotations.DataPrepperExtensionPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.plugin.ExtensionPlugin;
import org.opensearch.dataprepper.model.plugin.ExtensionPoints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@DataPrepperExtensionPlugin(modelType = KafkaConnectConfig.class, rootKey = "kafka_connect_config")
public class KafkaConnectConfigExtension implements ExtensionPlugin {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaConnectConfigExtension.class);
    private DefaultKafkaConnectConfigSupplier defaultKafkaConnectConfigSupplier;

    @DataPrepperPluginConstructor
    public KafkaConnectConfigExtension(final KafkaConnectConfig kafkaConnectConfig) {
        this.defaultKafkaConnectConfigSupplier = new DefaultKafkaConnectConfigSupplier(kafkaConnectConfig);
    }

    @Override
    public void apply(ExtensionPoints extensionPoints) {
        LOG.info("Applying Kafka Connect Config Extension.");
        extensionPoints.addExtensionProvider(new KafkaConnectConfigProvider(this.defaultKafkaConnectConfigSupplier));
    }
}
