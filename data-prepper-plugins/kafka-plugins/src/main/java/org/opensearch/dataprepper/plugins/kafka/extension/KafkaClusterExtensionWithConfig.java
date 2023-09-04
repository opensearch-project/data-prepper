/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.extension;

import org.opensearch.dataprepper.model.annotations.DataPrepperExtensionPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.plugin.ExtensionPlugin;
import org.opensearch.dataprepper.model.plugin.ExtensionPoints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@DataPrepperExtensionPlugin(modelType = KafkaClusterExtensionConfig.class, rootKey = "kafka_cluster_config")
public class KafkaClusterExtensionWithConfig implements ExtensionPlugin {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaClusterExtensionWithConfig.class);
    private DefaultKafkaClusterConfigSupplier defaultKafkaClusterConfigSupplier;

    @DataPrepperPluginConstructor
    public KafkaClusterExtensionWithConfig(final KafkaClusterExtensionConfig kafkaClusterExtensionConfig) {
        this.defaultKafkaClusterConfigSupplier = new DefaultKafkaClusterConfigSupplier(kafkaClusterExtensionConfig);
    }
    @Override
    public void apply(ExtensionPoints extensionPoints) {
        LOG.info("Applying Kafka Cluster Config Extension.");
        extensionPoints.addExtensionProvider(new KafkaClusterExtensionConfigProvider(this.defaultKafkaClusterConfigSupplier));
    }
}
