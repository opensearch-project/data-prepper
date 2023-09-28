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

@DataPrepperExtensionPlugin(modelType = KafkaClusterConfig.class, rootKeyJsonPath = "/kafka_cluster_config")
public class KafkaClusterConfigExtension implements ExtensionPlugin {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaClusterConfigExtension.class);
    private DefaultKafkaClusterConfigSupplier defaultKafkaClusterConfigSupplier;

    @DataPrepperPluginConstructor
    public KafkaClusterConfigExtension(final KafkaClusterConfig kafkaClusterConfig) {
        this.defaultKafkaClusterConfigSupplier = new DefaultKafkaClusterConfigSupplier(kafkaClusterConfig);
    }
    @Override
    public void apply(ExtensionPoints extensionPoints) {
        LOG.info("Applying Kafka Cluster Config Extension.");
        extensionPoints.addExtensionProvider(new KafkaClusterConfigProvider(this.defaultKafkaClusterConfigSupplier));
    }
}
