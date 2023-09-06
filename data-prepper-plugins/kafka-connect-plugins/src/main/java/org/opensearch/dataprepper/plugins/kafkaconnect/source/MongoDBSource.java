/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafkaconnect.source;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.kafka.extension.KafkaClusterConfigSupplier;
import org.opensearch.dataprepper.plugins.kafkaconnect.configuration.MongoDBConfig;
import org.opensearch.dataprepper.plugins.kafkaconnect.extension.KafkaConnectConfigSupplier;

/**
 * The starting point of the mysql source which ingest CDC data using Kafka Connect and Debezium Connector.
 */
@SuppressWarnings("deprecation")
@DataPrepperPlugin(name = "mongodb", pluginType = Source.class, pluginConfigurationType = MongoDBConfig.class)
public class MongoDBSource extends KafkaConnectSource {

    @DataPrepperPluginConstructor
    public MongoDBSource(final MongoDBConfig mongoDBConfig,
                         final PluginMetrics pluginMetrics,
                         final PipelineDescription pipelineDescription,
                         final KafkaClusterConfigSupplier kafkaClusterConfigSupplier,
                         final KafkaConnectConfigSupplier kafkaConnectConfigSupplier) {
        super(mongoDBConfig, pluginMetrics, pipelineDescription, kafkaClusterConfigSupplier, kafkaConnectConfigSupplier);
    }
}
