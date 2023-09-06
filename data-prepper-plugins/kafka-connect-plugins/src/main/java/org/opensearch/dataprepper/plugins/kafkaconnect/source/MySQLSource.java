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
import org.opensearch.dataprepper.plugins.kafkaconnect.configuration.MySQLConfig;
import org.opensearch.dataprepper.plugins.kafkaconnect.extension.KafkaConnectConfigSupplier;

/**
 * The starting point of the mysql source which ingest CDC data using Kafka Connect and Debezium Connector.
 */
@SuppressWarnings("deprecation")
@DataPrepperPlugin(name = "mysql", pluginType = Source.class, pluginConfigurationType = MySQLConfig.class)
public class MySQLSource extends KafkaConnectSource {

    @DataPrepperPluginConstructor
    public MySQLSource(final MySQLConfig mySQLConfig,
                       final PluginMetrics pluginMetrics,
                       final PipelineDescription pipelineDescription,
                       final KafkaClusterConfigSupplier kafkaClusterConfigSupplier,
                       final KafkaConnectConfigSupplier kafkaConnectConfigSupplier) {
        super(mySQLConfig, pluginMetrics, pipelineDescription, kafkaClusterConfigSupplier, kafkaConnectConfigSupplier);
    }
}
