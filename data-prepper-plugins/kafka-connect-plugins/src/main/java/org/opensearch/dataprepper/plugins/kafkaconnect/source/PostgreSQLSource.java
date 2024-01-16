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
import org.opensearch.dataprepper.plugins.kafkaconnect.configuration.PostgreSQLConfig;
import org.opensearch.dataprepper.plugins.kafkaconnect.extension.KafkaConnectConfigSupplier;

/**
 * The starting point of the mysql source which ingest CDC data using Kafka Connect and Debezium Connector.
 */
@SuppressWarnings("deprecation")
@DataPrepperPlugin(name = "postgresql", pluginType = Source.class, pluginConfigurationType = PostgreSQLConfig.class)
public class PostgreSQLSource extends KafkaConnectSource {

    @DataPrepperPluginConstructor
    public PostgreSQLSource(final PostgreSQLConfig postgreSQLConfig,
                            final PluginMetrics pluginMetrics,
                            final PipelineDescription pipelineDescription,
                            final KafkaClusterConfigSupplier kafkaClusterConfigSupplier,
                            final KafkaConnectConfigSupplier kafkaConnectConfigSupplier) {
        super(postgreSQLConfig, pluginMetrics, pipelineDescription, kafkaClusterConfigSupplier, kafkaConnectConfigSupplier);
    }
}
