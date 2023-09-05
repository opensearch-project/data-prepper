/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafkaconnect.source;

import org.apache.kafka.connect.runtime.WorkerConfig;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.kafka.extension.KafkaClusterConfigSupplier;
import org.opensearch.dataprepper.plugins.kafka.util.KafkaSourceSecurityConfigurer;
import org.opensearch.dataprepper.plugins.kafkaconnect.configuration.KafkaConnectSourceConfig;
import org.opensearch.dataprepper.plugins.kafkaconnect.configuration.WorkerProperties;
import org.opensearch.dataprepper.plugins.kafkaconnect.util.Connector;
import org.opensearch.dataprepper.plugins.kafkaconnect.util.KafkaConnect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * The starting point of the debezium-source.
 * The debezium engine is configured and runs async here.
 */

@SuppressWarnings("deprecation")
@DataPrepperPlugin(name = "kafka_connect", pluginType = Source.class, pluginConfigurationType = KafkaConnectSourceConfig.class)
public class KafkaConnectSource implements Source<Record<Object>> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaConnectSource.class);
    private final KafkaConnectSourceConfig sourceConfig;
    private final String pipelineName;
    private KafkaConnect kafkaConnect;

    @DataPrepperPluginConstructor
    public KafkaConnectSource(final KafkaConnectSourceConfig sourceConfig,
                              final PluginMetrics pluginMetrics,
                              final PipelineDescription pipelineDescription,
                              final KafkaClusterConfigSupplier kafkaClusterConfigSupplier) {
        this.sourceConfig = sourceConfig;
        this.pipelineName = pipelineDescription.getPipelineName();
        this.updateConfig(kafkaClusterConfigSupplier);
        kafkaConnect = KafkaConnect.getPipelineInstance(
                pipelineName,
                pluginMetrics,
                sourceConfig.getConnectTimeoutMs(),
                sourceConfig.getConnectorTimeoutMs());
    }

    @Override
    public void start(Buffer<Record<Object>> buffer) {
        LOG.info("Starting Kafka Connect Source for pipeline: {}", pipelineName);
        // Please make sure buildWokerProperties is always first to execute.
        final WorkerProperties workerProperties = this.sourceConfig.getWorkerProperties();
        Map<String, String> workerProps = workerProperties.buildKafkaConnectPropertyMap();
        if (workerProps.get(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG) == null || workerProps.get(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG).isEmpty()) {
            throw new IllegalArgumentException("Bootstrap Servers cannot be null or empty");
        }
        final List<Connector> connectors = this.sourceConfig.getConnectors();
        kafkaConnect.addConnectors(connectors);
        kafkaConnect.initialize(workerProps);
        kafkaConnect.start();
    }

    @Override
    public void stop() {
        LOG.info("Stopping Kafka Connect Source for pipeline: {}", pipelineName);
        kafkaConnect.stop();
    }

    private void updateConfig(final KafkaClusterConfigSupplier kafkaClusterConfigSupplier) {
        if (kafkaClusterConfigSupplier != null) {
            if (sourceConfig.getBootStrapServers() == null && kafkaClusterConfigSupplier.getBootStrapServers() != null) {
                this.sourceConfig.setBootstrapServers(String.join(",", kafkaClusterConfigSupplier.getBootStrapServers()));
            }
            if (sourceConfig.getAuthConfig() == null) {
                sourceConfig.setAuthConfig(kafkaClusterConfigSupplier.getAuthConfig());
            }
            if (sourceConfig.getAwsConfig() == null) {
                sourceConfig.setAwsConfig(kafkaClusterConfigSupplier.getAwsConfig());
            }
            if (sourceConfig.getEncryptionConfig() == null) {
                sourceConfig.setEncryptionConfig(kafkaClusterConfigSupplier.getEncryptionConfig());
            }
        }
        Properties authProperties = new Properties();
        KafkaSourceSecurityConfigurer.setAuthProperties(authProperties, sourceConfig, LOG);
        this.sourceConfig.setAuthProperty(authProperties);
    }
}
