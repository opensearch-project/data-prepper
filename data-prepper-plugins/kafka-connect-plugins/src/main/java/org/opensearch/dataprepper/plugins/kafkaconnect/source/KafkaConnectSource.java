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
import org.opensearch.dataprepper.model.plugin.kafka.AuthConfig;
import org.opensearch.dataprepper.model.plugin.kafka.AwsConfig;
import org.opensearch.dataprepper.model.plugin.kafka.EncryptionConfig;
import org.opensearch.dataprepper.model.plugin.kafka.UsesKafkaClusterConfig;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.kafka.util.KafkaSourceSecurityConfigurer;
import org.opensearch.dataprepper.plugins.kafkaconnect.util.Connector;
import org.opensearch.dataprepper.plugins.kafkaconnect.configuration.KafkaConnectSourceConfig;
import org.opensearch.dataprepper.plugins.kafkaconnect.configuration.WorkerProperties;
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
public class KafkaConnectSource implements Source<Record<Object>>, UsesKafkaClusterConfig {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaConnectSource.class);
    private static final String KAFKA_CONNECT_SOURCE_CONFIG_PREFIX = "producer.";
    private final KafkaConnectSourceConfig sourceConfig;
    private final String pipelineName;
    private KafkaConnect kafkaConnect;

    @DataPrepperPluginConstructor
    public KafkaConnectSource(final KafkaConnectSourceConfig sourceConfig,
                              final PluginMetrics pluginMetrics,
                              final PipelineDescription pipelineDescription) {
        this.sourceConfig = sourceConfig;
        this.pipelineName = pipelineDescription.getPipelineName();
        kafkaConnect = KafkaConnect.getPipelineInstance(pipelineName, pluginMetrics);
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

    @Override
    public void setBootstrapServers(final List<String> bootstrapServers) {
        if (bootstrapServers == null) return;
        this.sourceConfig.setBootstrapServers(String.join(",", bootstrapServers));
    }

    @Override
    public void setKafkaClusterAuthConfig(final AuthConfig authConfig,
                                          final AwsConfig awsConfig,
                                          final EncryptionConfig encryptionConfig) {
        this.sourceConfig.setAuthConfig(authConfig);
        this.sourceConfig.setAwsConfig(awsConfig);
        this.sourceConfig.setEncryptionConfig(encryptionConfig);
        Properties authProperties = new Properties();
        KafkaSourceSecurityConfigurer.setAuthProperties(authProperties, sourceConfig, LOG);
        this.sourceConfig.setAuthProperty(authProperties);
    }
}
