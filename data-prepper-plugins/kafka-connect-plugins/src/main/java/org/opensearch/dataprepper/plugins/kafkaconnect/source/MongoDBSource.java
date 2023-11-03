/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafkaconnect.source;

import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.UsesSourceCoordination;
import org.opensearch.dataprepper.plugins.kafka.extension.KafkaClusterConfigSupplier;
import org.opensearch.dataprepper.plugins.kafkaconnect.configuration.MongoDBConfig;
import org.opensearch.dataprepper.plugins.kafkaconnect.extension.KafkaConnectConfigSupplier;
import org.opensearch.dataprepper.plugins.kafkaconnect.source.mongoDB.MongoDBService;
import org.opensearch.dataprepper.plugins.kafkaconnect.source.mongoDB.MongoDBSnapshotProgressState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * The starting point of the mysql source which ingest CDC data using Kafka Connect and Debezium Connector.
 */
@SuppressWarnings("deprecation")
@DataPrepperPlugin(name = "mongodb", pluginType = Source.class, pluginConfigurationType = MongoDBConfig.class)
public class MongoDBSource extends KafkaConnectSource implements UsesSourceCoordination {
    private static final Logger LOG = LoggerFactory.getLogger(MongoDBSource.class);

    private final AwsCredentialsSupplier awsCredentialsSupplier;

    private final PluginMetrics pluginMetrics;

    private final MongoDBConfig mongoDBConfig;

    private final AcknowledgementSetManager acknowledgementSetManager;

    private MongoDBService mongoDBService;

    private SourceCoordinator<MongoDBSnapshotProgressState> sourceCoordinator;

    @DataPrepperPluginConstructor
    public MongoDBSource(final MongoDBConfig mongoDBConfig,
                         final PluginMetrics pluginMetrics,
                         final PipelineDescription pipelineDescription,
                         final AcknowledgementSetManager acknowledgementSetManager,
                         final AwsCredentialsSupplier awsCredentialsSupplier,
                         final KafkaClusterConfigSupplier kafkaClusterConfigSupplier,
                         final KafkaConnectConfigSupplier kafkaConnectConfigSupplier) {
        super(mongoDBConfig, pluginMetrics, pipelineDescription, kafkaClusterConfigSupplier, kafkaConnectConfigSupplier);
        this.pluginMetrics = pluginMetrics;
        this.mongoDBConfig = mongoDBConfig;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.awsCredentialsSupplier = awsCredentialsSupplier;
    }

    @Override
    public void start(Buffer<Record<Object>> buffer) {
        super.start(buffer);
        if (shouldStartInitialLoad()) {
            LOG.info("Starting initial load");
            this.mongoDBService = MongoDBService.create(mongoDBConfig, sourceCoordinator, buffer, acknowledgementSetManager, pluginMetrics);
            this.mongoDBService.start();
        }
    }

    @Override
    public void stop(){
        super.stop();
        if (shouldStartInitialLoad() && Objects.nonNull(mongoDBService) && Objects.nonNull(sourceCoordinator)) {
            LOG.info("Stopping initial load");
            mongoDBService.stop();
            sourceCoordinator.giveUpPartitions();
        }
    }

    @Override
    public <T> void setSourceCoordinator(final SourceCoordinator<T> sourceCoordinator) {
        this.sourceCoordinator = (SourceCoordinator<MongoDBSnapshotProgressState>) sourceCoordinator;
    }

    @Override
    public Class<?> getPartitionProgressStateClass() {
        return MongoDBSnapshotProgressState.class;
    }

    @Override
    public boolean shouldStartKafkaConnect() {
        return false;
//        return mongoDBConfig.getSnapshotMode().equals("export_stream") || mongoDBConfig.getSnapshotMode().equals("stream");
    }

    private boolean shouldStartInitialLoad() {
        return mongoDBConfig.getIngestionMode().equals("export_stream") || mongoDBConfig.getIngestionMode().equals("export");
    }
}
