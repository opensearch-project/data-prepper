/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb;

import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.UsesEnhancedSourceCoordination;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.PartitionFactory;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.InitPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.function.Function;

@DataPrepperPlugin(name = "dynamodb", pluginType = Source.class, pluginConfigurationType = DynamoDBSourceConfig.class)
public class DynamoDBSource implements Source<Record<Event>>, UsesEnhancedSourceCoordination {

    private static final Logger LOG = LoggerFactory.getLogger(DynamoDBSource.class);

    private final PluginMetrics pluginMetrics;

    private final DynamoDBSourceConfig sourceConfig;

    private final PluginFactory pluginFactory;

    private final ClientFactory clientFactory;

    private final AcknowledgementSetManager acknowledgementSetManager;

    private EnhancedSourceCoordinator coordinator;

    private DynamoDBService dynamoDBService;


    @DataPrepperPluginConstructor
    public DynamoDBSource(final PluginMetrics pluginMetrics,
                          final DynamoDBSourceConfig sourceConfig,
                          final PluginFactory pluginFactory,
                          final AwsCredentialsSupplier awsCredentialsSupplier,
                          final AcknowledgementSetManager acknowledgementSetManager) {
        LOG.info("Create DynamoDB Source");
        this.pluginMetrics = pluginMetrics;
        this.sourceConfig = sourceConfig;
        this.pluginFactory = pluginFactory;
        this.acknowledgementSetManager = acknowledgementSetManager;

        clientFactory = new ClientFactory(awsCredentialsSupplier, sourceConfig.getAwsAuthenticationConfig(), sourceConfig.getTableConfigs().get(0).getExportConfig());
    }

    @Override
    public void start(Buffer<Record<Event>> buffer) {
        Objects.requireNonNull(coordinator);

        coordinator.createPartition(new InitPartition());

        // Create DynamoDB Service
        dynamoDBService = new DynamoDBService(coordinator, clientFactory, sourceConfig, pluginMetrics, acknowledgementSetManager);
        dynamoDBService.init();

        LOG.info("Start DynamoDB service");
        dynamoDBService.start(buffer);
    }


    @Override
    public void stop() {
        LOG.info("Stop DynamoDB Source");
        if (Objects.nonNull(dynamoDBService)) {
            dynamoDBService.shutdown();
        }

    }

    @Override
    public void setEnhancedSourceCoordinator(final EnhancedSourceCoordinator sourceCoordinator) {
        coordinator = sourceCoordinator;
        coordinator.initialize();
    }

    @Override
    public Function<SourcePartitionStoreItem, EnhancedSourcePartition> getPartitionFactory() {
        return new PartitionFactory();
    }
}
