/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb;

import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.model.source.SourceCoordinationStore;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.DefaultEnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.PartitionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

@DataPrepperPlugin(name = "dynamodb", pluginType = Source.class, pluginConfigurationType = DynamoDBSourceConfig.class)
public class DynamoDBSource implements Source<Record<Event>> {

    private static final Logger LOG = LoggerFactory.getLogger(DynamoDBSource.class);

    private static final String SOURCE_COORDINATOR_METRIC_PREFIX = "source-coordinator";

    private final PluginMetrics pluginMetrics;

    private final DynamoDBSourceConfig sourceConfig;

    private final PluginFactory pluginFactory;

    private final SourceCoordinationStore coordinationStore;

    private final EnhancedSourceCoordinator coordinator;

    private final DynamoDBService dynamoDBService;


    @DataPrepperPluginConstructor
    public DynamoDBSource(PluginMetrics pluginMetrics, final DynamoDBSourceConfig sourceConfig, final PluginFactory pluginFactory, final PluginSetting pluginSetting, final AwsCredentialsSupplier awsCredentialsSupplier) {
        LOG.info("Create DynamoDB Source");
        this.pluginMetrics = pluginMetrics;
        this.sourceConfig = sourceConfig;
        this.pluginFactory = pluginFactory;


        // Load Coordination Store via PluginFactory
        // This part will be updated.
        PluginSetting sourceCoordinationStoreSetting = new PluginSetting(sourceConfig.getCoordinationStoreConfig().getPluginName(), sourceConfig.getCoordinationStoreConfig().getPluginSettings());
        sourceCoordinationStoreSetting.setPipelineName(SOURCE_COORDINATOR_METRIC_PREFIX);
        coordinationStore = pluginFactory.loadPlugin(SourceCoordinationStore.class, sourceCoordinationStoreSetting);
        String pipelineName = pluginSetting.getPipelineName();

        // Create and initialize coordinator
        coordinator = new DefaultEnhancedSourceCoordinator(coordinationStore, pipelineName, new PartitionFactory());
        coordinator.initialize();

        ClientFactory clientFactory = new ClientFactory(awsCredentialsSupplier, sourceConfig.getAwsAuthenticationConfig());

        // Create DynamoDB Service
        dynamoDBService = new DynamoDBService(coordinator, clientFactory, sourceConfig, pluginMetrics);
        dynamoDBService.init();
    }

    @Override
    public void start(Buffer<Record<Event>> buffer) {
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

}
