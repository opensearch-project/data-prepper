/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds;

import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.UsesEnhancedSourceCoordination;
import org.opensearch.dataprepper.plugins.source.rds.coordination.PartitionFactory;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.LeaderPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.function.Function;

@DataPrepperPlugin(name = "rds", pluginType = Source.class, pluginConfigurationType = RdsSourceConfig.class)
public class RdsSource implements Source<Record<Event>>, UsesEnhancedSourceCoordination {

    private static final Logger LOG = LoggerFactory.getLogger(RdsSource.class);

    private final ClientFactory clientFactory;
    private final PluginMetrics pluginMetrics;
    private final RdsSourceConfig sourceConfig;
    private EnhancedSourceCoordinator sourceCoordinator;
    private RdsService rdsService;

    @DataPrepperPluginConstructor
    public RdsSource(final PluginMetrics pluginMetrics,
                     final RdsSourceConfig sourceConfig,
                     final AwsCredentialsSupplier awsCredentialsSupplier) {
        this.pluginMetrics = pluginMetrics;
        this.sourceConfig = sourceConfig;

        clientFactory = new ClientFactory(awsCredentialsSupplier, sourceConfig.getAwsAuthenticationConfig());
    }

    @Override
    public void start(Buffer<Record<Event>> buffer) {
        Objects.requireNonNull(sourceCoordinator);
        sourceCoordinator.createPartition(new LeaderPartition());

        rdsService = new RdsService(sourceCoordinator, sourceConfig, clientFactory, pluginMetrics);

        LOG.info("Start RDS service");
        rdsService.start(buffer);
    }

    @Override
    public void stop() {
        LOG.info("Stop RDS service");
        if (Objects.nonNull(rdsService)) {
            rdsService.shutdown();
        }
    }

    @Override
    public void setEnhancedSourceCoordinator(EnhancedSourceCoordinator sourceCoordinator) {
        this.sourceCoordinator = sourceCoordinator;
        this.sourceCoordinator.initialize();
    }

    @Override
    public Function<SourcePartitionStoreItem, EnhancedSourcePartition> getPartitionFactory() {
        return new PartitionFactory();
    }
}
