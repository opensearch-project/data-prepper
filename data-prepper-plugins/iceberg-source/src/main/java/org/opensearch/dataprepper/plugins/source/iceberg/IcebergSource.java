/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.iceberg;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
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
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.PartitionFactory;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.partition.LeaderPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.function.Function;

@DataPrepperPlugin(name = "iceberg", pluginType = Source.class, pluginConfigurationType = IcebergSourceConfig.class)
public class IcebergSource implements Source<Record<Event>>, UsesEnhancedSourceCoordination {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergSource.class);

    private final IcebergSourceConfig sourceConfig;
    private final PluginMetrics pluginMetrics;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private EnhancedSourceCoordinator sourceCoordinator;
    private IcebergService icebergService;

    @DataPrepperPluginConstructor
    public IcebergSource(final IcebergSourceConfig sourceConfig,
                         final PluginMetrics pluginMetrics,
                         final AcknowledgementSetManager acknowledgementSetManager) {
        this.sourceConfig = sourceConfig;
        this.pluginMetrics = pluginMetrics;
        this.acknowledgementSetManager = acknowledgementSetManager;
        LOG.info("Creating Iceberg Source for {} table(s)", sourceConfig.getTables().size());
    }

    @Override
    public void start(final Buffer<Record<Event>> buffer) {
        LOG.info("Starting Iceberg Source");
        Objects.requireNonNull(sourceCoordinator);

        sourceCoordinator.createPartition(new LeaderPartition());

        icebergService = new IcebergService(sourceCoordinator, sourceConfig, pluginMetrics, acknowledgementSetManager);
        icebergService.start(buffer);
    }

    @Override
    public void stop() {
        LOG.info("Stopping Iceberg Source");
        if (Objects.nonNull(icebergService)) {
            icebergService.shutdown();
        }
    }

    @Override
    public boolean areAcknowledgementsEnabled() {
        return sourceConfig.isAcknowledgmentsEnabled();
    }

    @Override
    public void setEnhancedSourceCoordinator(final EnhancedSourceCoordinator sourceCoordinator) {
        this.sourceCoordinator = sourceCoordinator;
        this.sourceCoordinator.initialize();
    }

    @Override
    public Function<SourcePartitionStoreItem, EnhancedSourcePartition> getPartitionFactory() {
        return new PartitionFactory();
    }
}
