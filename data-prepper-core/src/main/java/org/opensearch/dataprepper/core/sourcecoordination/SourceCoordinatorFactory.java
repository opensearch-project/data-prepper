/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.sourcecoordination;

import org.opensearch.dataprepper.core.parser.model.SourceCoordinationConfig;
import org.opensearch.dataprepper.core.sourcecoordination.enhanced.EnhancedLeaseBasedSourceCoordinator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.source.SourceCoordinationStore;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

/**
 * A factory class that will create the {@link SourceCoordinator} implementation based on the
 * source_coordination configuration
 * @since 2.2
 */
public class SourceCoordinatorFactory {
    private static final Logger LOG = LoggerFactory.getLogger(SourceCoordinatorFactory.class);

    private static final String SOURCE_COORDINATOR_PLUGIN_NAME_FOR_METRICS = "source-coordinator";

    private final SourceCoordinationConfig sourceCoordinationConfig;
    private final PluginFactory pluginFactory;

    public SourceCoordinatorFactory(final SourceCoordinationConfig sourceCoordinationConfig,
                                    final PluginFactory pluginFactory){

        this.sourceCoordinationConfig = sourceCoordinationConfig;
        this.pluginFactory = pluginFactory;
    }

    public <T> SourceCoordinator<T> provideSourceCoordinator(final Class<T> clazz, final String subPipelineName) {
        if (sourceCoordinationConfig == null
                || sourceCoordinationConfig.getSourceCoordinationStoreConfig() == null
                || sourceCoordinationConfig.getSourceCoordinationStoreConfig().getName() == null) {
            return null;
        }

        final SourceCoordinationStore sourceCoordinationStore =
                pluginFactory.loadPlugin(SourceCoordinationStore.class, sourceCoordinationConfig.getSourceCoordinationStoreConfig());

        final PluginMetrics sourceCoordinatorMetrics = PluginMetrics.fromNames(SOURCE_COORDINATOR_PLUGIN_NAME_FOR_METRICS, subPipelineName);

        LOG.info("Creating LeaseBasedSourceCoordinator with coordination store {} for sub-pipeline {}",
                sourceCoordinationConfig.getSourceCoordinationStoreConfig().getName(), subPipelineName);
        return new LeaseBasedSourceCoordinator<T>(clazz, sourceCoordinationStore, sourceCoordinationConfig, subPipelineName, sourceCoordinatorMetrics);
    }

    public EnhancedSourceCoordinator provideEnhancedSourceCoordinator(final Function<SourcePartitionStoreItem, EnhancedSourcePartition> partitionFactory,
                                                                      final String subPipelineName) {
        if (sourceCoordinationConfig == null
                || sourceCoordinationConfig.getSourceCoordinationStoreConfig() == null
                || sourceCoordinationConfig.getSourceCoordinationStoreConfig().getName() == null) {
            return null;
        }

        final SourceCoordinationStore sourceCoordinationStore =
                pluginFactory.loadPlugin(SourceCoordinationStore.class, sourceCoordinationConfig.getSourceCoordinationStoreConfig());

        final PluginMetrics sourceCoordinatorMetrics = PluginMetrics.fromNames(SOURCE_COORDINATOR_PLUGIN_NAME_FOR_METRICS, subPipelineName);

        LOG.info("Creating EnhancedLeaseBasedSourceCoordinator with coordination store {} for sub-pipeline {}",
                sourceCoordinationConfig.getSourceCoordinationStoreConfig().getName(), subPipelineName);
        return new EnhancedLeaseBasedSourceCoordinator(sourceCoordinationStore, sourceCoordinationConfig, sourceCoordinatorMetrics, subPipelineName, partitionFactory);
    }
}
