/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.sourcecoordination;

import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.model.source.SourceCoordinationStore;
import org.opensearch.dataprepper.parser.model.SourceCoordinationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A factory class that will create the {@link SourceCoordinator} implementation based on the
 * source_coordination configuration
 * @since 2.2
 */
public class SourceCoordinatorFactory {
    private static final Logger LOG = LoggerFactory.getLogger(SourceCoordinatorFactory.class);

    private final SourceCoordinationConfig sourceCoordinationConfig;
    private final PluginFactory pluginFactory;

    public SourceCoordinatorFactory(final SourceCoordinationConfig sourceCoordinationConfig,
                                    final PluginFactory pluginFactory){

        this.sourceCoordinationConfig = sourceCoordinationConfig;
        this.pluginFactory = pluginFactory;
    }

    public <T> SourceCoordinator<T> provideSourceCoordinator(final Class<T> clazz, final String ownerPrefix) {
        if (sourceCoordinationConfig == null
                || sourceCoordinationConfig.getSourceCoordinationStoreConfig() == null
                || sourceCoordinationConfig.getSourceCoordinationStoreConfig().getName() == null) {
            return null;
        }



        final SourceCoordinationStore sourceCoordinationStore =
                pluginFactory.loadPlugin(SourceCoordinationStore.class, sourceCoordinationConfig.getSourceCoordinationStoreConfig());

        LOG.info("Creating LeaseBasedSourceCoordinator with coordination store {} for sub-pipeline {}",
                sourceCoordinationConfig.getSourceCoordinationStoreConfig().getName(), ownerPrefix);
        return new LeaseBasedSourceCoordinator<T>(clazz, sourceCoordinationStore, sourceCoordinationConfig, new PartitionManager<>(), ownerPrefix);
    }
}
