/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sourcecoordinator.dynamodb;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.source.PartitionIdentifier;
import org.opensearch.dataprepper.model.source.SourceCoordinator;
import org.opensearch.dataprepper.model.source.SourcePartition;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * An implementation of {@link org.opensearch.dataprepper.model.source.SourceCoordinator} when DynamoDB is used at the distributed store
 * for source_coordination
 * @since 2.2
 */

@DataPrepperPlugin(name = "dynamodb", pluginType = SourceCoordinator.class, pluginConfigurationType = DynamoStoreSettings.class )
public class DynamoDbSourceCoordinator<T> implements SourceCoordinator<T> {

    private final DynamoStoreSettings dynamoStoreSettings;
    private final PluginMetrics pluginMetrics;

    @DataPrepperPluginConstructor
    public DynamoDbSourceCoordinator(final DynamoStoreSettings dynamoStoreSettings, final PluginMetrics pluginMetrics) {
        this.dynamoStoreSettings = dynamoStoreSettings;
        this.pluginMetrics = pluginMetrics;
    }

    @Override
    public void createPartitions(final List<PartitionIdentifier> partitionIdentifierKeys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<SourcePartition<T>> getNextPartition() {
        return Optional.empty();
    }

    @Override
    public void completePartition(PartitionIdentifier partitionIdentifier) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void closePartition(PartitionIdentifier partitionIdentifier, Duration reopenAfter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <S extends T> void saveProgressStateForPartition(PartitionIdentifier partitionIdentifier, S partitionState) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void giveUpPartitions() {
        throw new UnsupportedOperationException();
    }
}
