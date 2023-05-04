/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sourcecoordinator.inmemory;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.source.SourceCoordinationStore;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStatus;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * An implementation of {@link org.opensearch.dataprepper.model.source.SourceCoordinationStore} that will be used by default if no store is specified. This store is only usable
 * for single node clusters of Data Prepper, and is meant to allow sources to keep their code the same, and to not care whether the source is running in a single or multi-node environment.
 * This is not recommended for use in production environments.
 * @since 2.3
 */
@DataPrepperPlugin(name = "in_memory", pluginType = SourceCoordinationStore.class)
public class InMemorySourceCoordinationStore implements SourceCoordinationStore {

    private final InMemoryPartitionAccessor inMemoryPartitionAccessor;

    @DataPrepperPluginConstructor
    public InMemorySourceCoordinationStore(final PluginSetting pluginSetting) {
        this(new InMemoryPartitionAccessor());
    }

    // For testing
    public InMemorySourceCoordinationStore(final InMemoryPartitionAccessor inMemoryPartitionAccessor) {
        this.inMemoryPartitionAccessor = inMemoryPartitionAccessor;
    }

    @Override
    public void initializeStore() {

    }

    @Override
    public Optional<SourcePartitionStoreItem> getSourcePartitionItem(final String partitionKey) {
        return inMemoryPartitionAccessor.getItem(partitionKey);
    }

    @Override
    public boolean tryCreatePartitionItem(final String partitionKey, final SourcePartitionStatus sourcePartitionStatus, final Long closedCount, final String partitionProgressState) {
        final InMemorySourcePartitionStoreItem inMemorySourcePartitionStoreItem = new InMemorySourcePartitionStoreItem();
        inMemorySourcePartitionStoreItem.setSourcePartitionKey(partitionKey);
        inMemorySourcePartitionStoreItem.setSourcePartitionStatus(sourcePartitionStatus);
        inMemorySourcePartitionStoreItem.setClosedCount(closedCount);
        inMemorySourcePartitionStoreItem.setPartitionProgressState(partitionProgressState);

        if (inMemoryPartitionAccessor.getItem(partitionKey).isEmpty()) {
            inMemoryPartitionAccessor.queuePartition(inMemorySourcePartitionStoreItem);
            return true;
        }

        return false;
    }

    @Override
    public Optional<SourcePartitionStoreItem> tryAcquireAvailablePartition(final String ownerId, final Duration ownershipTimeout) {
        final Optional<SourcePartitionStoreItem> nextItem = inMemoryPartitionAccessor.getNextItem();

        if (nextItem.isPresent()) {
            nextItem.get().setPartitionOwner(ownerId);
            nextItem.get().setPartitionOwnershipTimeout(Instant.now().plus(ownershipTimeout));
            nextItem.get().setSourcePartitionStatus(SourcePartitionStatus.ASSIGNED);
        }

        return nextItem;
    }

    @Override
    public void tryUpdateSourcePartitionItem(final SourcePartitionStoreItem updateItem) {
        inMemoryPartitionAccessor.updateItem((InMemorySourcePartitionStoreItem) updateItem);
    }
}
