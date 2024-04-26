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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * An implementation of {@link org.opensearch.dataprepper.model.source.SourceCoordinationStore} that will be used by default if no store is specified. This store is only usable
 * for single node clusters of Data Prepper, and is meant to allow sources to keep their code the same, and to not care whether the source is running in a single or multi-node environment.
 * This is not recommended for use in production environments.
 *
 * @since 2.3
 */
@DataPrepperPlugin(name = "in_memory", pluginType = SourceCoordinationStore.class)
public class InMemorySourceCoordinationStore implements SourceCoordinationStore {

    private static final Logger LOG = LoggerFactory.getLogger(InMemorySourceCoordinationStore.class);

    private final InMemoryPartitionAccessor inMemoryPartitionAccessor;

    @DataPrepperPluginConstructor
    public InMemorySourceCoordinationStore(final PluginSetting pluginSetting) {
        this(new InMemoryPartitionAccessor());
    }

    /**
     * For Testing
     */
    public InMemorySourceCoordinationStore(final InMemoryPartitionAccessor inMemoryPartitionAccessor) {
        this.inMemoryPartitionAccessor = inMemoryPartitionAccessor;
    }

    @Override
    public void initializeStore() {
        LOG.warn("The in_memory source coordination store is not recommended for production workloads. It is only effective in single node environments of Data Prepper, " +
                "and can run into memory limitations over time if the number of partitions is too great.");
    }

    @Override
    public Optional<SourcePartitionStoreItem> getSourcePartitionItem(final String sourceIdentifier, final String partitionKey) {
        synchronized (this) {
            return inMemoryPartitionAccessor.getItem(sourceIdentifier, partitionKey);
        }
    }

    @Override
    public List<SourcePartitionStoreItem> querySourcePartitionItemsByStatus(final String sourceIdentifier, final SourcePartitionStatus sourcePartitionStatus, final String startPartitionPriority) {
        throw new UnsupportedOperationException("querySourcePartitionItemsByStatus is currently not supported in In Memory Store");
    }

    @Override
    public List<SourcePartitionStoreItem> queryAllSourcePartitionItems(String sourceIdentifier) {
        synchronized (this) {
            return inMemoryPartitionAccessor.getAllItem(sourceIdentifier);
        }
    }

    @Override
    public boolean tryCreatePartitionItem(final String sourceIdentifier,
                                          final String partitionKey,
                                          final SourcePartitionStatus sourcePartitionStatus,
                                          final Long closedCount,
                                          final String partitionProgressState,
                                          final boolean isReadOnlyItem) {
        synchronized (this) {
            if (inMemoryPartitionAccessor.getItem(sourceIdentifier, partitionKey).isEmpty()) {
                final InMemorySourcePartitionStoreItem inMemorySourcePartitionStoreItem = new InMemorySourcePartitionStoreItem();
                inMemorySourcePartitionStoreItem.setSourceIdentifier(sourceIdentifier);
                inMemorySourcePartitionStoreItem.setSourcePartitionKey(partitionKey);
                inMemorySourcePartitionStoreItem.setSourcePartitionStatus(sourcePartitionStatus);
                inMemorySourcePartitionStoreItem.setClosedCount(closedCount);
                inMemorySourcePartitionStoreItem.setPartitionProgressState(partitionProgressState);
                inMemoryPartitionAccessor.queuePartition(inMemorySourcePartitionStoreItem);
                return true;
            }
        }

        return false;
    }

    @Override
    public Optional<SourcePartitionStoreItem> tryAcquireAvailablePartition(final String sourceIdentifier,
                                                                           final String ownerId, final Duration ownershipTimeout) {

        synchronized (this) {
            final Optional<SourcePartitionStoreItem> nextItem = inMemoryPartitionAccessor.getNextItem();

            if (nextItem.isPresent()) {
                nextItem.get().setPartitionOwner(ownerId);
                nextItem.get().setPartitionOwnershipTimeout(Instant.now().plus(ownershipTimeout));
                nextItem.get().setSourcePartitionStatus(SourcePartitionStatus.ASSIGNED);
            }

            return nextItem;
        }
    }

    @Override
    public void tryUpdateSourcePartitionItem(final SourcePartitionStoreItem updateItem) {
        synchronized (this) {
            inMemoryPartitionAccessor.updateItem((InMemorySourcePartitionStoreItem) updateItem);
        }
    }

    @Override
    public void tryUpdateSourcePartitionItem(final SourcePartitionStoreItem updateItem, final Instant priorityForUnassignedPartitions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void tryDeletePartitionItem(SourcePartitionStoreItem deleteItem) {
        throw new UnsupportedOperationException("deleting partitions is not currently supported by the in memory source coordination store");
    }
}
