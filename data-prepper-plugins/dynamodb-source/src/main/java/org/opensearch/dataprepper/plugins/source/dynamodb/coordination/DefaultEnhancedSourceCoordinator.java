/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.coordination;

import org.opensearch.dataprepper.model.source.SourceCoordinationStore;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStatus;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.InitPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;


/**
 * An implemetation of {@link EnhancedSourceCoordinator} backend by {@link SourceCoordinationStore}
 */
public class DefaultEnhancedSourceCoordinator implements EnhancedSourceCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultEnhancedSourceCoordinator.class);

    //Default time out duration for lease.
    private static final Duration DEFAULT_LEASE_TIMEOUT = Duration.ofMinutes(10);

    // Default identifier For global state
    private static final String DEFAULT_GLOBAL_STATE_PARTITION_TYPE = "GLOBAL";

    private final SourceCoordinationStore coordinationStore;

    /**
     * A unique identifier for a source, normally the pipeline name/id.
     * As the coordination store may be shared in different pipelines.
     */
    private final String sourceIdentifier;

    /**
     * In order to support different types of partitions.
     * A custom factory is required to map a {@link SourcePartitionStoreItem} to a {@link SourcePartition}
     */
    private final Function<SourcePartitionStoreItem, SourcePartition> partitionFactory;

    /**
     * Use host name of the node as the default ownerId
     */
    static final String hostName;


    static {
        try {
            hostName = InetAddress.getLocalHost().getHostName();
        } catch (final UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }


    public DefaultEnhancedSourceCoordinator(final SourceCoordinationStore coordinationStore, String sourceIdentifier, Function<SourcePartitionStoreItem, SourcePartition> partitionFactory) {
        this.coordinationStore = coordinationStore;
        this.sourceIdentifier = sourceIdentifier;
        this.partitionFactory = partitionFactory;

    }

    @Override
    public void initialize() {
        coordinationStore.initializeStore();
        createPartition(new InitPartition());
    }

    @Override
    public <T> boolean createPartition(SourcePartition<T> partition) {
        String partitionType = partition.getPartitionType() == null ? DEFAULT_GLOBAL_STATE_PARTITION_TYPE : partition.getPartitionType();
        // Don't need the status for Global state which is not for lease.
        SourcePartitionStatus status = partition.getPartitionType() == null ? null : SourcePartitionStatus.UNASSIGNED;

        boolean partitionCreated = coordinationStore.tryCreatePartitionItem(
                this.sourceIdentifier + "|" + partitionType,
                partition.getPartitionKey(),
                status,
                0L,
                partition.convertPartitionProgressStatetoString(partition.getProgressState())
        );
        return partitionCreated;

    }


    @Override
    public Optional<SourcePartition> acquireAvailablePartition(String partitionType) {
        // Not available for global state.
        Objects.nonNull(partitionType);
        LOG.debug("Try to acquire an available {} partition", partitionType);
        Optional<SourcePartitionStoreItem> sourceItem = coordinationStore.tryAcquireAvailablePartition(this.sourceIdentifier + "|" + partitionType, hostName, DEFAULT_LEASE_TIMEOUT);
        if (sourceItem.isEmpty()) {
            LOG.info("Partition owner {} failed to acquire a partition, no available {} partitions now", hostName, partitionType);
            return Optional.empty();
        }

        return Optional.of(partitionFactory.apply(sourceItem.get()));
    }


    @Override
    public <T> void saveProgressStateForPartition(SourcePartition<T> partition) {
        String partitionType = partition.getPartitionType() == null ? DEFAULT_GLOBAL_STATE_PARTITION_TYPE : partition.getPartitionType();
        LOG.debug("Try to save progress for partition {} (Type {})", partition.getPartitionKey(), partitionType);

        //
        if (partition.getSourcePartitionStoreItem() == null) {
            LOG.error("Unable to save progress, the item was not found.");
            return;
        }

        final SourcePartitionStoreItem updateItem = partition.getSourcePartitionStoreItem();
        // Also extend the timeout of the lease (ownership)
        if (updateItem.getPartitionOwnershipTimeout() != null) {
            updateItem.setPartitionOwnershipTimeout(Instant.now().plus(DEFAULT_LEASE_TIMEOUT));
        }
        updateItem.setPartitionProgressState(partition.convertPartitionProgressStatetoString(partition.getProgressState()));

        coordinationStore.tryUpdateSourcePartitionItem(updateItem);
        LOG.info("Progress for for partition {} (Type {}) was saved", partition.getPartitionKey(), partitionType);
    }

    @Override
    public <T> void giveUpPartition(SourcePartition<T> partition) {
        Objects.nonNull(partition.getPartitionType());

        LOG.debug("Try to give up the ownership for partition {} (Type {})", partition.getPartitionKey(), partition.getPartitionType());

        if (partition.getSourcePartitionStoreItem() == null) {
            LOG.error("Unable to give up ownership, the item was not found.");
            return;
        }

        final SourcePartitionStoreItem updateItem = partition.getSourcePartitionStoreItem();
        // Clear the ownership and reset status.
        updateItem.setSourcePartitionStatus(SourcePartitionStatus.UNASSIGNED);
        updateItem.setPartitionOwner(null);
        updateItem.setPartitionOwnershipTimeout(null);

        // Throws UpdateException if update failed.
        coordinationStore.tryUpdateSourcePartitionItem(updateItem);
        LOG.info("Partition key {} was given up by owner {}", partition.getPartitionKey(), hostName);

    }

    @Override
    public <T> void completePartition(SourcePartition<T> partition) {
        Objects.nonNull(partition.getPartitionType());

        LOG.debug("Try to complete partition {} (Type {})", partition.getPartitionKey(), partition.getPartitionType());

        if (partition.getSourcePartitionStoreItem() == null) {
            LOG.error("Unable to complete, the item is not found.");
            return;
        }

        SourcePartitionStoreItem updateItem = partition.getSourcePartitionStoreItem();
        updateItem.setPartitionOwner(null);
        updateItem.setReOpenAt(null);
        updateItem.setPartitionOwnershipTimeout(null);
        updateItem.setSourcePartitionStatus(SourcePartitionStatus.COMPLETED);

        updateItem.setPartitionProgressState(partition.convertPartitionProgressStatetoString(partition.getProgressState()));

        // Throws UpdateException if update failed.
        coordinationStore.tryUpdateSourcePartitionItem(updateItem);
    }

    @Override
    public <T> void closePartition(SourcePartition<T> partition, final Duration reopenAfter, final int maxClosedCount) {

        Objects.nonNull(partition.getPartitionType());

        LOG.debug("Try to close partition {} (Type {})", partition.getPartitionKey(), partition.getPartitionType());
        if (partition.getSourcePartitionStoreItem() == null) {
            LOG.error("Unable to close, the item is not found.");
            return;
        }

        SourcePartitionStoreItem updateItem = partition.getSourcePartitionStoreItem();

        // Reset ownership
        updateItem.setPartitionOwner(null);
        updateItem.setPartitionOwnershipTimeout(null);
        updateItem.setPartitionProgressState(partition.convertPartitionProgressStatetoString(partition.getProgressState()));

        updateItem.setClosedCount(updateItem.getClosedCount() + 1L);
        if (updateItem.getClosedCount() >= maxClosedCount) {
            updateItem.setSourcePartitionStatus(SourcePartitionStatus.COMPLETED);
            updateItem.setReOpenAt(null);
        } else {
            updateItem.setSourcePartitionStatus(SourcePartitionStatus.CLOSED);
            updateItem.setReOpenAt(Instant.now().plus(reopenAfter));
        }

        // Throws UpdateException if update failed.
        coordinationStore.tryUpdateSourcePartitionItem(updateItem);
    }


    @Override
    public Optional<SourcePartition> getPartition(String partitionKey) {
        // Default to Global State only.
        final Optional<SourcePartitionStoreItem> sourceItem = coordinationStore.getSourcePartitionItem(this.sourceIdentifier + "|" + DEFAULT_GLOBAL_STATE_PARTITION_TYPE, partitionKey);
        if (!sourceItem.isPresent()) {
            LOG.error("Global state {} is not found.", partitionKey);
            return Optional.empty();
        }
        return Optional.of(partitionFactory.apply(sourceItem.get()));
    }

}
