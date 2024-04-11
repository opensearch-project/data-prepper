/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.sourcecoordination.enhanced;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.source.SourceCoordinationStore;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStatus;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.parser.model.SourceCoordinationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * An implemetation of {@link EnhancedSourceCoordinator} backend by {@link SourceCoordinationStore}
 */
public class EnhancedLeaseBasedSourceCoordinator implements EnhancedSourceCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(EnhancedLeaseBasedSourceCoordinator.class);

    /**
     * Default time out duration for lease.
     */
    private static final Duration DEFAULT_LEASE_TIMEOUT = Duration.ofMinutes(10);

    /**
     * Default identifier For global state
     */
    private static final String DEFAULT_GLOBAL_STATE_PARTITION_TYPE = "GLOBAL";

    /**
     * A backend coordination store
     */
    private final SourceCoordinationStore coordinationStore;

    /**
     * A unique identifier for a source, normally the pipeline name/id.
     * As the coordination store may be shared in different pipelines.
     */
    private final String sourceIdentifier;

    /**
     * In order to support different types of partitions.
     * A custom factory is required to map a {@link SourcePartitionStoreItem} to a {@link EnhancedSourcePartition}
     */
    private final Function<SourcePartitionStoreItem, EnhancedSourcePartition> partitionFactory;

    private final PluginMetrics pluginMetrics;

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


    public EnhancedLeaseBasedSourceCoordinator(final SourceCoordinationStore coordinationStore,
                                               final SourceCoordinationConfig sourceCoordinationConfig,
                                               final PluginMetrics pluginMetrics,
                                               final String sourceIdentifier,
                                               final Function<SourcePartitionStoreItem, EnhancedSourcePartition> partitionFactory) {
        this.coordinationStore = coordinationStore;
        this.sourceIdentifier = Objects.nonNull(sourceCoordinationConfig.getPartitionPrefix()) ?
                sourceCoordinationConfig.getPartitionPrefix() + "|" + sourceIdentifier :
                sourceIdentifier;
        this.pluginMetrics = pluginMetrics;
        this.partitionFactory = partitionFactory;
    }

    @Override
    public void initialize() {
        coordinationStore.initializeStore();
    }

    @Override
    public <T> boolean createPartition(EnhancedSourcePartition<T> partition) {
        String partitionType = partition.getPartitionType() == null ? DEFAULT_GLOBAL_STATE_PARTITION_TYPE : partition.getPartitionType();
        // Don't need the status for Global state which is not for lease.
        SourcePartitionStatus status = partition.getPartitionType() == null ? null : SourcePartitionStatus.UNASSIGNED;

        return coordinationStore.tryCreatePartitionItem(
                this.sourceIdentifier + "|" + partitionType,
                partition.getPartitionKey(),
                status,
                0L,
                partition.convertPartitionProgressStatetoString(partition.getProgressState()),
                // For now, global items with no partitionType will be considered ReadOnly, but this should be directly in EnhancedSourcePartition in the future
                partition.getPartitionType() == null);

    }


    @Override
    public Optional<EnhancedSourcePartition> acquireAvailablePartition(final String partitionType) {
        // Not available for global state.
        Objects.requireNonNull(partitionType);
        LOG.debug("Try to acquire an available {} partition", partitionType);
        Optional<SourcePartitionStoreItem> sourceItem = coordinationStore.tryAcquireAvailablePartition(this.sourceIdentifier + "|" + partitionType, hostName, DEFAULT_LEASE_TIMEOUT);
        if (sourceItem.isEmpty()) {
            LOG.debug("Partition owner {} failed to acquire a partition, no available {} partitions now", hostName, partitionType);
            return Optional.empty();
        }

        return Optional.of(partitionFactory.apply(sourceItem.get()));
    }

    @Override
    public List<EnhancedSourcePartition> queryCompletedPartitions(final String partitionType, final Instant fromCompletionTime) {
        Objects.requireNonNull(partitionType);
        LOG.debug("Try to query a list of completed {} partitions", partitionType);
        long startTime = System.currentTimeMillis();
        List<SourcePartitionStoreItem> sourcePartitionStoreItems = coordinationStore.querySourcePartitionItemsByStatus(
                this.sourceIdentifier + "|" + partitionType,
                SourcePartitionStatus.COMPLETED,
                fromCompletionTime.toString());

        List<EnhancedSourcePartition> sourcePartitions = sourcePartitionStoreItems.stream()
                .map(sourcePartitionStoreItem -> partitionFactory.apply(sourcePartitionStoreItem))
                .collect(Collectors.toList());
        long endTime = System.currentTimeMillis();
        LOG.info("Query of completed partitions took {} milliseconds with {} items found", endTime - startTime, sourcePartitions.size());
        return sourcePartitions;
    }

    @Override
    public List<EnhancedSourcePartition> queryAllPartitions(final String partitionType) {
        Objects.requireNonNull(partitionType);
        LOG.debug("Try to query all {} partitions", partitionType);
        long startTime = System.currentTimeMillis();
        final List<SourcePartitionStoreItem> sourcePartitionStoreItems = coordinationStore.queryAllSourcePartitionItems(
                this.sourceIdentifier + "|" + partitionType);

        final List<EnhancedSourcePartition> sourcePartitions = sourcePartitionStoreItems.stream()
                .map(partitionFactory)
                .collect(Collectors.toList());

        long endTime = System.currentTimeMillis();
        LOG.info("Query of partitions took {} milliseconds with {} items found", endTime - startTime, sourcePartitions.size());
        return sourcePartitions;
    }

    @Override
    public <T> void saveProgressStateForPartition(EnhancedSourcePartition<T> partition, final Duration ownershipTimeoutRenewal) {
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
            updateItem.setPartitionOwnershipTimeout(Instant.now().plus(ownershipTimeoutRenewal == null ? DEFAULT_LEASE_TIMEOUT : ownershipTimeoutRenewal));
        }
        updateItem.setPartitionProgressState(partition.convertPartitionProgressStatetoString(partition.getProgressState()));

        coordinationStore.tryUpdateSourcePartitionItem(updateItem);
        LOG.debug("Progress for for partition {} (Type {}) was saved", partition.getPartitionKey(), partitionType);
    }

    @Override
    public <T> void giveUpPartition(EnhancedSourcePartition<T> partition) {
        Objects.requireNonNull(partition.getPartitionType());

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
        LOG.debug("Partition key {} was given up by owner {}", partition.getPartitionKey(), hostName);

    }

    @Override
    public <T> void completePartition(EnhancedSourcePartition<T> partition) {
        Objects.requireNonNull(partition.getPartitionType());

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
    public <T> void closePartition(EnhancedSourcePartition<T> partition, final Duration reopenAfter, final int maxClosedCount) {

        Objects.requireNonNull(partition.getPartitionType());

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
    public Optional<EnhancedSourcePartition> getPartition(final String partitionKey) {
        // Default to Global State only.
        final Optional<SourcePartitionStoreItem> sourceItem = coordinationStore.getSourcePartitionItem(this.sourceIdentifier + "|" + DEFAULT_GLOBAL_STATE_PARTITION_TYPE, partitionKey);
        if (!sourceItem.isPresent()) {
            LOG.warn("Global partition item with sourcePartitionKey '{}' could not be found.", partitionKey);
            return Optional.empty();
        }
        return Optional.of(partitionFactory.apply(sourceItem.get()));
    }

}
