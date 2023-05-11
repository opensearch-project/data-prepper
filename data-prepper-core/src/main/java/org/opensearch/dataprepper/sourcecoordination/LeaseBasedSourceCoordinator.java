/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.sourcecoordination;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.model.source.SourceCoordinationStore;
import org.opensearch.dataprepper.model.source.coordinator.PartitionIdentifier;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartition;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStatus;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionNotFoundException;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionNotOwnedException;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.UninitializedSourceCoordinatorException;
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
import java.util.function.Supplier;

public class LeaseBasedSourceCoordinator<T> implements SourceCoordinator<T> {

    private static final Logger LOG = LoggerFactory.getLogger(LeaseBasedSourceCoordinator.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    static final Duration DEFAULT_LEASE_TIMEOUT = Duration.ofMinutes(10);

    private static final String hostName;

    private final SourceCoordinationConfig sourceCoordinationConfig;
    private final SourceCoordinationStore sourceCoordinationStore;
    private final PartitionManager<T> partitionManager;

    private final Class<T> partitionProgressStateClass;
    private final String ownerId;
    private boolean initialized = false;

    static {
        try {
            hostName = InetAddress.getLocalHost().getHostName();
        } catch (final UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    public LeaseBasedSourceCoordinator(final Class<T> partitionProgressStateClass,
                                       final SourceCoordinationStore sourceCoordinationStore,
                                       final SourceCoordinationConfig sourceCoordinationConfig,
                                       final PartitionManager<T> partitionManager,
                                       final String ownerPrefix) {
        this.sourceCoordinationConfig = sourceCoordinationConfig;
        this.sourceCoordinationStore = sourceCoordinationStore;
        this.partitionProgressStateClass = partitionProgressStateClass;
        this.partitionManager = partitionManager;
        this.ownerId = ownerPrefix + ":" + hostName;
    }

    @Override
    public void initialize() {
        sourceCoordinationStore.initializeStore();
        initialized = true;
    }

    @Override
    public Optional<SourcePartition<T>> getNextPartition(final Supplier<List<PartitionIdentifier>> partitionsCreatorSupplier) {
        validateIsInitialized();

        if (partitionManager.getActivePartition().isPresent()) {
            return partitionManager.getActivePartition();
        }

        Optional<SourcePartitionStoreItem> ownedPartitions = sourceCoordinationStore.tryAcquireAvailablePartition(ownerId, DEFAULT_LEASE_TIMEOUT);

        if (ownedPartitions.isEmpty()) {
            LOG.info("Partition owner {} did not acquire any partitions. Running partition creation supplier to create more partitions", ownerId);

            createPartitions(partitionsCreatorSupplier.get());

            ownedPartitions = sourceCoordinationStore.tryAcquireAvailablePartition(ownerId, DEFAULT_LEASE_TIMEOUT);
        }

        if (ownedPartitions.isEmpty()) {
            LOG.info("Partition owner {} did not acquire any partitions even after running the partition creation supplier", ownerId);
            return Optional.empty();
        }

        final SourcePartition<T> sourcePartition = SourcePartition.builder(partitionProgressStateClass)
                .withPartitionKey(ownedPartitions.get().getSourcePartitionKey())
                .withPartitionState(convertStringToPartitionProgressStateClass(ownedPartitions.get().getPartitionProgressState()))
                .build();

        partitionManager.setActivePartition(sourcePartition);

        LOG.debug("Partition key {} was acquired by owner {}", sourcePartition.getPartitionKey(), ownerId);

        return Optional.of(sourcePartition);
    }

    private void createPartitions(final List<PartitionIdentifier> partitionIdentifiers) {
        for (final PartitionIdentifier partitionIdentifier : partitionIdentifiers) {
            final Optional<SourcePartitionStoreItem> optionalPartitionItem = sourceCoordinationStore.getSourcePartitionItem(partitionIdentifier.getPartitionKey());

            if (optionalPartitionItem.isPresent()) {
                return;
            }

            final boolean partitionCreated = sourceCoordinationStore.tryCreatePartitionItem(
                    partitionIdentifier.getPartitionKey(),
                    SourcePartitionStatus.UNASSIGNED,
                    0L,
                    null
            );

            if (partitionCreated) {
                LOG.info("Partition successfully created by owner {} for source partition key {}", ownerId, partitionIdentifier.getPartitionKey());
            }
        }
    }

    @Override
    public void completePartition(final String partitionKey) {
        validateIsInitialized();

        final SourcePartitionStoreItem itemToUpdate = validateAndGetSourcePartitionStoreItem(partitionKey, "complete");
        validatePartitionOwnership(itemToUpdate);

        itemToUpdate.setPartitionOwner(null);
        itemToUpdate.setReOpenAt(null);
        itemToUpdate.setPartitionOwnershipTimeout(null);
        itemToUpdate.setSourcePartitionStatus(SourcePartitionStatus.COMPLETED);

        sourceCoordinationStore.tryUpdateSourcePartitionItem(itemToUpdate);
        partitionManager.removeActivePartition();

        LOG.info("Partition key {} was completed by owner {}.", partitionKey, ownerId);
    }

    @Override
    public void closePartition(final String partitionKey, final Duration reopenAfter, final int maxClosedCount) {
        validateIsInitialized();

        final SourcePartitionStoreItem itemToUpdate = validateAndGetSourcePartitionStoreItem(partitionKey, "close");
        validatePartitionOwnership(itemToUpdate);

        itemToUpdate.setPartitionOwner(null);
        itemToUpdate.setPartitionOwnershipTimeout(null);
        itemToUpdate.setPartitionProgressState(null);
        itemToUpdate.setClosedCount(itemToUpdate.getClosedCount() + 1L);
        if (itemToUpdate.getClosedCount() >= maxClosedCount) {
            itemToUpdate.setSourcePartitionStatus(SourcePartitionStatus.COMPLETED);
            itemToUpdate.setReOpenAt(null);
        } else {
            itemToUpdate.setSourcePartitionStatus(SourcePartitionStatus.CLOSED);
            itemToUpdate.setReOpenAt(Instant.now().plus(reopenAfter));
        }


        sourceCoordinationStore.tryUpdateSourcePartitionItem(itemToUpdate);
        partitionManager.removeActivePartition();

        LOG.info("Partition key {} was closed by owner {}. The resulting status of the partition is now {}", partitionKey, ownerId, itemToUpdate.getSourcePartitionStatus());
    }

    @Override
    public <S extends T> void saveProgressStateForPartition(final String partitionKey, final S partitionProgressState) {
        validateIsInitialized();

        final SourcePartitionStoreItem itemToUpdate = validateAndGetSourcePartitionStoreItem(partitionKey, "save state");
        validatePartitionOwnership(itemToUpdate);

        itemToUpdate.setPartitionOwnershipTimeout(Instant.now().plus(DEFAULT_LEASE_TIMEOUT));
        itemToUpdate.setPartitionProgressState(convertPartitionProgressStateClasstoString(partitionProgressState));

        sourceCoordinationStore.tryUpdateSourcePartitionItem(itemToUpdate);

        LOG.debug("State was saved for partition key {} by owner {}. The saved state is: {}",
                partitionKey, ownerId, itemToUpdate.getPartitionProgressState());
    }

    @Override
    public void giveUpPartitions() {
        validateIsInitialized();

        final Optional<SourcePartition<T>> activePartition = partitionManager.getActivePartition();
        if (activePartition.isPresent()) {
            final Optional<SourcePartitionStoreItem> optionalItem = sourceCoordinationStore.getSourcePartitionItem(activePartition.get().getPartitionKey());
            if (optionalItem.isPresent()) {
                final SourcePartitionStoreItem updateItem = optionalItem.get();
                validatePartitionOwnership(updateItem);

                updateItem.setSourcePartitionStatus(SourcePartitionStatus.UNASSIGNED);
                updateItem.setPartitionOwner(null);
                updateItem.setPartitionOwnershipTimeout(null);

                sourceCoordinationStore.tryUpdateSourcePartitionItem(updateItem);

                LOG.debug("Partition key {} was given up by owner {}", updateItem.getSourcePartitionKey(), ownerId);
            }
            partitionManager.removeActivePartition();
        }
    }

    private T convertStringToPartitionProgressStateClass(final String serializedPartitionProgressState) {
        return objectMapper.convertValue(serializedPartitionProgressState, partitionProgressStateClass);
    }

    private String convertPartitionProgressStateClasstoString(final T partitionProgressState) {
        return objectMapper.convertValue(partitionProgressState, String.class);
    }

    private boolean isActivelyOwnedPartition(final String partitionKey) {
        final Optional<SourcePartition<T>> activePartition = partitionManager.getActivePartition();
        return activePartition.isPresent() && activePartition.get().getPartitionKey().equals(partitionKey);
    }

    private void validatePartitionOwnership(final SourcePartitionStoreItem item) {
        if (Objects.isNull(item.getPartitionOwner()) || !item.getPartitionOwner().equals(ownerId)) {
            partitionManager.removeActivePartition();
            throw new PartitionNotOwnedException(String.format("The partition is no longer owned by this instance of Data Prepper. " +
                    "The partition ownership timeout most likely expired and was grabbed by another instance of Data Prepper for partition owner %s and partition key %s.",
                    ownerId, item.getSourcePartitionKey()));
        }
    }

    private SourcePartitionStoreItem validateAndGetSourcePartitionStoreItem(final String partitionKey, final String action) {
        if (!isActivelyOwnedPartition(partitionKey)) {
            throw new PartitionNotOwnedException(
                    String.format("Unable to %s for the partition because partition key %s is not owned by this instance of Data Prepper for owner %s", action, partitionKey, ownerId)
            );
        }

        final Optional<SourcePartitionStoreItem> optionalPartitionItem = sourceCoordinationStore.getSourcePartitionItem(partitionKey);

        if (optionalPartitionItem.isEmpty()) {
            throw new PartitionNotFoundException(String.format("Unable to %s for the partition because partition key %s was not found by owner %s", action, partitionKey, ownerId));
        }

        return optionalPartitionItem.get();
    }

    private void validateIsInitialized() {
        if (!initialized) {
            throw new UninitializedSourceCoordinatorException("The initialize method has not been called on this source coordinator. initialize must be called before further interactions with the SourceCoordinator");
        }
    }
}
