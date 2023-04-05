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

public class LeaseBasedSourceCoordinator<T> implements SourceCoordinator<T> {

    private static final Logger LOG = LoggerFactory.getLogger(LeaseBasedSourceCoordinator.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    // These three could be configurable by the sources
    static final Duration DEFAULT_LEASE_TIMEOUT = Duration.ofMinutes(8);

    static final Long DEFAULT_MAX_CLOSED_COUNT = 5L;
    private static final Duration DEFAULT_REOPEN_AT = Duration.ofMinutes(15);

    private static final String hostName;

    private final SourceCoordinationConfig sourceCoordinationConfig;
    private final SourceCoordinationStore sourceCoordinationStore;
    private final PartitionManager<T> partitionManager;

    private final Class<T> partitionProgressStateClass;
    private final String ownerId;

    static {
        try {
            // Is there a better identifier for the instance
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
    public void createPartitions(final List<PartitionIdentifier> partitionIdentifiers) {
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
    public Optional<SourcePartition<T>> getNextPartition() {
        if (partitionManager.getActivePartition().isPresent()) {
            // renew lease timeout here? Or is renewing on save state enough?
            return partitionManager.getActivePartition();
        }

        final Optional<SourcePartitionStoreItem> ownedPartitions = sourceCoordinationStore.tryAcquireAvailablePartition();

        if (ownedPartitions.isEmpty()) {
            // Potential metric (AcquirePartitionMisses). May indicate that the number of nodes could be lowered or new partitions need to be added
            LOG.info("Partition owner {} did not acquire any partitions", ownerId);
            return Optional.empty();
        } else {
            final SourcePartition<T> sourcePartition = SourcePartition.builder(partitionProgressStateClass)
                    .withPartitionKey(ownedPartitions.get().getSourcePartitionKey())
                    .withPartitionState(convertStringToPartitionProgressStateClass(ownedPartitions.get().getPartitionProgressState()))
                    .build();

            partitionManager.setActivePartition(sourcePartition);

            LOG.debug("Partition key {} was acquired by owner {}", sourcePartition.getPartitionKey(), ownerId);

            return Optional.of(sourcePartition);
        }
    }

    @Override
    public boolean completePartition(final String partitionKey) {

        if (!isActivelyOwnedPartition(partitionKey)) {
            throw new PartitionNotOwnedException(
                    String.format("Unable to complete the partition because partition key %s is not owned by this instance of Data Prepper for owner %s", partitionKey, ownerId)
            );
        }

        final Optional<SourcePartitionStoreItem> optionalPartitionItem = sourceCoordinationStore.getSourcePartitionItem(partitionKey);

        if (optionalPartitionItem.isEmpty()) {
            throw new PartitionNotFoundException(String.format("Unable to complete the partition because partition key %s was not found by owner %s. It may have been deleted.", partitionKey, ownerId));
        }

        final SourcePartitionStoreItem itemToUpdate = optionalPartitionItem.get();
        validatePartitionOwnership(itemToUpdate);

        itemToUpdate.setPartitionOwner(null);
        itemToUpdate.setReOpenAt(null);
        itemToUpdate.setPartitionOwnershipTimeout(null);
        itemToUpdate.setSourcePartitionStatus(SourcePartitionStatus.COMPLETED);

        final boolean completedSuccessfully = sourceCoordinationStore.tryUpdateSourcePartitionItem(itemToUpdate);

        if (!completedSuccessfully) {
            LOG.warn("Unable to complete the partition for owner {} with partition key {}", ownerId, partitionKey);
            return false;
        }

        LOG.debug("Partition key {} was completed by owner {}.", partitionKey, ownerId);

        partitionManager.removeActivePartition();
        return true;
    }

    @Override
    public boolean closePartition(final String partitionKey, final Duration reopenAfter) {
        if (!isActivelyOwnedPartition(partitionKey)) {
            throw new PartitionNotOwnedException(
                    String.format("Unable to close the partition because partition key %s is not owned by this instance of Data Prepper for owner %s", partitionKey, ownerId)
            );
        }

        final Optional<SourcePartitionStoreItem> optionalPartitionItem = sourceCoordinationStore.getSourcePartitionItem(partitionKey);

        if (optionalPartitionItem.isEmpty()) {
            throw new PartitionNotFoundException(String.format("Unable to close the partition because partition key %s was not found by owner %s", partitionKey, ownerId));
        }

        final SourcePartitionStoreItem itemToUpdate = optionalPartitionItem.get();
        validatePartitionOwnership(itemToUpdate);

        itemToUpdate.setPartitionOwner(null);
        itemToUpdate.setPartitionOwnershipTimeout(null);
        if (itemToUpdate.getClosedCount() >= DEFAULT_MAX_CLOSED_COUNT) {
            itemToUpdate.setSourcePartitionStatus(SourcePartitionStatus.COMPLETED);
        } else {
            itemToUpdate.setSourcePartitionStatus(SourcePartitionStatus.CLOSED);
            itemToUpdate.setReOpenAt(Instant.now().plus(DEFAULT_REOPEN_AT));
            itemToUpdate.setClosedCount(itemToUpdate.getClosedCount() + 1);
        }


        final boolean closedSuccessfully = sourceCoordinationStore.tryUpdateSourcePartitionItem(itemToUpdate);

        if (!closedSuccessfully) {
            LOG.warn("Unable to close the partition for partition owner {} with partition key {}", ownerId, partitionKey);
            return false;
        }

        LOG.debug("Partition key {} was closed by owner {}. The resulting status of the partition is now {}", partitionKey, ownerId, itemToUpdate.getSourcePartitionStatus());

        partitionManager.removeActivePartition();
        return true;
    }

    @Override
    public <S extends T> boolean saveProgressStateForPartition(final String partitionKey, final S partitionProgressState) {
        if (!isActivelyOwnedPartition(partitionKey)) {
            throw new PartitionNotOwnedException(
                    String.format("Unable to save state for the partition because partition key %s is not owned by this instance of Data Prepper for owner %s", partitionKey, ownerId)
            );
        }

        final Optional<SourcePartitionStoreItem> optionalPartitionItem = sourceCoordinationStore.getSourcePartitionItem(partitionKey);

        if (optionalPartitionItem.isEmpty()) {
            throw new PartitionNotFoundException(String.format("Unable to save state for the partition because partition key %s was not found by owner %s", partitionKey, ownerId));
        }

        final SourcePartitionStoreItem itemToUpdate = optionalPartitionItem.get();
        validatePartitionOwnership(itemToUpdate);

        itemToUpdate.setPartitionOwnershipTimeout(Instant.now().plus(DEFAULT_LEASE_TIMEOUT));
        itemToUpdate.setPartitionProgressState(convertPartitionProgressStateClasstoString(partitionProgressState));

        final boolean savedStateSuccessfully = sourceCoordinationStore.tryUpdateSourcePartitionItem(itemToUpdate);

        if (!savedStateSuccessfully) {
            LOG.warn("Unable to save state for the partition for owner {} with partition key {}", ownerId, partitionKey);
            return false;
        }

        LOG.debug("State was saved for partition key {} by owner {}. The saved state is: {}",
                partitionKey, ownerId, itemToUpdate.getPartitionProgressState());

        return true;
    }

    @Override
    public void giveUpPartitions() {
        final Optional<SourcePartition<T>> activePartition = partitionManager.getActivePartition();
        if (activePartition.isPresent()) {
            final Optional<SourcePartitionStoreItem> optionalItem = sourceCoordinationStore.getSourcePartitionItem(activePartition.get().getPartitionKey());
            if (optionalItem.isPresent()) {
                final SourcePartitionStoreItem updateItem = optionalItem.get();
                validatePartitionOwnership(updateItem);

                updateItem.setSourcePartitionStatus(SourcePartitionStatus.UNASSIGNED);
                updateItem.setPartitionOwner(null);
                updateItem.setPartitionOwnershipTimeout(null);

                final boolean releasedPartition = sourceCoordinationStore.tryUpdateSourcePartitionItem(updateItem);
                if (!releasedPartition) {
                    // Should these booleans and logs be exceptions that bubble up from SourceCoordinationStore and get logged here?
                    // Would show exact conditional check that failed in case of ddb, and other stores could have detailed errors logged.
                    // This would keep them from having to log themselves as they could just throw custom exception PartitionItemUpdateException?
                    LOG.warn("Unable to release partition for owner {} with partition key {}.",
                            ownerId, updateItem.getSourcePartitionKey());
                } else {
                    LOG.debug("Partition key {} was given up by owner {}", updateItem.getSourcePartitionKey(), ownerId);
                }
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
        // Should we also validate that partitionOwnershipTimeout is not in the past? Seems ok to give that a pass here,
        // because even though this instance didn't renew lease timeout in time, no other instance has grabbed the partition if owner still matches
        if (Objects.isNull(item.getPartitionOwner()) || !item.getPartitionOwner().equals(ownerId)) {
            partitionManager.removeActivePartition();
            throw new PartitionNotOwnedException(String.format("The partition is no longer owned by this instance of Data Prepper. " +
                    "The partition ownership timeout most likely expired and was grabbed by another instance of Data Prepper for partition owner %s and partition key %s.",
                    ownerId, item.getSourcePartitionKey()));
        }
    }
}
