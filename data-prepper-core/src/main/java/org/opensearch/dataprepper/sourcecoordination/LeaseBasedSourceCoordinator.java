/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.sourcecoordination;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.source.SourceCoordinationStore;
import org.opensearch.dataprepper.model.source.coordinator.PartitionIdentifier;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartition;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStatus;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionNotFoundException;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionNotOwnedException;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionUpdateException;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.UninitializedSourceCoordinatorException;
import org.opensearch.dataprepper.parser.model.SourceCoordinationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

public class LeaseBasedSourceCoordinator<T> implements SourceCoordinator<T> {

    private static final Logger LOG = LoggerFactory.getLogger(LeaseBasedSourceCoordinator.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final String COMPLETE_ACTION = "complete";
    private static final String CLOSE_ACTION = "close";
    private static final String SAVE_STATE_ACTION = "saveState";

    static final String PARTITION_CREATION_SUPPLIER_INVOCATION_COUNT = "partitionCreationSupplierInvocations";
    static final String NO_PARTITIONS_ACQUIRED_COUNT = "noPartitionsAcquired";
    static final String PARTITION_CREATED_COUNT = "partitionsCreatedCount";
    static final String PARTITIONS_ACQUIRED_COUNT = "partitionsAcquired";
    static final String PARTITIONS_COMPLETED_COUNT = "partitionsCompleted";
    static final String PARTITIONS_CLOSED_COUNT = "partitionsClosed";

    static final String PARTITIONS_DELETED = "partitionsDeleted";
    static final String SAVE_PROGRESS_STATE_INVOCATION_SUCCESS_COUNT = "savePartitionProgressStateInvocationsSuccess";
    static final String PARTITION_OWNERSHIP_GIVEN_UP_COUNT = "partitionsGivenUp";

    static final String PARTITION_NOT_FOUND_ERROR_COUNT = "partitionNotFoundErrors";
    static final String PARTITION_NOT_OWNED_ERROR_COUNT = "partitionNotOwnedErrors";
    static final String PARTITION_UPDATE_ERROR_COUNT = "PartitionUpdateErrors";

    static final Duration DEFAULT_LEASE_TIMEOUT = Duration.ofMinutes(10);

    private static final String hostName;
    static final String PARTITION_TYPE = "PARTITION";
    static final String GLOBAL_STATE_TYPE = "GLOBAL_STATE";
    static final String GLOBAL_STATE_SOURCE_PARTITION_KEY_FOR_CREATING_PARTITIONS = "GLOBAL_STATE_SOURCE_PARTITION_KEY_FOR_CREATING_PARTITIONS";

    private final SourceCoordinationConfig sourceCoordinationConfig;
    private final SourceCoordinationStore sourceCoordinationStore;

    private final Class<T> partitionProgressStateClass;
    private final String ownerId;
    private final String sourceIdentifier;
    private final String sourceIdentifierWithPartitionType;
    private final String sourceIdentifierWithGlobalStateType;
    private boolean initialized = false;

    private final PluginMetrics pluginMetrics;
    private final Counter partitionCreationSupplierInvocationsCounter;
    private final Counter partitionsCreatedCounter;
    private final Counter noPartitionsAcquiredCounter;
    private final Counter partitionsAcquiredCounter;
    private final Counter partitionsCompletedCounter;
    private final Counter partitionsClosedCounter;
    private final Counter saveProgressStateInvocationSuccessCounter;
    private final Counter partitionsGivenUpCounter;
    private final Counter partitionNotFoundErrorCounter;
    private final Counter partitionNotOwnedErrorCounter;
    private final Counter saveStatePartitionUpdateErrorCounter;
    private final Counter closePartitionUpdateErrorCounter;
    private final Counter completePartitionUpdateErrorCounter;

    private final Counter partitionsDeleted;
    private final ReentrantLock lock;

    private Instant lastSupplierRunTime;

    static final Duration FORCE_SUPPLIER_AFTER_DURATION = Duration.ofMinutes(5);

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
                                       final String sourceIdentifier,
                                       final PluginMetrics pluginMetrics) {
        this.sourceCoordinationConfig = sourceCoordinationConfig;
        this.sourceCoordinationStore = sourceCoordinationStore;
        this.partitionProgressStateClass = partitionProgressStateClass;
        this.sourceIdentifier = Objects.nonNull(sourceCoordinationConfig.getPartitionPrefix()) ?
                sourceCoordinationConfig.getPartitionPrefix() + "|" + sourceIdentifier :
                sourceIdentifier;
        this.sourceIdentifierWithPartitionType = this.sourceIdentifier + "|" + PARTITION_TYPE;
        this.sourceIdentifierWithGlobalStateType = this.sourceIdentifier + "|" + GLOBAL_STATE_TYPE;
        this.ownerId = this.sourceIdentifier + ":" + hostName;
        this.pluginMetrics = pluginMetrics;
        this.partitionCreationSupplierInvocationsCounter = pluginMetrics.counter(PARTITION_CREATION_SUPPLIER_INVOCATION_COUNT);
        this.partitionsCreatedCounter = pluginMetrics.counter(PARTITION_CREATED_COUNT);
        this.noPartitionsAcquiredCounter = pluginMetrics.counter(NO_PARTITIONS_ACQUIRED_COUNT);
        this.partitionsAcquiredCounter = pluginMetrics.counter(PARTITIONS_ACQUIRED_COUNT);
        this.partitionsCompletedCounter = pluginMetrics.counter(PARTITIONS_COMPLETED_COUNT);
        this.partitionsClosedCounter = pluginMetrics.counter(PARTITIONS_CLOSED_COUNT);
        this.saveProgressStateInvocationSuccessCounter = pluginMetrics.counter(SAVE_PROGRESS_STATE_INVOCATION_SUCCESS_COUNT);
        this.partitionsGivenUpCounter = pluginMetrics.counter(PARTITION_OWNERSHIP_GIVEN_UP_COUNT);
        this.partitionNotFoundErrorCounter = pluginMetrics.counter(PARTITION_NOT_FOUND_ERROR_COUNT);
        this.partitionNotOwnedErrorCounter = pluginMetrics.counter(PARTITION_NOT_OWNED_ERROR_COUNT);
        this.saveStatePartitionUpdateErrorCounter = pluginMetrics.counter(PARTITION_UPDATE_ERROR_COUNT, SAVE_STATE_ACTION);
        this.closePartitionUpdateErrorCounter = pluginMetrics.counter(PARTITION_UPDATE_ERROR_COUNT, CLOSE_ACTION);
        this.completePartitionUpdateErrorCounter = pluginMetrics.counter(PARTITION_UPDATE_ERROR_COUNT, COMPLETE_ACTION);
        this.partitionsDeleted = pluginMetrics.counter(PARTITIONS_DELETED);
        this.lock = new ReentrantLock();
        this.lastSupplierRunTime = Instant.now();
    }

    @Override
    public void initialize() {
        sourceCoordinationStore.initializeStore();
        initialized = true;
        sourceCoordinationStore.tryCreatePartitionItem(sourceIdentifierWithGlobalStateType, GLOBAL_STATE_SOURCE_PARTITION_KEY_FOR_CREATING_PARTITIONS, SourcePartitionStatus.UNASSIGNED, 0L, null, false);
    }

    @Override
    public Optional<SourcePartition<T>> getNextPartition(final Function<Map<String, Object>, List<PartitionIdentifier>> partitionCreationSupplier) {
        return getNextPartitionInternal(partitionCreationSupplier, false);
    }

    @Override
    public Optional<SourcePartition<T>> getNextPartition(final Function<Map<String, Object>, List<PartitionIdentifier>> partitionCreationSupplier, final boolean forceSupplierEnabled) {
        if (forceSupplierEnabled && Instant.now().isAfter(lastSupplierRunTime.plus(FORCE_SUPPLIER_AFTER_DURATION))) {
            return getNextPartitionInternal(partitionCreationSupplier, true);
        }

        return getNextPartitionInternal(partitionCreationSupplier, false);
    }

    private Optional<SourcePartition<T>> getNextPartitionInternal(final Function<Map<String, Object>, List<PartitionIdentifier>> partitionCreationSupplier, final boolean forceSupplier) {
        validateIsInitialized();

        Optional<SourcePartitionStoreItem> ownedPartitions = sourceCoordinationStore.tryAcquireAvailablePartition(sourceIdentifierWithPartitionType, ownerId, DEFAULT_LEASE_TIMEOUT);
        try {
            if ((ownedPartitions.isEmpty() || forceSupplier) && lock.tryLock()) {
                lastSupplierRunTime = Instant.now();
                final Optional<SourcePartitionStoreItem> acquiredGlobalStateForPartitionCreation = acquireGlobalStateForPartitionCreation();
                if (acquiredGlobalStateForPartitionCreation.isPresent()) {
                    final Map<String, Object> globalStateMap = convertStringToGlobalStateMap(acquiredGlobalStateForPartitionCreation.get().getPartitionProgressState());
                    LOG.info("Partition owner {} did not acquire any partitions. Running partition creation supplier to create more partitions", ownerId);
                    createPartitions(partitionCreationSupplier.apply(globalStateMap));
                    partitionCreationSupplierInvocationsCounter.increment();
                    giveUpAndSaveGlobalStateForPartitionCreation(acquiredGlobalStateForPartitionCreation.get(), globalStateMap);
                }
            }
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
            if (ownedPartitions.isEmpty()) {
                ownedPartitions = sourceCoordinationStore.tryAcquireAvailablePartition(sourceIdentifierWithPartitionType, ownerId, DEFAULT_LEASE_TIMEOUT);
            }
        }

        if (ownedPartitions.isEmpty()) {
            LOG.info("Partition owner {} did not acquire any partitions even after running the partition creation supplier", ownerId);
            noPartitionsAcquiredCounter.increment();
            return Optional.empty();
        }

        final SourcePartition<T> sourcePartition = SourcePartition.builder(partitionProgressStateClass)
                .withPartitionKey(ownedPartitions.get().getSourcePartitionKey())
                .withPartitionState(convertStringToPartitionProgressStateClass(ownedPartitions.get().getPartitionProgressState()))
                .withPartitionClosedCount(ownedPartitions.get().getClosedCount())
                .build();


        LOG.debug("Partition key {} was acquired by owner {}", sourcePartition.getPartitionKey(), ownerId);
        partitionsAcquiredCounter.increment();
        return Optional.of(sourcePartition);
    }

    private void createPartitions(final List<PartitionIdentifier> partitionIdentifiers) {
        for (final PartitionIdentifier partitionIdentifier : partitionIdentifiers) {
            final Optional<SourcePartitionStoreItem> optionalPartitionItem = sourceCoordinationStore.getSourcePartitionItem(sourceIdentifierWithPartitionType, partitionIdentifier.getPartitionKey());

            if (optionalPartitionItem.isPresent()) {
                continue;
            }

            final boolean partitionCreated = sourceCoordinationStore.tryCreatePartitionItem(
                    sourceIdentifierWithPartitionType,
                    partitionIdentifier.getPartitionKey(),
                    SourcePartitionStatus.UNASSIGNED,
                    0L,
                    null,
                    false
            );

            if (partitionCreated) {
                LOG.info("Partition successfully created by owner {} for source partition key {}", ownerId, partitionIdentifier.getPartitionKey());
                partitionsCreatedCounter.increment();
            }
        }
    }

    @Override
    public void completePartition(final String partitionKey, final Boolean fromAcknowledgmentsCallback) {
        validateIsInitialized();

        final SourcePartitionStoreItem itemToUpdate = getSourcePartitionStoreItem(partitionKey, COMPLETE_ACTION);
        validatePartitionOwnership(itemToUpdate);

        itemToUpdate.setPartitionOwner(null);
        itemToUpdate.setReOpenAt(null);
        itemToUpdate.setPartitionOwnershipTimeout(null);
        itemToUpdate.setSourcePartitionStatus(SourcePartitionStatus.COMPLETED);

        try {
            sourceCoordinationStore.tryUpdateSourcePartitionItem(itemToUpdate);
        } catch (final PartitionUpdateException e) {
            completePartitionUpdateErrorCounter.increment();
            throw e;
        }

        LOG.info("Partition key {} was completed by owner {}.", partitionKey, ownerId);
        partitionsCompletedCounter.increment();
    }

    @Override
    public void closePartition(final String partitionKey, final Duration reopenAfter, final int maxClosedCount, final Boolean fromAcknowledgmentsCallback) {
        validateIsInitialized();

        final SourcePartitionStoreItem itemToUpdate = getSourcePartitionStoreItem(partitionKey, CLOSE_ACTION);
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

        try {
            sourceCoordinationStore.tryUpdateSourcePartitionItem(itemToUpdate);
        } catch (final PartitionUpdateException e) {
            if (SourcePartitionStatus.COMPLETED.equals(itemToUpdate.getSourcePartitionStatus())) {
                completePartitionUpdateErrorCounter.increment();
            } else {
                closePartitionUpdateErrorCounter.increment();
            }
            throw e;
        }

        if (SourcePartitionStatus.COMPLETED.equals(itemToUpdate.getSourcePartitionStatus())) {
            partitionsCompletedCounter.increment();
        } else {
            partitionsClosedCounter.increment();
        }

        LOG.info("Partition key {} was closed by owner {}. The resulting status of the partition is now {}", partitionKey, ownerId, itemToUpdate.getSourcePartitionStatus());
    }

    @Override
    public <S extends T> void saveProgressStateForPartition(final String partitionKey, final S partitionProgressState) {
        validateIsInitialized();

        final SourcePartitionStoreItem itemToUpdate = getSourcePartitionStoreItem(partitionKey, SAVE_STATE_ACTION);
        validatePartitionOwnership(itemToUpdate);

        itemToUpdate.setPartitionOwnershipTimeout(Instant.now().plus(DEFAULT_LEASE_TIMEOUT));
        itemToUpdate.setPartitionProgressState(convertPartitionProgressStateClasstoString(partitionProgressState));

        try {
            sourceCoordinationStore.tryUpdateSourcePartitionItem(itemToUpdate);
        } catch (final PartitionUpdateException e) {
            saveStatePartitionUpdateErrorCounter.increment();
            throw e;
        }

        LOG.debug("State was saved for partition key {} by owner {}. The saved state is: {}",
                partitionKey, ownerId, itemToUpdate.getPartitionProgressState());

        saveProgressStateInvocationSuccessCounter.increment();
    }

    @Override
    public void updatePartitionForAcknowledgmentWait(final String partitionKey, final Duration ackowledgmentTimeout) {
        validateIsInitialized();

        final SourcePartitionStoreItem itemToUpdate = getSourcePartitionStoreItem(partitionKey, "update for ack wait");
        validatePartitionOwnership(itemToUpdate);

        itemToUpdate.setPartitionOwnershipTimeout(Instant.now().plus(ackowledgmentTimeout));

        sourceCoordinationStore.tryUpdateSourcePartitionItem(itemToUpdate);
    }

    @Override
    public void giveUpPartition(final String partitionKey) {
        giveUpPartitionInternal(partitionKey, null);
    }

    @Override
    public void giveUpPartition(final String partitionKey, final Instant priorityTimestamp) {
        giveUpPartitionInternal(partitionKey, priorityTimestamp);
    }

    private void giveUpPartitionInternal(final String partitionKey, final Instant priorityTimestamp) {
        if (!initialized) {
            return;
        }

        final Optional<SourcePartitionStoreItem> optionalItem = sourceCoordinationStore.getSourcePartitionItem(sourceIdentifierWithPartitionType, partitionKey);
        if (optionalItem.isPresent()) {
            final SourcePartitionStoreItem updateItem = optionalItem.get();
            validatePartitionOwnership(updateItem);

            updateItem.setSourcePartitionStatus(SourcePartitionStatus.UNASSIGNED);
            updateItem.setPartitionOwner(null);
            updateItem.setPartitionOwnershipTimeout(null);

            try {
                sourceCoordinationStore.tryUpdateSourcePartitionItem(updateItem, priorityTimestamp);
            } catch (final PartitionUpdateException e) {
                LOG.info("Unable to explicitly give up partition {}. Partition can be considered given up.", updateItem.getSourcePartitionKey());
            }


            LOG.info("Partition key {} was given up by owner {}", updateItem.getSourcePartitionKey(), ownerId);
        }
        partitionsGivenUpCounter.increment();
    }

    @Override
    public void deletePartition(final String partitionKey) {
        final Optional<SourcePartitionStoreItem> optionalItem = sourceCoordinationStore.getSourcePartitionItem(sourceIdentifierWithPartitionType, partitionKey);
        if (optionalItem.isPresent()) {
            final SourcePartitionStoreItem deleteItem = optionalItem.get();
            validatePartitionOwnership(deleteItem);

            try {
                sourceCoordinationStore.tryDeletePartitionItem(deleteItem);
            } catch (final PartitionUpdateException e) {
                LOG.info("Unable to delete partition {}: {}.", deleteItem.getSourcePartitionKey(), e.getMessage());
                return;
            }

            partitionsDeleted.increment();
            LOG.info("Partition key {} was deleted by owner {}", deleteItem.getSourcePartitionKey(), ownerId);
        }
    }

    private T convertStringToPartitionProgressStateClass(final String serializedPartitionProgressState) {
        if (Objects.isNull(serializedPartitionProgressState)) {
            return null;
        }

        try {
            return objectMapper.readValue(serializedPartitionProgressState, partitionProgressStateClass);
        } catch (final JsonProcessingException e) {
            LOG.error("Unable to convert string to partition progress state class {}", partitionProgressStateClass.getName(), e);
            return null;
        }
    }

    private String convertPartitionProgressStateClasstoString(final T partitionProgressState) {
        try {
            return objectMapper.writeValueAsString(partitionProgressState);
        } catch (final JsonProcessingException e) {
            LOG.error("Unable to convert partition progress state class {} to string", partitionProgressStateClass.getName(), e);
            return null;
        }
    }

    private Map<String, Object> convertStringToGlobalStateMap(final String serializedGlobalState) {
        if (Objects.isNull(serializedGlobalState)) {
            return new HashMap<>();
        }

        try {
            return objectMapper.readValue(serializedGlobalState, new TypeReference<>() {});
        } catch (final JsonProcessingException e) {
            LOG.error("Unable to convert global state string to Map<String, Object>", e);
            return new HashMap<>();
        }
    }

    private void validatePartitionOwnership(final SourcePartitionStoreItem item) {
        if (Objects.isNull(item.getPartitionOwner()) || !ownerId.equals(item.getPartitionOwner())) {
            partitionNotOwnedErrorCounter.increment();
            throw new PartitionNotOwnedException(String.format("The partition is no longer owned by this instance of Data Prepper. " +
                    "The partition ownership timeout most likely expired and was grabbed by another instance of Data Prepper for partition owner %s and partition key %s.",
                    ownerId, item.getSourcePartitionKey()));
        }
    }

    private SourcePartitionStoreItem getSourcePartitionStoreItem(final String partitionKey, final String action) {
        final Optional<SourcePartitionStoreItem> optionalPartitionItem = sourceCoordinationStore.getSourcePartitionItem(sourceIdentifierWithPartitionType, partitionKey);

        if (optionalPartitionItem.isEmpty()) {
            partitionNotFoundErrorCounter.increment();
            throw new PartitionNotFoundException(String.format("Unable to %s for the partition because partition key %s was not found by owner %s", action, partitionKey, ownerId));
        }

        return optionalPartitionItem.get();
    }

    private void validateIsInitialized() {
        if (!initialized) {
            throw new UninitializedSourceCoordinatorException("The initialize method has not been called on this source coordinator. initialize() must be called before further interactions with the SourceCoordinator");
        }
    }

    private Optional<SourcePartitionStoreItem> acquireGlobalStateForPartitionCreation() {
        final Optional<SourcePartitionStoreItem> globalStateItemForPartitionCreation = sourceCoordinationStore.getSourcePartitionItem(
                sourceIdentifierWithGlobalStateType, GLOBAL_STATE_SOURCE_PARTITION_KEY_FOR_CREATING_PARTITIONS);

        if (globalStateItemForPartitionCreation.isPresent()) {

            if (SourcePartitionStatus.ASSIGNED.equals(globalStateItemForPartitionCreation.get().getSourcePartitionStatus()) &&
                    (Objects.isNull(globalStateItemForPartitionCreation.get().getPartitionOwnershipTimeout()) ||
                            Instant.now().isAfter(globalStateItemForPartitionCreation.get().getPartitionOwnershipTimeout()) ||
                            ownerId.equals(globalStateItemForPartitionCreation.get().getPartitionOwner()))
            ) {
                LOG.warn("The global state item for partition creation is owned by {}, but the ownership has expired. Attempting to acquire global state for owner {}",
                        globalStateItemForPartitionCreation.get().getPartitionOwner(), ownerId);
            } else if (Objects.nonNull(globalStateItemForPartitionCreation.get().getPartitionOwner()) && !ownerId.equals(globalStateItemForPartitionCreation.get().getPartitionOwner())
                   || !SourcePartitionStatus.UNASSIGNED.equals(globalStateItemForPartitionCreation.get().getSourcePartitionStatus())) {
                return Optional.empty();
            }

            try {
                globalStateItemForPartitionCreation.get().setSourcePartitionStatus(SourcePartitionStatus.ASSIGNED);
                globalStateItemForPartitionCreation.get().setPartitionOwner(ownerId);
                globalStateItemForPartitionCreation.get().setPartitionOwnershipTimeout(Instant.now().plus(DEFAULT_LEASE_TIMEOUT));
                sourceCoordinationStore.tryUpdateSourcePartitionItem(globalStateItemForPartitionCreation.get());
            } catch (final PartitionUpdateException e) {
                LOG.debug("Owner %s was not able to acquire the global state item to create new partitions. This means that another instance of Data Prepper has gained the responsibility of creating partitions at this time");
                return Optional.empty();
            }
        }

        return globalStateItemForPartitionCreation;
    }

    private void giveUpAndSaveGlobalStateForPartitionCreation(final SourcePartitionStoreItem globalStateItemForPartitionCreation, final Map<String, Object> globalStateMap) {

        globalStateItemForPartitionCreation.setSourcePartitionStatus(SourcePartitionStatus.UNASSIGNED);
        globalStateItemForPartitionCreation.setPartitionOwner(null);
        globalStateItemForPartitionCreation.setPartitionOwnershipTimeout(null);

        try {
            globalStateItemForPartitionCreation.setPartitionProgressState(objectMapper.writeValueAsString(globalStateMap));
            sourceCoordinationStore.tryUpdateSourcePartitionItem(globalStateItemForPartitionCreation);
        } catch (final JsonProcessingException | PartitionUpdateException e) {
            LOG.warn("There was an error saving global state after creating partitions.", e);
        }
    }

}
