/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sourcecoordinator.dynamodb;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.source.SourceCoordinationStore;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStatus;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * An implementation of {@link org.opensearch.dataprepper.model.source.SourceCoordinationStore} when DynamoDB is used at the distributed store
 * for source_coordination
 *
 * @since 2.2
 */

@DataPrepperPlugin(name = "dynamodb", pluginType = SourceCoordinationStore.class, pluginConfigurationType = DynamoStoreSettings.class)
public class DynamoDbSourceCoordinationStore implements SourceCoordinationStore {

    private static final Logger LOG = LoggerFactory.getLogger(DynamoDbSourceCoordinationStore.class);

    private final DynamoStoreSettings dynamoStoreSettings;
    private final PluginMetrics pluginMetrics;
    private final DynamoDbClientWrapper dynamoDbClientWrapper;

    static final String SOURCE_STATUS_COMBINATION_KEY_FORMAT = "%s|%s";

    @DataPrepperPluginConstructor
    public DynamoDbSourceCoordinationStore(final DynamoStoreSettings dynamoStoreSettings, final PluginMetrics pluginMetrics) {
        this.dynamoStoreSettings = dynamoStoreSettings;
        this.pluginMetrics = pluginMetrics;
        this.dynamoDbClientWrapper = DynamoDbClientWrapper.create(
                dynamoStoreSettings.getRegion(),
                dynamoStoreSettings.getStsRoleArn(),
                dynamoStoreSettings.getStsExternalId());
    }

    @Override
    public void initializeStore() {
        dynamoDbClientWrapper.initializeTable(dynamoStoreSettings, constructProvisionedThroughput(
                dynamoStoreSettings.getProvisionedReadCapacityUnits(), dynamoStoreSettings.getProvisionedWriteCapacityUnits()));
    }

    @Override
    public Optional<SourcePartitionStoreItem> getSourcePartitionItem(final String sourceIdentifier, final String sourcePartitionKey) {
        return dynamoDbClientWrapper.getSourcePartitionItem(sourceIdentifier, sourcePartitionKey);
    }

    @Override
    public List<SourcePartitionStoreItem> querySourcePartitionItemsByStatus(final String sourceIdentifier, final SourcePartitionStatus sourcePartitionStatus, final String startPartitionPriority) {
        String statusKey = String.format(SOURCE_STATUS_COMBINATION_KEY_FORMAT, sourceIdentifier, sourcePartitionStatus);
        return dynamoDbClientWrapper.queryPartitionsByStatus(statusKey, startPartitionPriority);
    }

    public List<SourcePartitionStoreItem> queryAllSourcePartitionItems(final String sourceIdentifier) {
        return dynamoDbClientWrapper.queryAllPartitions(sourceIdentifier);
    }

    @Override
    public boolean tryCreatePartitionItem(final String sourceIdentifier,
                                          final String sourcePartitionKey,
                                          final SourcePartitionStatus sourcePartitionStatus,
                                          final Long closedCount,
                                          final String partitionProgressState,
                                          final boolean isReadOnlyItem) {
        final DynamoDbSourcePartitionItem newPartitionItem = new DynamoDbSourcePartitionItem();

        if (!isReadOnlyItem && Objects.nonNull(dynamoStoreSettings.getTtl())) {
            newPartitionItem.setExpirationTime(Instant.now().plus(dynamoStoreSettings.getTtl()).getEpochSecond());
        }
        newPartitionItem.setSourceIdentifier(sourceIdentifier);
        newPartitionItem.setSourceStatusCombinationKey(String.format(SOURCE_STATUS_COMBINATION_KEY_FORMAT, sourceIdentifier, sourcePartitionStatus));
        newPartitionItem.setPartitionPriority(Instant.now().toString());
        newPartitionItem.setSourcePartitionKey(sourcePartitionKey);
        newPartitionItem.setSourcePartitionStatus(sourcePartitionStatus);
        newPartitionItem.setClosedCount(closedCount);
        newPartitionItem.setPartitionProgressState(partitionProgressState);
        newPartitionItem.setVersion(0L);

        return dynamoDbClientWrapper.tryCreatePartitionItem(newPartitionItem);
    }

    @Override
    public Optional<SourcePartitionStoreItem> tryAcquireAvailablePartition(final String sourceIdentifier, final String ownerId, final Duration ownershipTimeout) {
        final Optional<SourcePartitionStoreItem> acquiredAssignedItem = dynamoDbClientWrapper.getAvailablePartition(
                ownerId, ownershipTimeout, SourcePartitionStatus.ASSIGNED,
                String.format(SOURCE_STATUS_COMBINATION_KEY_FORMAT, sourceIdentifier, SourcePartitionStatus.ASSIGNED), 1, dynamoStoreSettings.getTtl());

        if (acquiredAssignedItem.isPresent()) {
            return acquiredAssignedItem;
        }

        final Optional<SourcePartitionStoreItem> acquiredUnassignedItem = dynamoDbClientWrapper.getAvailablePartition(
                ownerId, ownershipTimeout, SourcePartitionStatus.UNASSIGNED,
                String.format(SOURCE_STATUS_COMBINATION_KEY_FORMAT, sourceIdentifier, SourcePartitionStatus.UNASSIGNED), 5, dynamoStoreSettings.getTtl());

        if (acquiredUnassignedItem.isPresent()) {
            return acquiredUnassignedItem;
        }

        return dynamoDbClientWrapper.getAvailablePartition(
                ownerId, ownershipTimeout, SourcePartitionStatus.CLOSED,
                String.format(SOURCE_STATUS_COMBINATION_KEY_FORMAT, sourceIdentifier, SourcePartitionStatus.CLOSED), 1, dynamoStoreSettings.getTtl());
    }

    @Override
    public void tryUpdateSourcePartitionItem(final SourcePartitionStoreItem updateItem) {
        tryUpdateSourcePartitionItemInternal(updateItem, null);
    }

    @Override
    public void tryUpdateSourcePartitionItem(final SourcePartitionStoreItem updateItem, final Instant priorityOverride) {
        tryUpdateSourcePartitionItemInternal(updateItem, priorityOverride);
    }

    private void tryUpdateSourcePartitionItemInternal(final SourcePartitionStoreItem updateItem, final Instant priorityOverride) {
        final DynamoDbSourcePartitionItem dynamoDbSourcePartitionItem = (DynamoDbSourcePartitionItem) updateItem;
        dynamoDbSourcePartitionItem.setSourceStatusCombinationKey(
                String.format(SOURCE_STATUS_COMBINATION_KEY_FORMAT, updateItem.getSourceIdentifier(), updateItem.getSourcePartitionStatus()));

        if (SourcePartitionStatus.CLOSED.equals(updateItem.getSourcePartitionStatus())) {
            dynamoDbSourcePartitionItem.setPartitionPriority(updateItem.getReOpenAt().toString());
        }

        if (SourcePartitionStatus.ASSIGNED.equals(updateItem.getSourcePartitionStatus())) {
            dynamoDbSourcePartitionItem.setPartitionPriority(updateItem.getPartitionOwnershipTimeout().toString());
        }

        if (priorityOverride != null && SourcePartitionStatus.UNASSIGNED.equals(updateItem.getSourcePartitionStatus())) {
            dynamoDbSourcePartitionItem.setPartitionPriority(priorityOverride.toString());
        }

        if (Objects.nonNull(dynamoStoreSettings.getTtl())) {
            dynamoDbSourcePartitionItem.setExpirationTime(Instant.now().plus(dynamoStoreSettings.getTtl()).getEpochSecond());
        }

        dynamoDbClientWrapper.tryUpdatePartitionItem(dynamoDbSourcePartitionItem);
    }

    @Override
    public void tryDeletePartitionItem(final SourcePartitionStoreItem deleteItem) {
        dynamoDbClientWrapper.tryDeletePartitionItem((DynamoDbSourcePartitionItem) deleteItem);
    }

    private ProvisionedThroughput constructProvisionedThroughput(final Long readCapacityUnits,
                                                                 final Long writeCapacityUnits) {
        return ProvisionedThroughput.builder()
                .readCapacityUnits(readCapacityUnits)
                .writeCapacityUnits(writeCapacityUnits)
                .build();
    }
}
