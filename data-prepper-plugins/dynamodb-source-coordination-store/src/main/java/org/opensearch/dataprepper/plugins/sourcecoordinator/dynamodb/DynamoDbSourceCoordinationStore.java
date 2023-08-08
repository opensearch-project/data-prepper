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
import java.util.Objects;
import java.util.Optional;

/**
 * An implementation of {@link org.opensearch.dataprepper.model.source.SourceCoordinationStore} when DynamoDB is used at the distributed store
 * for source_coordination
 * @since 2.2
 */

@DataPrepperPlugin(name = "dynamodb", pluginType = SourceCoordinationStore.class, pluginConfigurationType = DynamoStoreSettings.class )
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
    public boolean tryCreatePartitionItem(final String sourceIdentifier,
                                          final String sourcePartitionKey,
                                          final SourcePartitionStatus sourcePartitionStatus,
                                          final Long closedCount,
                                          final String partitionProgressState) {
        final DynamoDbSourcePartitionItem newPartitionItem = new DynamoDbSourcePartitionItem();

        if (Objects.nonNull(dynamoStoreSettings.getTtl())) {
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
                String.format(SOURCE_STATUS_COMBINATION_KEY_FORMAT, sourceIdentifier, SourcePartitionStatus.ASSIGNED), 1);

        if (acquiredAssignedItem.isPresent()) {
            return acquiredAssignedItem;
        }

        final Optional<SourcePartitionStoreItem> acquiredClosedItem = dynamoDbClientWrapper.getAvailablePartition(
                ownerId, ownershipTimeout, SourcePartitionStatus.CLOSED,
                String.format(SOURCE_STATUS_COMBINATION_KEY_FORMAT, sourceIdentifier, SourcePartitionStatus.CLOSED), 1);

        if (acquiredClosedItem.isPresent()) {
            return acquiredClosedItem;
        }

        return dynamoDbClientWrapper.getAvailablePartition(
                ownerId, ownershipTimeout, SourcePartitionStatus.UNASSIGNED,
                String.format(SOURCE_STATUS_COMBINATION_KEY_FORMAT, sourceIdentifier, SourcePartitionStatus.UNASSIGNED), 5);
    }

    @Override
    public void tryUpdateSourcePartitionItem(final SourcePartitionStoreItem updateItem) {
        final DynamoDbSourcePartitionItem dynamoDbSourcePartitionItem = (DynamoDbSourcePartitionItem) updateItem;
        dynamoDbSourcePartitionItem.setSourceStatusCombinationKey(
                String.format(SOURCE_STATUS_COMBINATION_KEY_FORMAT, updateItem.getSourceIdentifier(), updateItem.getSourcePartitionStatus()));

        if (SourcePartitionStatus.CLOSED.equals(updateItem.getSourcePartitionStatus())) {
            dynamoDbSourcePartitionItem.setPartitionPriority(updateItem.getReOpenAt().toString());
        }

        if (SourcePartitionStatus.ASSIGNED.equals(updateItem.getSourcePartitionStatus())) {
            dynamoDbSourcePartitionItem.setPartitionPriority(updateItem.getPartitionOwnershipTimeout().toString());
        }

        dynamoDbClientWrapper.tryUpdatePartitionItem(dynamoDbSourcePartitionItem);
    }

    private ProvisionedThroughput constructProvisionedThroughput(final Long readCapacityUnits,
                                                                 final Long writeCapacityUnits) {
        return ProvisionedThroughput.builder()
                .readCapacityUnits(readCapacityUnits)
                .writeCapacityUnits(writeCapacityUnits)
                .build();
    }
}
