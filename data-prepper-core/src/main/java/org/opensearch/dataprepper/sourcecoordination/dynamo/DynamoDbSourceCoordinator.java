/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.sourcecoordination.dynamo;

import org.opensearch.dataprepper.model.source.SourceCoordinator;
import org.opensearch.dataprepper.model.source.SourcePartition;
import org.opensearch.dataprepper.parser.model.sourcecoordination.DynamoDBSourceCoordinationStoreConfig;
import org.opensearch.dataprepper.parser.model.sourcecoordination.DynamoStoreSettings;
import org.opensearch.dataprepper.parser.model.sourcecoordination.SourceCoordinationConfig;
import org.opensearch.dataprepper.sourcecoordination.PartitionManager;
import org.opensearch.dataprepper.sourcecoordination.SourcePartitionStatus;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;

/**
 * An implementation of {@link org.opensearch.dataprepper.model.source.SourceCoordinator} when DynamoDB is used at the distributed store
 * for source_coordination
 * @since 2.2
 */
public class DynamoDbSourceCoordinator implements SourceCoordinator {

    private final DynamoStoreSettings dynamoStoreSettings;
    private final PartitionManager partitionManager;
    private final DynamoDbClientWrapper dynamoDbClientWrapper;

    private static final String ownerId = UUID.randomUUID().toString();
    private static final int ownerNumber = new Random().nextInt(5);
    private static final Duration DEFAULT_LEASE_TIMEOUT = Duration.ofMinutes(8);


    public DynamoDbSourceCoordinator(final SourceCoordinationConfig sourceCoordinationConfig,
                                     final PartitionManager partitionManager) {
        this.dynamoStoreSettings = ((DynamoDBSourceCoordinationStoreConfig) sourceCoordinationConfig.getSourceCoordinationStoreConfig()).getStoreSettings();
        this.partitionManager = partitionManager;
        this.dynamoDbClientWrapper = new DynamoDbClientWrapper(dynamoStoreSettings.getRegion(),
                dynamoStoreSettings.getStsRoleArn());

        dynamoDbClientWrapper.tryCreateTable(dynamoStoreSettings.getTableName(),
                constructProvisionedThroughput(dynamoStoreSettings.getProvisionedReadCapacityUnits(), dynamoStoreSettings.getProvisionedWriteCapacityUnits()));
    }

    @Override
    public void createPartitions(final List<String> partitionKeys) {
        // Use BatchGetItem here as potential optimization
        for (final String partitionKey : partitionKeys) {
            final Optional<DynamoDbSourcePartitionItem> optionalPartitionItem = dynamoDbClientWrapper.getSourcePartitionItem(partitionKey);
            if (optionalPartitionItem.isEmpty()) {
                final DynamoDbSourcePartitionItem newPartitionItem = initializeItemForPartition(partitionKey);
                dynamoDbClientWrapper.tryCreatePartitionItem(newPartitionItem);
                continue;
            }

            final DynamoDbSourcePartitionItem existingPartitionItem = optionalPartitionItem.get();

            if (existingPartitionItem.getPartitionOwner().equals(ownerId)) {
                final DynamoDbSourcePartitionItem copy = existingPartitionItem;
                copy.setVersion(existingPartitionItem.getVersion() + 1L);
                if (new Random().nextInt(5) == ownerNumber) {
                    copy.setPartitionOwnershipTimeout(Instant.now().plus(DEFAULT_LEASE_TIMEOUT));
                } else {
                    // How can we just take away ownership in the middle of processing? Use reopenAt to block other nodes from grabbing and allow for 1 more saveProgressState call?
                    // Could also potentially notify the source that it is going to lose ownership and give it time to take action
                    copy.setPartitionOwner(null);
                    copy.setSourcePartitionStatus(SourcePartitionStatus.UNASSIGNED);
                    // If we don't unassign when partition key is not hashed to the node anymore, how do we allow new nodes (when increasing cluster size)
                    // to pick up new partitions? One solution would be to only assign one partition at a time, then there is no need to rehash existing owned partitions
                }
                dynamoDbClientWrapper.updatePartitionItem(copy);
                continue;
            }

            if ((SourcePartitionStatus.UNASSIGNED.equals(existingPartitionItem.getSourcePartitionStatus())
                || existingPartitionItem.getReOpenAt().isBefore(Instant.now())
                || existingPartitionItem.getPartitionOwnershipTimeout().isBefore(Instant.now()))
                    && new Random().nextInt(5) == ownerNumber) {
                // Need to reread and check these again if conditional check fails

                existingPartitionItem.setPartitionOwner(ownerId);
                existingPartitionItem.setVersion(existingPartitionItem.getVersion() + 1L);
                existingPartitionItem.setSourcePartitionStatus(SourcePartitionStatus.ASSIGNED);
                existingPartitionItem.setPartitionOwnershipTimeout(Instant.now().plus(DEFAULT_LEASE_TIMEOUT));

                final boolean acquiredPartition = dynamoDbClientWrapper.updatePartitionItem(existingPartitionItem);

                if (acquiredPartition) {
                    partitionManager.queuePartition(SourcePartition.builder()
                            .withPartitionKey(partitionKey)
                            .withPartitionState(existingPartitionItem.getPartitionProgressState())
                            .build());
                }
            }
        }
    }

    @Override
    public Optional<SourcePartition> getNextPartition() {
        return partitionManager.getNextPartition();
    }

    @Override
    public void completePartition(final String partitionKey) {
        final Optional<DynamoDbSourcePartitionItem> optionalItem = dynamoDbClientWrapper.getSourcePartitionItem(partitionKey);

        if (optionalItem.isEmpty()) {
            return;
        }

        final DynamoDbSourcePartitionItem item = optionalItem.get();
        item.setVersion(item.getVersion() + 1L);
        item.setPartitionOwner(null);
        item.setSourcePartitionStatus(SourcePartitionStatus.COMPLETED);

        dynamoDbClientWrapper.updatePartitionItem(item);

        partitionManager.removePartition(partitionKey);
    }

    @Override
    public void closePartition(final String partitionKey, Duration reopenAfter) {
        final Optional<DynamoDbSourcePartitionItem> optionalItem = dynamoDbClientWrapper.getSourcePartitionItem(partitionKey);

        if (optionalItem.isEmpty()) {
            return;
        }

        final DynamoDbSourcePartitionItem item = optionalItem.get();
        item.setVersion(item.getVersion() + 1L);
        item.setPartitionOwner(null);
        item.setSourcePartitionStatus(SourcePartitionStatus.CLOSED);
        item.setClosedCount(item.getClosedCount() + 1L);
        item.setReOpenAt(Instant.now().plus(reopenAfter));

        dynamoDbClientWrapper.updatePartitionItem(item);

        partitionManager.removePartition(partitionKey);
    }

    @Override
    public void saveStateForPartition(final String partitionKey, Object partitionState) {
        final Optional<DynamoDbSourcePartitionItem> optionalItem = dynamoDbClientWrapper.getSourcePartitionItem(partitionKey);

        if (optionalItem.isEmpty()) {
            // Return status to source saying the item wasn't found? Can the source even do anything?
            return;
        }

        final DynamoDbSourcePartitionItem item = optionalItem.get();
        item.setVersion(item.getVersion() + 1L);
        item.setPartitionProgressState(partitionState);

        dynamoDbClientWrapper.updatePartitionItem(item);
    }

    @Override
    public void giveUpPartitions() {
        partitionManager.clearOwnedPartitions();
    }

    private ProvisionedThroughput constructProvisionedThroughput(final Long readCapacityUnits,
                                                                 final Long writeCapacityUnits) {
        return ProvisionedThroughput.builder()
                .readCapacityUnits(readCapacityUnits)
                .writeCapacityUnits(writeCapacityUnits)
                .build();
    }

    private DynamoDbSourcePartitionItem initializeItemForPartition(final String partitionKey) {
        final DynamoDbSourcePartitionItem item = new DynamoDbSourcePartitionItem();
        item.setSourcePartitionKey(partitionKey);
        item.setPartitionOwner(ownerId);
        item.setSourcePartitionStatus(SourcePartitionStatus.UNASSIGNED);
        item.setClosedCount(0L);
        item.setPartitionProgressState(null);
        item.setVersion(0L);

        return item;
    }
}
