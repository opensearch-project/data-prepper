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
import org.opensearch.dataprepper.peerforwarder.HashRing;
import org.opensearch.dataprepper.sourcecoordination.PartitionManager;
import org.opensearch.dataprepper.sourcecoordination.SourcePartitionStatus;
import org.opensearch.dataprepper.sourcecoordination.exceptions.PartitionNotFoundException;
import org.opensearch.dataprepper.sourcecoordination.exceptions.PartitionNotOwnedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.enhanced.dynamodb.Expression;
import software.amazon.awssdk.enhanced.dynamodb.model.PageIterable;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * An implementation of {@link org.opensearch.dataprepper.model.source.SourceCoordinator} when DynamoDB is used at the distributed store
 * for source_coordination
 * @since 2.2
 */
public class DynamoDbSourceCoordinator implements SourceCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(DynamoDbSourceCoordinator.class);
    private static final Duration DEFAULT_LEASE_TIMEOUT = Duration.ofMinutes(8);
    private static final int MAX_CLOSED_COUNT = 5;

    private final DynamoStoreSettings dynamoStoreSettings;
    private final PartitionManager partitionManager;
    private final DynamoDbClientWrapper dynamoDbClientWrapper;
    private final HashRing hashRing;

    private final String ownerId;


    public DynamoDbSourceCoordinator(final SourceCoordinationConfig sourceCoordinationConfig,
                                     final PartitionManager partitionManager,
                                     final HashRing hashRing) {
        this.dynamoStoreSettings = ((DynamoDBSourceCoordinationStoreConfig) sourceCoordinationConfig.getSourceCoordinationStoreConfig()).getStoreSettings();
        this.partitionManager = partitionManager;
        this.hashRing = hashRing;
        this.dynamoDbClientWrapper = new DynamoDbClientWrapper(dynamoStoreSettings.getRegion(),
                dynamoStoreSettings.getStsRoleArn());

        this.ownerId = hashRing.getLocalEndpoint();

        dynamoDbClientWrapper.tryCreateTable(dynamoStoreSettings.getTableName(),
                constructProvisionedThroughput(dynamoStoreSettings.getProvisionedReadCapacityUnits(), dynamoStoreSettings.getProvisionedWriteCapacityUnits()));
    }

    @Override
    public void createPartitions(final List<String> partitionKeys) {
        // Can hash these to get only one node to handle the creation of partition keys to avoid throughput increase
        // Is this worth requiring service discovery?
        // PARTITION_CREATOR with ownerId
        if (hashRing.isHashedToThisServer(partitionKeys)) {
            for (final String partitionKey : partitionKeys) {
                // Probably don't need this, can lower to only 1 throughput
                final Optional<DynamoDbSourcePartitionItem> optionalPartitionItem = dynamoDbClientWrapper.getSourcePartitionItem(partitionKey);
                if (optionalPartitionItem.isEmpty()) {
                    final DynamoDbSourcePartitionItem newPartitionItem = initializeItemForPartition(partitionKey);
                    dynamoDbClientWrapper.tryCreatePartitionItem(newPartitionItem);
                    //continue;
                }
            }
        }
    }

    @Override
    public Optional<SourcePartition> getNextPartition() {

        if (partitionManager.getActivePartition().isPresent()) {
            final boolean renewedPartitionTimeout = renewPartitionOwnershipTimeout(partitionManager.getActivePartition().get().getPartitionKey());
            // Do anything if we aren't able to renew the partition ownership?
            if (!renewedPartitionTimeout) {
                LOG.warn("Unable to renew partition ownership for owner {} on source partition key {}", ownerId, partitionManager.getActivePartition().get().getPartitionKey());
            }

            return partitionManager.getActivePartition();
        }

        final PageIterable<DynamoDbSourcePartitionItem> dynamoDbSourcePartitionItemPageIterable =
                dynamoDbClientWrapper.getSourcePartitionItems(Expression.builder()
                        .expressionValues(Map.of(
                                ":s", AttributeValue.builder().s(SourcePartitionStatus.UNASSIGNED.name()).build(),
                                ":t", AttributeValue.builder().s(Instant.now().toString()).build(),
                                ":ro", AttributeValue.builder().s(Instant.now().toString()).build(),
                                ":null", AttributeValue.builder().nul(true).build()))
                        .expression("contains(sourcePartitionStatus, :s) " +
                                "and (attribute_not_exists(reOpenAt) or reOpenAt = :null or reOpenAt <= :ro) " +
                                "and (attribute_not_exists(partitionOwnershipTimeout) or partitionOwnershipTimeout = :null or partitionOwnershipTimeout <= :t)")
                        .build());

        for (final DynamoDbSourcePartitionItem item : dynamoDbSourcePartitionItemPageIterable.items()) {
            final boolean acquired = tryAcquireItem(item);

            if (acquired) {
                final SourcePartition sourcePartition = SourcePartition.builder()
                        .withPartitionKey(item.getSourcePartitionKey())
                        .withPartitionState(item.getPartitionProgressState())
                        .build();
                partitionManager.setActivePartition(sourcePartition);
                return Optional.of(sourcePartition);
            }
        }

        return Optional.empty();
    }

    @Override
    public void completePartition(final String partitionKey) {

        if (!isActivelyOwnedPartition(partitionKey)) {
            throw new PartitionNotOwnedException(String.format("Unable to complete the partition because partition key %s is not owned by this node", partitionKey));
        }

        final Optional<DynamoDbSourcePartitionItem> optionalItem = dynamoDbClientWrapper.getSourcePartitionItem(partitionKey);
        if (optionalItem.isEmpty()) {
            throw new PartitionNotFoundException(String.format("Unable to complete the partition because partition key %s was not found", partitionKey));
        }

        final DynamoDbSourcePartitionItem item = optionalItem.get();
        item.setPartitionOwner(null);
        item.setReOpenAt(null);
        item.setPartitionOwnershipTimeout(null);
        item.setSourcePartitionStatus(SourcePartitionStatus.COMPLETED);

        // TTL for when marking as COMPLETED?
        final boolean completedSuccessfully = dynamoDbClientWrapper.updatePartitionItem(item);
        // Do anything if we don't mark the partition as completed. retry?
        partitionManager.removeActivePartition();
    }

    @Override
    public void closePartition(final String partitionKey, Duration reopenAfter) {
        if (!isActivelyOwnedPartition(partitionKey)) {
            throw new PartitionNotOwnedException(String.format("Unable to close the partition because partition key %s is not owned by this node", partitionKey));
        }

        final Optional<DynamoDbSourcePartitionItem> optionalItem = dynamoDbClientWrapper.getSourcePartitionItem(partitionKey);
        if (optionalItem.isEmpty()) {
            throw new PartitionNotFoundException(String.format("Unable to close the partition because partition key %s was not found", partitionKey));
        }

        final DynamoDbSourcePartitionItem item = optionalItem.get();
        item.setPartitionOwner(null);

        if (item.getClosedCount() >= MAX_CLOSED_COUNT) {
            item.setSourcePartitionStatus(SourcePartitionStatus.COMPLETED);
        } else {
            item.setSourcePartitionStatus(SourcePartitionStatus.CLOSED);
            item.setClosedCount(item.getClosedCount() + 1L);
            item.setReOpenAt(Instant.now().plus(reopenAfter));
        }

        final boolean closedSuccessfully = dynamoDbClientWrapper.updatePartitionItem(item);
        // Do anything if not closed successfully? Probably just retry
        if (!closedSuccessfully) {
            LOG.error("Unable to close the partition for partition key {}", item.getSourcePartitionKey());
        }

        partitionManager.removeActivePartition();
    }

    @Override
    public void saveStateForPartition(final String partitionKey, Object partitionState) {

        if (!isActivelyOwnedPartition(partitionKey)) {
            throw new PartitionNotOwnedException(String.format("Unable to save state for the partition because partition key %s is not owned by this node", partitionKey));
        }
        final Optional<DynamoDbSourcePartitionItem> optionalItem = dynamoDbClientWrapper.getSourcePartitionItem(partitionKey);
        if (optionalItem.isEmpty()) {
            throw new PartitionNotFoundException(String.format("Unable to save state for the partition because partition key %s was not found", partitionKey));
        }

        final DynamoDbSourcePartitionItem item = optionalItem.get();
        item.setPartitionProgressState(partitionState.toString());

        final boolean savedStateSuccessfully = dynamoDbClientWrapper.updatePartitionItem(item);
        // Do anything if state isn't saved successfully? Just retry by reading and updating again?
        if (!savedStateSuccessfully) {
            LOG.error("Unable to save state for owner {} and partition key {}", item.getPartitionOwner(), item.getSourcePartitionKey());
        }
    }

    @Override
    public void giveUpPartitions() {
        partitionManager.removeActivePartition();
    }

    private ProvisionedThroughput constructProvisionedThroughput(final Long readCapacityUnits,
                                                                 final Long writeCapacityUnits) {
        return ProvisionedThroughput.builder()
                .readCapacityUnits(readCapacityUnits)
                .writeCapacityUnits(writeCapacityUnits)
                .build();
    }

    private boolean tryAcquireItem(final DynamoDbSourcePartitionItem item) {
        item.setPartitionOwner(ownerId);
        item.setSourcePartitionStatus(SourcePartitionStatus.ASSIGNED);
        item.setPartitionOwnershipTimeout(Instant.now().plus(DEFAULT_LEASE_TIMEOUT));

        return dynamoDbClientWrapper.updatePartitionItem(item);
    }

    private DynamoDbSourcePartitionItem initializeItemForPartition(final String partitionKey) {
        final DynamoDbSourcePartitionItem item = new DynamoDbSourcePartitionItem();
        item.setSourcePartitionKey(partitionKey);
        item.setSourcePartitionStatus(SourcePartitionStatus.UNASSIGNED);
        item.setClosedCount(0L);
        item.setPartitionProgressState(null);
        item.setVersion(0L);

        return item;
    }

    private boolean isActivelyOwnedPartition(final String partitionKey) {
        final Optional<SourcePartition> activePartition = partitionManager.getActivePartition();
        return activePartition.isPresent() && activePartition.get().getPartitionKey().equals(partitionKey);
    }

    private boolean renewPartitionOwnershipTimeout(final String partitionKey) {
        final Optional<DynamoDbSourcePartitionItem> item = dynamoDbClientWrapper.getSourcePartitionItem(partitionKey);

        if (item.isPresent()) {
            final DynamoDbSourcePartitionItem updateItem = item.get();
            updateItem.setPartitionOwnershipTimeout(Instant.now().plus(DEFAULT_LEASE_TIMEOUT));
            return dynamoDbClientWrapper.updatePartitionItem(updateItem);
        }

        return false;
    }
}
