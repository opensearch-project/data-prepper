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
import software.amazon.awssdk.enhanced.dynamodb.Expression;
import software.amazon.awssdk.enhanced.dynamodb.model.PageIterable;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/**
 * An implementation of {@link org.opensearch.dataprepper.model.source.SourceCoordinationStore} when DynamoDB is used at the distributed store
 * for source_coordination
 * @since 2.2
 */

@DataPrepperPlugin(name = "dynamodb", pluginType = SourceCoordinationStore.class, pluginConfigurationType = DynamoStoreSettings.class )
public class DynamoDbSourceCoordinationStore implements SourceCoordinationStore {

    private static final Logger LOG = LoggerFactory.getLogger(DynamoDbSourceCoordinationStore.class);

    /**
     * This filter expression considers partitions available in the following scenarios
     * 1. The partition status is UNASSIGNED
     * 2. The partition status is CLOSED and the reOpenAt timestamp has passed
     * 3. The partition status is ASSIGNED and the partitionOwnershipTimeout has passed
     */
    static final String AVAILABLE_PARTITIONS_FILTER_EXPRESSION = "contains(sourcePartitionStatus, :unassigned) " +
            "or (contains(sourcePartitionStatus, :closed) and (attribute_not_exists(reOpenAt) or reOpenAt = :null or reOpenAt <= :ro)) " +
            "or (contains(sourcePartitionStatus, :assigned) and (attribute_not_exists(partitionOwnershipTimeout) or partitionOwnershipTimeout = :null or partitionOwnershipTimeout <= :t))";

    private final DynamoStoreSettings dynamoStoreSettings;
    private final PluginMetrics pluginMetrics;
    private final DynamoDbClientWrapper dynamoDbClientWrapper;

    @DataPrepperPluginConstructor
    public DynamoDbSourceCoordinationStore(final DynamoStoreSettings dynamoStoreSettings, final PluginMetrics pluginMetrics) {
        this.dynamoStoreSettings = dynamoStoreSettings;
        this.pluginMetrics = pluginMetrics;
        this.dynamoDbClientWrapper = DynamoDbClientWrapper.create(dynamoStoreSettings.getRegion(), dynamoStoreSettings.getStsRoleArn());
    }

    @Override
    public void initializeStore() {
        dynamoDbClientWrapper.tryCreateTable(dynamoStoreSettings.getTableName(), constructProvisionedThroughput(
                dynamoStoreSettings.getProvisionedReadCapacityUnits(), dynamoStoreSettings.getProvisionedWriteCapacityUnits()));
    }

    @Override
    public Optional<SourcePartitionStoreItem> getSourcePartitionItem(final String partitionKey) {
        return dynamoDbClientWrapper.getSourcePartitionItem(partitionKey);
    }

    @Override
    public boolean tryCreatePartitionItem(final String partitionKey,
                                          final SourcePartitionStatus sourcePartitionStatus,
                                          final Long closedCount,
                                          final String partitionProgressState) {
        final DynamoDbSourcePartitionItem newPartitionItem = new DynamoDbSourcePartitionItem();
        newPartitionItem.setSourcePartitionKey(partitionKey);
        newPartitionItem.setSourcePartitionStatus(sourcePartitionStatus);
        newPartitionItem.setClosedCount(closedCount);
        newPartitionItem.setPartitionProgressState(partitionProgressState);
        newPartitionItem.setVersion(0L);

        return dynamoDbClientWrapper.tryCreatePartitionItem(newPartitionItem);
    }

    @Override
    public Optional<SourcePartitionStoreItem> tryAcquireAvailablePartition(final String ownerId, final Duration ownershipTimeout) {
        final Optional<PageIterable<DynamoDbSourcePartitionItem>> dynamoDbSourcePartitionItemPageIterable =
                dynamoDbClientWrapper.getSourcePartitionItems(Expression.builder()
                        .expressionValues(Map.of(
                                ":unassigned", AttributeValue.builder().s(SourcePartitionStatus.UNASSIGNED.name()).build(),
                                ":assigned", AttributeValue.builder().s(SourcePartitionStatus.ASSIGNED.name()).build(),
                                ":closed", AttributeValue.builder().s(SourcePartitionStatus.CLOSED.name()).build(),
                                ":t", AttributeValue.builder().s(Instant.now().toString()).build(),
                                ":ro", AttributeValue.builder().s(Instant.now().toString()).build(),
                                ":null", AttributeValue.builder().nul(true).build()))
                        .expression(AVAILABLE_PARTITIONS_FILTER_EXPRESSION)
                        .build());

        if (dynamoDbSourcePartitionItemPageIterable.isEmpty()) {
            return Optional.empty();
        }

        try {
            for (final DynamoDbSourcePartitionItem item : dynamoDbSourcePartitionItemPageIterable.get().items()) {
                item.setPartitionOwner(ownerId);
                item.setPartitionOwnershipTimeout(Instant.now().plus(ownershipTimeout));
                item.setSourcePartitionStatus(SourcePartitionStatus.ASSIGNED);
                final boolean acquired = dynamoDbClientWrapper.tryAcquirePartitionItem(item);

                if (acquired) {
                    return Optional.of(item);
                }
            }
        } catch (final Exception e) {
            LOG.error("An exception occurred while iterating on items to acquire in DynamoDb", e);
        }

        return Optional.empty();
    }

    @Override
    public void tryUpdateSourcePartitionItem(final SourcePartitionStoreItem updateItem) {
        final DynamoDbSourcePartitionItem dynamoDbSourcePartitionItem = (DynamoDbSourcePartitionItem) updateItem;
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
