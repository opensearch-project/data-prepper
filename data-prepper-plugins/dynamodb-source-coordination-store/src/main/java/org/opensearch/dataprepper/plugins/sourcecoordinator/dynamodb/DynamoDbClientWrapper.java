/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sourcecoordinator.dynamodb;

import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionUpdateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.internal.waiters.ResponseOrException;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Expression;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.CreateTableEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.GetItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.PageIterable;
import software.amazon.awssdk.enhanced.dynamodb.model.PutItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.ScanEnhancedRequest;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class DynamoDbClientWrapper {

    private static final Logger LOG = LoggerFactory.getLogger(DynamoDbClientWrapper.class);

    static final String ITEM_DOES_NOT_EXIST_EXPRESSION = "attribute_not_exists(sourcePartitionKey)";
    static final String ITEM_EXISTS_AND_HAS_LATEST_VERSION = "attribute_exists(sourcePartitionKey) and version = :v";

    private final DynamoDbEnhancedClient dynamoDbEnhancedClient;
    private DynamoDbTable<DynamoDbSourcePartitionItem> table;


    private DynamoDbClientWrapper(final String region, final String stsRoleArn) {
        this.dynamoDbEnhancedClient = DynamoDbClientFactory.provideDynamoDbEnhancedClient(region, stsRoleArn);
    }

    public static DynamoDbClientWrapper create(final String region, final String stsRoleArn) {
        return new DynamoDbClientWrapper(region, stsRoleArn);
    }

    public void tryCreateTable(final String tableName,
                                  final ProvisionedThroughput provisionedThroughput) {

        this.table = dynamoDbEnhancedClient.table(tableName, TableSchema.fromBean(DynamoDbSourcePartitionItem.class));

        try {
            final CreateTableEnhancedRequest createTableEnhancedRequest = CreateTableEnhancedRequest.builder()
                            .provisionedThroughput(provisionedThroughput)
                            .build();
            table.createTable(createTableEnhancedRequest);
        } catch (final ResourceInUseException e) {
            LOG.info("The table creation for {} was already triggered by another instance of data prepper", tableName);
        }

        try(final DynamoDbWaiter dynamoDbWaiter = DynamoDbWaiter.create()) {
            final DescribeTableRequest describeTableRequest = DescribeTableRequest.builder().tableName(tableName).build();
            final ResponseOrException<DescribeTableResponse> response = dynamoDbWaiter
                    .waitUntilTableExists(describeTableRequest)
                    .matched();

            final DescribeTableResponse describeTableResponse = response.response().orElseThrow(
                    () -> new RuntimeException(String.format("Table %s was not created successfully", tableName))
            );

            LOG.info("DynamoDB table {} was created successfully for source coordination", describeTableResponse.table().tableName());
        }
    }

    public boolean tryCreatePartitionItem(final DynamoDbSourcePartitionItem dynamoDbSourcePartitionItem) {

        try {
            table.putItem(PutItemEnhancedRequest.builder(DynamoDbSourcePartitionItem.class)
                    .item(dynamoDbSourcePartitionItem)
                    .conditionExpression(Expression.builder()
                            .expression(ITEM_DOES_NOT_EXIST_EXPRESSION)
                            .build())
                    .build());
            return true;
        } catch (final ConditionalCheckFailedException e) {
            return false;
        } catch (final Exception e) {
            LOG.error("An exception occurred while attempting to create a DynamoDb partition item {}", dynamoDbSourcePartitionItem.getSourcePartitionKey());
            return false;
        }
    }

    public boolean tryAcquirePartitionItem(final DynamoDbSourcePartitionItem dynamoDbSourcePartitionItem) {
        try {
            tryUpdateItem(dynamoDbSourcePartitionItem);
            return true;
        } catch (final Exception e) {
            return false;
        }
    }

    public void tryUpdatePartitionItem(final DynamoDbSourcePartitionItem dynamoDbSourcePartitionItem) {
        try {
            tryUpdateItem(dynamoDbSourcePartitionItem);
        } catch (final PartitionUpdateException e) {
            LOG.warn(e.getMessage(), e);
            throw e;
        }
    }

    private void tryUpdateItem(final DynamoDbSourcePartitionItem dynamoDbSourcePartitionItem) {
        try {
            dynamoDbSourcePartitionItem.setVersion(dynamoDbSourcePartitionItem.getVersion() + 1L);
            table.putItem(PutItemEnhancedRequest.builder(DynamoDbSourcePartitionItem.class)
                    .item(dynamoDbSourcePartitionItem)
                    .conditionExpression(Expression.builder()
                            .expression(ITEM_EXISTS_AND_HAS_LATEST_VERSION)
                            .expressionValues(Map.of(":v", AttributeValue.builder().n(String.valueOf(dynamoDbSourcePartitionItem.getVersion() - 1L)).build()))
                            .build())
                    .build());
        } catch (final ConditionalCheckFailedException e) {
            final String message = String.format(
                    "ConditionalCheckFailed while updating partition %s. This partition item was either deleted from the dynamo table, " +
                            "or another instance of Data Prepper has modified it.",
                    dynamoDbSourcePartitionItem.getSourcePartitionKey());
            throw new PartitionUpdateException(message, e);
        } catch (final Exception e) {
            final String errorMessage = String.format("An exception occurred while attempting to update a DynamoDb partition item %s",
                    dynamoDbSourcePartitionItem.getSourcePartitionKey());
            LOG.error(errorMessage, e);
            throw new PartitionUpdateException(errorMessage, e);
        }
    }

    public Optional<SourcePartitionStoreItem> getSourcePartitionItem(final String sourcePartitionKey) {
        try {
            final Key key = Key.builder()
                    .partitionValue(sourcePartitionKey)
                    .build();

            final SourcePartitionStoreItem result = table.getItem(GetItemEnhancedRequest.builder().key(key).build());

            if (Objects.isNull(result)) {
                return Optional.empty();
            }
            return Optional.of(result);
        } catch (final Exception e) {
            LOG.error("An exception occurred while attempting to get a DynamoDb partition item for {}", sourcePartitionKey, e);
            return Optional.empty();
        }
    }

    public Optional<PageIterable<DynamoDbSourcePartitionItem>> getSourcePartitionItems(final Expression filterExpression) {
        final ScanEnhancedRequest scanRequest = ScanEnhancedRequest.builder()
                .filterExpression(filterExpression)
                .limit(20)
                .build();

        try {
            return Optional.of(table.scan(scanRequest));
        } catch (final Exception e) {
            LOG.error("An exception occurred while attempting to get source partition items to acquire in DynamoDb", e);
        }

        return Optional.empty();
    }
}

