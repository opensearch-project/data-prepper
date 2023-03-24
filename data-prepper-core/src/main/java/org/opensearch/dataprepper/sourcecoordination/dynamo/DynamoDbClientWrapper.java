/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.sourcecoordination.dynamo;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.internal.waiters.ResponseOrException;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Expression;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.PutItemEnhancedRequest;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.util.Optional;
import java.util.UUID;

public class DynamoDbClientWrapper {

    private static final Logger LOG = LoggerFactory.getLogger(DynamoDbClientWrapper.class);

    private final DynamoDbEnhancedClient dynamoDbEnhancedClient;
    private DynamoDbTable<DynamoDbSourcePartitionItem> table;


    public DynamoDbClientWrapper(final String region, final String stsRoleArn) {
        final DynamoDbClient dynamoDbClient = DynamoDbClient.builder()
                .region(Region.of(region))
                .credentialsProvider(getAwsCredentials(Region.of(region), stsRoleArn))
                .build();

        this.dynamoDbEnhancedClient = DynamoDbEnhancedClient.builder()
                .dynamoDbClient(dynamoDbClient)
                .build();


    }

    public boolean tryCreateTable(final String tableName,
                                  final ProvisionedThroughput provisionedThroughput) {
        boolean createdTable = false;

        this.table = dynamoDbEnhancedClient.table(tableName, TableSchema.fromBean(DynamoDbSourcePartitionItem.class));

        try {
            // Probably need to configure strongly consistent reads for this, but also not really because of conditional checks for put and reread on conditional check failure
            table.createTable(builder -> builder
                    .provisionedThroughput(provisionedThroughput));
            createdTable = true;
        } catch (final ResourceInUseException e) {
            LOG.info("The table creation for {} was already triggered by another instance of data prepper", tableName);
        }

        try(final DynamoDbWaiter dynamoDbWaiter = DynamoDbWaiter.create()) {
            final ResponseOrException<DescribeTableResponse> response = dynamoDbWaiter
                    .waitUntilTableExists(builder -> builder.tableName(tableName).build())
                    .matched();

            final DescribeTableResponse describeTableResponse = response.response().orElseThrow(
                    () -> new RuntimeException(String.format("Table %s was not created successfully", tableName))
            );

            LOG.info("DynamoDB table {} was created successfully for source coordination", describeTableResponse.table().tableName());
        }

        return createdTable;
    }

    public boolean tryCreatePartitionItem(final DynamoDbSourcePartitionItem dynamoDbSourcePartitionItem) {
        try {
            table.putItem(PutItemEnhancedRequest.builder(DynamoDbSourcePartitionItem.class)
                    .item(dynamoDbSourcePartitionItem)
                    .conditionExpression(Expression.builder()
                            .expression("attribute_not_exists")
                            .build())
                    .build());
            return true;
        } catch (final ConditionalCheckFailedException e) {
            return false;
        } catch (final ProvisionedThroughputExceededException e) {
            // configure retries and exponential backoff
        }
        return false;
    }

    public boolean updatePartitionItem(final DynamoDbSourcePartitionItem dynamoDbSourcePartitionItem) {
        try {
            table.putItem(PutItemEnhancedRequest.builder(DynamoDbSourcePartitionItem.class)
                    .item(dynamoDbSourcePartitionItem)
                    .conditionExpression(Expression.builder()
                            .expression("attribute_exists")
                            .build())
                    .build());
            return true;
        } catch (final ConditionalCheckFailedException e) {
            LOG.error("The partitionKey {} does not exist");
            return false;
        } catch (final ProvisionedThroughputExceededException e) {
            // configure retries and exponential backoff
        }
        return false;
    }

    public Optional<DynamoDbSourcePartitionItem> getSourcePartitionItem(final String sourcePartitionKey) {
        try {
            final Key key = Key.builder()
                    .partitionValue(sourcePartitionKey)
                    .build();

            final DynamoDbSourcePartitionItem result = table.getItem(key);
            return Optional.of(result);
        } catch (final ResourceNotFoundException e) {
            return Optional.empty();
        }
    }

    private AwsCredentialsProvider getAwsCredentials(final Region region, final String stsRoleArn) {

        AwsCredentialsProvider awsCredentialsProvider;
        if (stsRoleArn != null && !stsRoleArn.isEmpty()) {
            try {
                Arn.fromString(stsRoleArn);
            } catch (final Exception e) {
                throw new IllegalArgumentException("Invalid ARN format for dynamodb sts_role_arn");
            }

            final StsClient stsClient = StsClient.builder()
                    .region(region)
                    .build();

            AssumeRoleRequest.Builder assumeRoleRequestBuilder = AssumeRoleRequest.builder()
                    .roleSessionName("Dynamo-Source-Coordination-" + UUID.randomUUID())
                    .roleArn(stsRoleArn);

            awsCredentialsProvider = StsAssumeRoleCredentialsProvider.builder()
                    .stsClient(stsClient)
                    .refreshRequest(assumeRoleRequestBuilder.build())
                    .build();

        } else {
            // use default credential provider
            awsCredentialsProvider = DefaultCredentialsProvider.create();
        }

        return awsCredentialsProvider;
    }
}
