/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kinesis.source.ingester;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DeleteStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;
import software.amazon.awssdk.services.kinesis.waiters.KinesisWaiter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class KinesisIngester {
    private static final int SHARDS = 4;
    private static final String LEASE_TABLE_PARTITION_KEY = "leaseKey";
    private static final String PARTITION_KEY = "record_key";
    private static final String ATTRIBUTE_VALUE = "record";

    private final KinesisClient kinesisClient;
    private final DynamoDbClient ddbClient;
    private final String streamName;
    private final String tableName;

    private final ObjectMapper objectMapper;

    public KinesisIngester(final KinesisClient kinesisClient, final String streamName, final DynamoDbClient ddbClient, final String tableName) {
        this.kinesisClient = kinesisClient;
        this.ddbClient = ddbClient;
        this.tableName = tableName;
        this.streamName = streamName;
        this.objectMapper = new ObjectMapper();
    }

    public void createStream() {
        CreateStreamRequest createStreamRequest = CreateStreamRequest.builder()
                .streamName(streamName)
                .shardCount(SHARDS)
                .build();
        try {
            kinesisClient.createStream(createStreamRequest);
            KinesisWaiter kinesisWaiter = kinesisClient.waiter();

            DescribeStreamRequest describeStreamRequest = DescribeStreamRequest.builder()
                    .streamName(streamName)
                    .build();

            kinesisWaiter.waitUntilStreamExists(describeStreamRequest);
            waitForStreamToBeActive();
        } catch (KinesisException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void createLeaseTable() {
        final CreateTableRequest createTableRequest = CreateTableRequest.builder()
                .tableName(tableName)
                .keySchema(getKeySchema())
                .attributeDefinitions(getAttributeDefinitions())
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .build();

        try {
            CreateTableResponse response = ddbClient.createTable(createTableRequest);
            DescribeTableRequest tableRequest = DescribeTableRequest.builder()
                    .tableName(tableName)
                    .build();
            // Wait until the Amazon DynamoDB table is created.
            DynamoDbWaiter dbWaiter = ddbClient.waiter();
            dbWaiter.waitUntilTableExists(tableRequest);
            waitForTableActive();

        } catch (final DynamoDbException e) {
            throw new RuntimeException(e);
        }
    }

    public void deleteLeaseTable() {

        DeleteTableRequest request = DeleteTableRequest.builder()
                .tableName(tableName)
                .build();
        try {
            ddbClient.deleteTable(request);
        } catch (final DynamoDbException e) {
            throw new RuntimeException(e);
        }
    }

    public void deleteStream() {
        DeleteStreamRequest deleteStreamRequest = DeleteStreamRequest.builder()
                .streamName(streamName)
                .enforceConsumerDeletion(true)
                .build();
        try {
            kinesisClient.deleteStream(deleteStreamRequest);
        } catch (KinesisException ex) {
            throw new RuntimeException(ex);
        }
    }

    private void waitForTableActive() {
        final DescribeTableRequest describeTableRequest = DescribeTableRequest.builder()
                .tableName(tableName)
                .build();

        await().atMost(5, TimeUnit.MINUTES).pollInterval(10, TimeUnit.SECONDS).untilAsserted(
                () -> {
                    final DescribeTableResponse describeTableResponse = ddbClient.describeTable(describeTableRequest);
                    final TableStatus tableStatus = describeTableResponse.table().tableStatus();

                    if (!TableStatus.CREATING.equals(tableStatus) && !TableStatus.ACTIVE.equals(tableStatus)) {
                        throw new RuntimeException("Table is not creating or active.");
                    }

                    assertThat(tableStatus, equalTo(TableStatus.ACTIVE));
                }
        );
    }

    private Collection<KeySchemaElement> getKeySchema() {
        List<KeySchemaElement> keySchema = new ArrayList<>();
        keySchema.add(KeySchemaElement.builder()
                .attributeName(LEASE_TABLE_PARTITION_KEY)
                .keyType(KeyType.HASH)
                .build());

        return keySchema;
    }

    private Collection<AttributeDefinition> getAttributeDefinitions() {
        List<AttributeDefinition> definitions = new ArrayList<>();
        definitions.add(AttributeDefinition.builder()
                .attributeName(LEASE_TABLE_PARTITION_KEY)
                .attributeType(ScalarAttributeType.S)
                .build());

        return definitions;
    }

    private void waitForStreamToBeActive() {
        final DescribeStreamRequest describeStreamRequest = DescribeStreamRequest.builder()
                .streamName(streamName)
                .build();

        await().atMost(5, TimeUnit.MINUTES).pollInterval(10, TimeUnit.SECONDS).untilAsserted(
                () -> {
                    final DescribeStreamResponse describeStreamResponse =
                            kinesisClient.describeStream(describeStreamRequest);
                    final StreamStatus streamStatus = describeStreamResponse
                            .streamDescription().streamStatus();

                    if (!StreamStatus.CREATING.equals(streamStatus) && !StreamStatus.ACTIVE.equals(streamStatus)) {
                        throw new RuntimeException("Stream is not creating or active.");
                    }

                    assertThat(streamStatus, equalTo(StreamStatus.ACTIVE));
                }
        );
    }

    public void ingest(List<String> logs) {
        try {
            for (String log : logs) {
                putRecord(UUID.randomUUID().toString(), log);
            }
        } catch (final Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private void putRecord(final String key, final String value) throws JsonProcessingException {
        Map<String, String> item = new HashMap<>();
        item.put(PARTITION_KEY, key);
        item.put(ATTRIBUTE_VALUE, value);
        PutRecordRequest putRecordRequest = PutRecordRequest.builder()
                .streamName(streamName)
                .partitionKey(key)
                .data(SdkBytes.fromByteArray(objectMapper.writeValueAsBytes(item)))
                .build();

        try {
            kinesisClient.putRecord(putRecordRequest);
        } catch (final Exception ex) {
            //log.error("Put Record failed, item key: {}, value: {}", key, value);
            throw new RuntimeException(ex);
        }
    }
}