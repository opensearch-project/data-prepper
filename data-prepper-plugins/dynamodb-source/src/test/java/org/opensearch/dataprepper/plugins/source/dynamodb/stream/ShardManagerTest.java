/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.dynamodb.model.SequenceNumberRange;
import software.amazon.awssdk.services.dynamodb.model.Shard;
import software.amazon.awssdk.services.dynamodb.model.ShardIteratorType;
import software.amazon.awssdk.services.dynamodb.model.StreamDescription;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class ShardManagerTest {


    @Mock
    private DynamoDbStreamsClient dynamoDbStreamsClient;

    @Mock
    private ShardManager shardManager;


    private final String tableName = UUID.randomUUID().toString();
    private final String tableArn = "arn:aws:dynamodb:us-west-2:123456789012:table/" + tableName;

    private final String streamArn = tableArn + "/stream/2023-09-14T05:46:45.367";

    private final String shardId = "shardId-" + UUID.randomUUID();
    private final String shardIterator = UUID.randomUUID().toString();
    private final String sequenceNumber = UUID.randomUUID().toString();


    private Shard buildShard(String shardId, String parentShardId, boolean isOpen) {
        String endingSequenceNumber = isOpen ? null : UUID.randomUUID().toString();
        return Shard.builder()
                .shardId(shardId)
                .parentShardId(parentShardId)
                .sequenceNumberRange(SequenceNumberRange.builder()
                        .endingSequenceNumber(endingSequenceNumber)
                        .startingSequenceNumber(UUID.randomUUID().toString())
                        .build())
                .build();

    }


    @BeforeEach
    void setup() {
        // Initialize some shards
        List<Shard> shardList = new ArrayList<>();
        shardList.add(buildShard("Shard-006", "Shard-004", true));
        shardList.add(buildShard("Shard-005", "Shard-003", true));
        shardList.add(buildShard("Shard-004", "Shard-002", false));
        shardList.add(buildShard("Shard-003", "Shard-001", false));

        StreamDescription description = StreamDescription.builder()
                .shards(shardList)
                .lastEvaluatedShardId(null)
                .build();
        DescribeStreamResponse response = DescribeStreamResponse.builder()
                .streamDescription(description)
                .build();

        lenient().when(dynamoDbStreamsClient.describeStream(any(DescribeStreamRequest.class))).thenReturn(response);
        shardManager = new ShardManager(dynamoDbStreamsClient);

    }

    @Test
    void test_getChildShardIds_should_return_child_shards() {
        List<String> childShards = shardManager.getChildShardIds(streamArn, "Shard-003");
        assertThat(childShards, notNullValue());
        assertThat(childShards.size(), equalTo(1));
        assertThat(childShards.get(0), equalTo("Shard-005"));

    }

    @Test
    void test_getActiveShards_should_return_open_shards() {
        List<String> activeShards = shardManager.getActiveShards(streamArn);
        assertThat(activeShards, notNullValue());
        assertThat(activeShards.size(), equalTo(2));
        assertThat(activeShards.contains("Shard-006"), equalTo(true));
        assertThat(activeShards.contains("Shard-005"), equalTo(true));
    }

    @Test
    void test_getShardIterator_with_sequenceNumber_should_return_shardIterator() {
        final ArgumentCaptor<GetShardIteratorRequest> getShardIterRequestArgumentCaptor = ArgumentCaptor.forClass(GetShardIteratorRequest.class);
        GetShardIteratorResponse response = GetShardIteratorResponse.builder()
                .shardIterator(shardIterator)
                .build();

        when(dynamoDbStreamsClient.getShardIterator(getShardIterRequestArgumentCaptor.capture())).thenReturn(response);
        String shardIterator1 = shardManager.getShardIterator(streamArn, shardId, sequenceNumber);
        assertThat(getShardIterRequestArgumentCaptor.getValue().shardId(), equalTo(shardId));
        assertThat(getShardIterRequestArgumentCaptor.getValue().shardIteratorType(), equalTo(ShardIteratorType.AFTER_SEQUENCE_NUMBER));
        assertThat(getShardIterRequestArgumentCaptor.getValue().sequenceNumber(), equalTo(sequenceNumber));
        assertThat(shardIterator1, equalTo(shardIterator));


    }

    @Test
    void test_getShardIterator_without_sequenceNumber_should_return_shardIterator() {
        final ArgumentCaptor<GetShardIteratorRequest> getShardIterRequestArgumentCaptor = ArgumentCaptor.forClass(GetShardIteratorRequest.class);
        GetShardIteratorResponse response = GetShardIteratorResponse.builder()
                .shardIterator(shardIterator)
                .build();

        when(dynamoDbStreamsClient.getShardIterator(getShardIterRequestArgumentCaptor.capture())).thenReturn(response);
        String shardIterator1 = shardManager.getShardIterator(streamArn, shardId, null);
        assertThat(getShardIterRequestArgumentCaptor.getValue().shardId(), equalTo(shardId));
        assertThat(getShardIterRequestArgumentCaptor.getValue().shardIteratorType(), equalTo(ShardIteratorType.TRIM_HORIZON));
        assertThat(shardIterator1, equalTo(shardIterator));
    }

    @Test
    void test_getRootShardIds_should_return_root_shards() {
        List<String> activeShards = shardManager.getRootShardIds(streamArn);
        assertThat(activeShards, notNullValue());
        assertThat(activeShards.size(), equalTo(2));
        assertThat(activeShards.contains("Shard-003"), equalTo(true));
        assertThat(activeShards.contains("Shard-004"), equalTo(true));
    }
}