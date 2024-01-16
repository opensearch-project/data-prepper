/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.leader;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.dynamodb.utils.DynamoDBSourceAggregateMetrics;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.InternalServerErrorException;
import software.amazon.awssdk.services.dynamodb.model.SequenceNumberRange;
import software.amazon.awssdk.services.dynamodb.model.Shard;
import software.amazon.awssdk.services.dynamodb.model.StreamDescription;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class ShardManagerTest {
    @Mock
    private DynamoDbStreamsClient dynamoDbStreamsClient;

    @Mock
    private DynamoDBSourceAggregateMetrics dynamoDBSourceAggregateMetrics;

    @Mock
    private Counter stream5xxErrors;

    @Mock
    private Counter stream4xxErrors;

    @Mock
    private Counter streamApiInvocations;

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
        shardList.add(buildShard("shardId-006", "shardId-003", true));
        shardList.add(buildShard("shardId-005", "shardId-003", true));
        shardList.add(buildShard("shardId-004", "shardId-002", false));
        shardList.add(buildShard("shardId-003", "shardId-001", false));
        shardList.add(buildShard("shardId-002", null, false));
        shardList.add(buildShard("shardId-001", null, false));

        StreamDescription description = StreamDescription.builder()
                .shards(shardList)
                .lastEvaluatedShardId(null)
                .build();
        DescribeStreamResponse response = DescribeStreamResponse.builder()
                .streamDescription(description)
                .build();

        lenient().when(dynamoDbStreamsClient.describeStream(any(DescribeStreamRequest.class))).thenReturn(response);
        shardManager = new ShardManager(dynamoDbStreamsClient, dynamoDBSourceAggregateMetrics);

        when(dynamoDBSourceAggregateMetrics.getStreamApiInvocations()).thenReturn(streamApiInvocations);
    }

    @Test
    void test_getChildShardIds_should_return_child_shards() {
        List<Shard> childShards = shardManager.runDiscovery(streamArn);
        assertThat(childShards, notNullValue());
        assertThat(childShards.size(), equalTo(6));

        List<String> childShardIds1 = shardManager.findChildShardIds(streamArn, "shardId-001");
        assertThat(childShardIds1, notNullValue());
        assertThat(childShardIds1.size(), equalTo(1));

        List<String> childShardIds2 = shardManager.findChildShardIds(streamArn, "shardId-003");
        assertThat(childShardIds2, notNullValue());
        assertThat(childShardIds2.size(), equalTo(2));

        List<String> childShardIds3 = shardManager.findChildShardIds(streamArn, "shardId-005");
        assertThat(childShardIds3, nullValue());

        verify(streamApiInvocations).increment();
    }

    @Test
    void stream5xxError_is_incremented_when_describe_stream_throws_internal_error() {
        when(dynamoDbStreamsClient.describeStream(any(DescribeStreamRequest.class))).thenThrow(InternalServerErrorException.class);
        when(dynamoDBSourceAggregateMetrics.getStream5xxErrors()).thenReturn(stream5xxErrors);

        final List<Shard> shards = shardManager.runDiscovery(streamArn);
        assertThat(shards, notNullValue());
        assertThat(shards.isEmpty(), equalTo(true));

        verify(stream5xxErrors).increment();
        verify(streamApiInvocations).increment();
    }

    @Test
    void stream4xxErrors_is_incremented_when_describe_stream_throws_DynamoDBException() {
        when(dynamoDbStreamsClient.describeStream(any(DescribeStreamRequest.class))).thenThrow(DynamoDbException.class);
        when(dynamoDBSourceAggregateMetrics.getStream4xxErrors()).thenReturn(stream4xxErrors);

        final List<Shard> shards = shardManager.runDiscovery(streamArn);
        assertThat(shards, notNullValue());
        assertThat(shards.isEmpty(), equalTo(true));

        verify(stream4xxErrors).increment();
        verify(streamApiInvocations).increment();
    }

}