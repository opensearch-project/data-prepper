/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.stream;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.dynamodb.configuration.StreamConfig;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.state.StreamProgressState;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.TableMetadata;
import org.opensearch.dataprepper.plugins.source.dynamodb.utils.DynamoDBSourceAggregateMetrics;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.dynamodb.model.InternalServerErrorException;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ShardConsumerFactoryTest {

    @Mock
    private EnhancedSourceCoordinator coordinator;

    @Mock
    private DynamoDbStreamsClient dynamoDbStreamsClient;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private DynamoDBSourceAggregateMetrics dynamoDBSourceAggregateMetrics;

    @Mock
    private Counter streamApiInvocations;

    private StreamPartition streamPartition;

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private GlobalState tableInfoGlobalState;

    @Mock
    private StreamConfig streamConfig;


    private final String tableName = UUID.randomUUID().toString();
    private final String tableArn = "arn:aws:dynamodb:us-west-2:123456789012:table/" + tableName;

    private final String exportArn = tableArn + "/export/01693291918297-bfeccbea";
    private final String streamArn = tableArn + "/stream/2023-09-14T05:46:45.367";

    private final String shardId = "shardId-" + UUID.randomUUID();
    private final String shardIterator = UUID.randomUUID().toString();

    private final String partitionKeyAttrName = "PK";
    private final String sortKeyAttrName = "SK";


    @BeforeEach
    void setup() {
        
        GetShardIteratorResponse response = GetShardIteratorResponse.builder()
                .shardIterator(shardIterator)
                .build();

        lenient().when(dynamoDbStreamsClient.getShardIterator(any(GetShardIteratorRequest.class))).thenReturn(response);

        // Mock Global Table Info
        lenient().when(coordinator.getPartition(tableArn)).thenReturn(Optional.of(tableInfoGlobalState));
        TableMetadata metadata = TableMetadata.builder()
                .exportRequired(true)
                .streamRequired(true)
                .partitionKeyAttributeName(partitionKeyAttrName)
                .sortKeyAttributeName(sortKeyAttrName)
                .build();
        lenient().when(tableInfoGlobalState.getProgressState()).thenReturn(Optional.of(metadata.toMap()));

        when(dynamoDBSourceAggregateMetrics.getStreamApiInvocations()).thenReturn(streamApiInvocations);
    }

    @Test
    public void test_create_shardConsumer_correctly() {

        StreamProgressState state = new StreamProgressState();
        state.setWaitForExport(false);
        state.setStartTime(Instant.now().toEpochMilli());
        streamPartition = new StreamPartition(streamArn, shardId, Optional.of(state));

        ShardConsumerFactory consumerFactory = new ShardConsumerFactory(coordinator, dynamoDbStreamsClient, pluginMetrics, dynamoDBSourceAggregateMetrics, buffer, streamConfig);
        Runnable consumer = consumerFactory.createConsumer(streamPartition, null, null);
        assertThat(consumer, notNullValue());
        verify(dynamoDbStreamsClient).getShardIterator(any(GetShardIteratorRequest.class));

        verify(streamApiInvocations).increment();
    }

    @Test
    public void test_create_shardConsumer_for_closedShards() {
        // For ending sequence number != null
        StreamProgressState state = new StreamProgressState();
        state.setWaitForExport(false);
        state.setStartTime(Instant.now().toEpochMilli());
        state.setEndingSequenceNumber(UUID.randomUUID().toString());
        streamPartition = new StreamPartition(streamArn, shardId, Optional.of(state));

        ShardConsumerFactory consumerFactory = new ShardConsumerFactory(coordinator, dynamoDbStreamsClient, pluginMetrics, dynamoDBSourceAggregateMetrics, buffer, streamConfig);
        Runnable consumer = consumerFactory.createConsumer(streamPartition, null, null);
        assertThat(consumer, notNullValue());
        // Should get iterators twice
        verify(dynamoDbStreamsClient, times(2)).getShardIterator(any(GetShardIteratorRequest.class));

        verify(streamApiInvocations, times(2)).increment();

    }

    @Test
    void stream5xxErrors_is_incremented_when_get_shard_iterator_throws_internal_exception() {
        StreamProgressState state = new StreamProgressState();
        state.setWaitForExport(false);
        state.setStartTime(Instant.now().toEpochMilli());
        streamPartition = new StreamPartition(streamArn, shardId, Optional.of(state));

        when(dynamoDbStreamsClient.getShardIterator(any(GetShardIteratorRequest.class))).thenThrow(InternalServerErrorException.class);
        final Counter stream5xxErrors = mock(Counter.class);
        when(dynamoDBSourceAggregateMetrics.getStream5xxErrors()).thenReturn(stream5xxErrors);

        ShardConsumerFactory consumerFactory = new ShardConsumerFactory(coordinator, dynamoDbStreamsClient, pluginMetrics, dynamoDBSourceAggregateMetrics, buffer, streamConfig);
        Runnable consumer = consumerFactory.createConsumer(streamPartition, null, null);
        assertThat(consumer, nullValue());
        verify(stream5xxErrors).increment();
        verify(streamApiInvocations).increment();
    }

    @Test
    void stream4xxErrors_is_incremented_when_get_shard_iterator_throws_dynamodb_exception() {
        StreamProgressState state = new StreamProgressState();
        state.setWaitForExport(false);
        state.setStartTime(Instant.now().toEpochMilli());
        streamPartition = new StreamPartition(streamArn, shardId, Optional.of(state));

        when(dynamoDbStreamsClient.getShardIterator(any(GetShardIteratorRequest.class))).thenThrow(DynamoDbException.class);
        final Counter stream4xxErrors = mock(Counter.class);
        when(dynamoDBSourceAggregateMetrics.getStream4xxErrors()).thenReturn(stream4xxErrors);

        ShardConsumerFactory consumerFactory = new ShardConsumerFactory(coordinator, dynamoDbStreamsClient, pluginMetrics, dynamoDBSourceAggregateMetrics, buffer, streamConfig);
        Runnable consumer = consumerFactory.createConsumer(streamPartition, null, null);
        assertThat(consumer, nullValue());
        verify(stream4xxErrors).increment();
        verify(streamApiInvocations).increment();
    }

}