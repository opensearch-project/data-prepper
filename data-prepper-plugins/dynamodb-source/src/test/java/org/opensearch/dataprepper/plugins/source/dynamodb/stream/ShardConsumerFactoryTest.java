/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.state.StreamProgressState;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.TableMetadata;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;

@ExtendWith(MockitoExtension.class)
class ShardConsumerFactoryTest {

    @Mock
    private EnhancedSourceCoordinator coordinator;
    @Mock
    private DynamoDbStreamsClient dynamoDbStreamsClient;
    @Mock
    private ShardManager shardManager;
    @Mock
    private PluginMetrics pluginMetrics;


    private StreamPartition streamPartition;


    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private GlobalState tableInfoGlobalState;


    private final String tableName = UUID.randomUUID().toString();
    private final String tableArn = "arn:aws:dynamodb:us-west-2:123456789012:table/" + tableName;

    private final String exportArn = tableArn + "/export/01693291918297-bfeccbea";
    private final String streamArn = tableArn + "/stream/2023-09-14T05:46:45.367";

    private final String shardId = "shardId-" + UUID.randomUUID();
    private final String shardIterator = UUID.randomUUID().toString();


    @BeforeEach
    void setup() {

        StreamProgressState state = new StreamProgressState();
        state.setWaitForExport(false);
        state.setStartTime(0);
        streamPartition = new StreamPartition(streamArn, shardId, Optional.of(state));

        lenient().when(shardManager.getShardIterator(eq(streamArn), eq(shardId), eq(null))).thenReturn(shardIterator);

        // Mock Global Table Info
        lenient().when(coordinator.getPartition(tableArn)).thenReturn(Optional.of(tableInfoGlobalState));
        TableMetadata metadata = TableMetadata.builder()
                .exportRequired(true)
                .streamRequired(true)
                .partitionKeyAttributeName("PK")
                .sortKeyAttributeName("SK")
                .build();
        lenient().when(tableInfoGlobalState.getProgressState()).thenReturn(Optional.of(metadata.toMap()));

    }

    @Test
    public void test_create_shardConsumer_correctly() {

        ShardConsumerFactory consumerFactory = new ShardConsumerFactory(coordinator, dynamoDbStreamsClient, pluginMetrics, shardManager, buffer);

        Runnable consumer = consumerFactory.createConsumer(streamPartition);

        assertThat(consumer, notNullValue());
    }

}