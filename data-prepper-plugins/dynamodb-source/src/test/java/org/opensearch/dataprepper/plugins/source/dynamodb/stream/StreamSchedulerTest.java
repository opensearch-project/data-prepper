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
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.state.StreamProgressState;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class StreamSchedulerTest {

    @Mock
    private EnhancedSourceCoordinator coordinator;

    @Mock
    private DynamoDbStreamsClient dynamoDbStreamsClient;


    @Mock
    private ShardManager shardManager;

    private StreamScheduler scheduler;


    private StreamPartition streamPartition;


    @Mock
    private ShardConsumerFactory consumerFactory;


    private final String tableName = UUID.randomUUID().toString();
    private final String tableArn = "arn:aws:dynamodb:us-west-2:123456789012:table/" + tableName;

    private final String exportArn = tableArn + "/export/01693291918297-bfeccbea";
    private final String streamArn = tableArn + "/stream/2023-09-14T05:46:45.367";

    private final String shardId = "shardId-" + UUID.randomUUID();

    private final long exportTimeMills = 1695021857760L;
    private final Instant exportTime = Instant.ofEpochMilli(exportTimeMills);


    @BeforeEach
    void setup() {

        StreamProgressState state = new StreamProgressState();
        state.setWaitForExport(false);
        state.setStartTime(0);

        streamPartition = new StreamPartition(streamArn, shardId, Optional.of(state));
        // Mock Coordinator methods
        lenient().when(coordinator.createPartition(any(EnhancedSourcePartition.class))).thenReturn(true);
        lenient().doNothing().when(coordinator).completePartition(any(EnhancedSourcePartition.class));
        lenient().doNothing().when(coordinator).saveProgressStateForPartition(any(EnhancedSourcePartition.class));
        lenient().doNothing().when(coordinator).giveUpPartition(any(EnhancedSourcePartition.class));

        lenient().when(consumerFactory.createConsumer(any(StreamPartition.class))).thenReturn(() -> System.out.println("Hello"));
        lenient().when(shardManager.getChildShardIds(anyString(), anyString())).thenReturn(List.of(shardId));

    }


    @Test
    public void test_normal_run() throws InterruptedException {
        given(coordinator.acquireAvailablePartition(StreamPartition.PARTITION_TYPE)).willReturn(Optional.of(streamPartition)).willReturn(Optional.empty());

        scheduler = new StreamScheduler(coordinator, consumerFactory, shardManager);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(scheduler);

        // Need to run a while
        Thread.sleep(500);
        // Should acquire the stream partition
        verify(coordinator).acquireAvailablePartition(StreamPartition.PARTITION_TYPE);
        // Should start a new consumer
        verify(consumerFactory).createConsumer(any(StreamPartition.class));
        // Should create stream partition for child shards.
        verify(coordinator).createPartition(any(StreamPartition.class));
        // Should mask the stream partition as completed.
        verify(coordinator).completePartition(any(StreamPartition.class));

        executor.shutdownNow();

    }
}