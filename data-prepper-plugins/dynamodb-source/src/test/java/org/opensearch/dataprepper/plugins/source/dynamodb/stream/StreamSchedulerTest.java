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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.dynamodb.stream.StreamScheduler.ACTIVE_CHANGE_EVENT_CONSUMERS;

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

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private AtomicLong activeShardConsumers;


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

        when(pluginMetrics.gauge(eq(ACTIVE_CHANGE_EVENT_CONSUMERS), any(AtomicLong.class))).thenReturn(activeShardConsumers);

    }


    @Test
    public void test_normal_run() throws InterruptedException {
        given(coordinator.acquireAvailablePartition(StreamPartition.PARTITION_TYPE)).willReturn(Optional.of(streamPartition)).willReturn(Optional.empty());

        scheduler = new StreamScheduler(coordinator, consumerFactory, shardManager, pluginMetrics);

        ExecutorService executorService = Executors.newSingleThreadExecutor();

        final Future<?> future = executorService.submit(() -> scheduler.run());
        Thread.sleep(2000);
        executorService.shutdown();
        future.cancel(true);

        // Should acquire the stream partition
        verify(coordinator).acquireAvailablePartition(StreamPartition.PARTITION_TYPE);
        // Should start a new consumer
        verify(consumerFactory).createConsumer(any(StreamPartition.class));
        // Should create stream partition for child shards.
        verify(coordinator).createPartition(any(StreamPartition.class));
        // Should mask the stream partition as completed.
        verify(coordinator).completePartition(any(StreamPartition.class));

        executorService.shutdownNow();
    }

    @Test
    void run_catches_exception_and_retries_when_exception_is_thrown_during_processing() throws InterruptedException {
        given(coordinator.acquireAvailablePartition(StreamPartition.PARTITION_TYPE)).willThrow(RuntimeException.class);

        scheduler = new StreamScheduler(coordinator, consumerFactory, shardManager, pluginMetrics);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        final Future<?> future = executorService.submit(() -> scheduler.run());
        Thread.sleep(100);
        assertThat(future.isDone(), equalTo(false));
        executorService.shutdown();
        future.cancel(true);
        assertThat(executorService.awaitTermination(1000, TimeUnit.MILLISECONDS), equalTo(true));
    }
}