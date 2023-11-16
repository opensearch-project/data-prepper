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
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.DynamoDBSourceConfig;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.state.StreamProgressState;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.dynamodb.stream.StreamScheduler.ACTIVE_CHANGE_EVENT_CONSUMERS;
import static org.opensearch.dataprepper.plugins.source.dynamodb.stream.StreamScheduler.SHARDS_IN_PROCESSING;

@ExtendWith(MockitoExtension.class)
class StreamSchedulerTest {

    @Mock
    private EnhancedSourceCoordinator coordinator;

    @Mock
    private DynamoDbStreamsClient dynamoDbStreamsClient;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private DynamoDBSourceConfig dynamoDBSourceConfig;

    private StreamScheduler scheduler;


    private StreamPartition streamPartition;


    @Mock
    private ShardConsumerFactory consumerFactory;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private AtomicLong activeShardConsumers;

    @Mock
    private AtomicLong activeShardsInProcessing;


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
        lenient().doNothing().when(coordinator).saveProgressStateForPartition(any(EnhancedSourcePartition.class), eq(null));
        lenient().doNothing().when(coordinator).giveUpPartition(any(EnhancedSourcePartition.class));

        when(pluginMetrics.gauge(eq(ACTIVE_CHANGE_EVENT_CONSUMERS), any(AtomicLong.class))).thenReturn(activeShardConsumers);
        when(pluginMetrics.gauge(eq(SHARDS_IN_PROCESSING), any(AtomicLong.class))).thenReturn(activeShardsInProcessing);

    }


    @Test
    public void test_normal_run() throws InterruptedException {
        when(consumerFactory.createConsumer(any(StreamPartition.class), eq(null), any(Duration.class))).thenReturn(() -> System.out.println("Hello"));
        when(coordinator.acquireAvailablePartition(StreamPartition.PARTITION_TYPE)).thenReturn(Optional.of(streamPartition)).thenReturn(Optional.empty());

        scheduler = new StreamScheduler(coordinator, consumerFactory, pluginMetrics, acknowledgementSetManager, dynamoDBSourceConfig);

        ExecutorService executorService = Executors.newSingleThreadExecutor();

        final Future<?> future = executorService.submit(() -> scheduler.run());
        Thread.sleep(2000);
        executorService.shutdown();
        future.cancel(true);

        // Should mask the stream partition as completed.
        verify(coordinator).completePartition(any(StreamPartition.class));

        verify(activeShardsInProcessing).incrementAndGet();
        verify(activeShardsInProcessing).decrementAndGet();

        executorService.shutdownNow();
    }

    @Test
    public void test_normal_run_with_acknowledgments() throws InterruptedException {
        given(coordinator.acquireAvailablePartition(StreamPartition.PARTITION_TYPE)).willReturn(Optional.of(streamPartition)).willReturn(Optional.empty());
        given(dynamoDBSourceConfig.isAcknowledgmentsEnabled()).willReturn(true);

        final Duration shardAcknowledgmentTimeout = Duration.ofSeconds(30);
        given(dynamoDBSourceConfig.getShardAcknowledgmentTimeout()).willReturn(shardAcknowledgmentTimeout);

        final AcknowledgementSet acknowledgementSet = mock(AcknowledgementSet.class);
        doAnswer(invocation -> {
            Consumer<Boolean> consumer = invocation.getArgument(0);
            consumer.accept(true);
            return acknowledgementSet;
        }).when(acknowledgementSetManager).create(any(Consumer.class), eq(shardAcknowledgmentTimeout));

        when(consumerFactory.createConsumer(any(StreamPartition.class), eq(acknowledgementSet), eq(shardAcknowledgmentTimeout))).thenReturn(() -> System.out.println("Hello"));

        scheduler = new StreamScheduler(coordinator, consumerFactory, pluginMetrics, acknowledgementSetManager, dynamoDBSourceConfig);

        ExecutorService executorService = Executors.newSingleThreadExecutor();

        final Future<?> future = executorService.submit(() -> scheduler.run());
        Thread.sleep(3000);
        executorService.shutdown();
        future.cancel(true);
        assertThat(executorService.awaitTermination(1000, TimeUnit.MILLISECONDS), equalTo(true));

        // Should mask the stream partition as completed.
        verify(coordinator).completePartition(any(StreamPartition.class));

        verify(activeShardsInProcessing).incrementAndGet();
        verify(activeShardsInProcessing).decrementAndGet();

        executorService.shutdownNow();
    }

    @Test
    void run_catches_exception_and_retries_when_exception_is_thrown_during_processing() throws InterruptedException {
        given(coordinator.acquireAvailablePartition(StreamPartition.PARTITION_TYPE)).willThrow(RuntimeException.class);
        scheduler = new StreamScheduler(coordinator, consumerFactory, pluginMetrics, acknowledgementSetManager, dynamoDBSourceConfig);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        final Future<?> future = executorService.submit(() -> scheduler.run());
        Thread.sleep(100);
        assertThat(future.isDone(), equalTo(false));
        executorService.shutdown();
        future.cancel(true);
        assertThat(executorService.awaitTermination(1000, TimeUnit.MILLISECONDS), equalTo(true));
    }
}