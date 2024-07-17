/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.stream;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.StreamPartition;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StreamSchedulerTest {

    @Mock
    private EnhancedSourceCoordinator sourceCoordinator;
    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private RdsSourceConfig sourceConfig;

    @Mock
    private BinaryLogClient binaryLogClient;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private Buffer<Record<Event>> buffer;

    private StreamScheduler objectUnderTest;

    @BeforeEach
    void setUp() {
        objectUnderTest = createObjectUnderTest();
    }

    @Test
    void test_given_no_stream_partition_then_no_stream_actions() throws InterruptedException {
        when(sourceCoordinator.acquireAvailablePartition(StreamPartition.PARTITION_TYPE)).thenReturn(Optional.empty());

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(objectUnderTest);
        await().atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> verify(sourceCoordinator).acquireAvailablePartition(StreamPartition.PARTITION_TYPE));
        Thread.sleep(100);
        executorService.shutdownNow();

        verify(binaryLogClient).registerEventListener(any(BinlogEventListener.class));
        verifyNoMoreInteractions(binaryLogClient);
    }

    @Test
    void test_given_stream_partition_then_start_stream() throws InterruptedException {
        final StreamPartition streamPartition = mock(StreamPartition.class);
        when(sourceCoordinator.acquireAvailablePartition(StreamPartition.PARTITION_TYPE)).thenReturn(Optional.of(streamPartition));

        StreamWorker streamWorker = mock(StreamWorker.class);
        doNothing().when(streamWorker).processStream(streamPartition);
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> {
            try (MockedStatic<StreamWorker> streamWorkerMockedStatic = mockStatic(StreamWorker.class)) {
                streamWorkerMockedStatic.when(() -> StreamWorker.create(sourceCoordinator, binaryLogClient, pluginMetrics))
                        .thenReturn(streamWorker);
                objectUnderTest.run();
            }

        });
        await().atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> verify(sourceCoordinator).acquireAvailablePartition(StreamPartition.PARTITION_TYPE));
        Thread.sleep(100);
        executorService.shutdownNow();

        verify(streamWorker).processStream(streamPartition);
    }

    @Test
    void test_shutdown() {
        lenient().when(sourceCoordinator.acquireAvailablePartition(StreamPartition.PARTITION_TYPE)).thenReturn(Optional.empty());

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(objectUnderTest);
        objectUnderTest.shutdown();
        verifyNoMoreInteractions(sourceCoordinator);
        executorService.shutdownNow();
    }

    private StreamScheduler createObjectUnderTest() {
        return new StreamScheduler(sourceCoordinator, sourceConfig, binaryLogClient, buffer, pluginMetrics);
    }
}