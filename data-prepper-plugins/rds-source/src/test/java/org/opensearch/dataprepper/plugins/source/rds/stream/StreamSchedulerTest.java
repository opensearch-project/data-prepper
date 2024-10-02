/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;
import org.opensearch.dataprepper.model.plugin.PluginConfigObserver;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.StreamPartition;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StreamSchedulerTest {

    @Mock
    private EnhancedSourceCoordinator sourceCoordinator;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private RdsSourceConfig sourceConfig;

    @Mock
    private BinlogClientFactory binlogClientFactory;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private PluginConfigObservable pluginConfigObservable;

    @Mock
    private StreamWorkerTaskRefresher streamWorkerTaskRefresher;

    private String s3Prefix;
    private StreamScheduler objectUnderTest;

    @BeforeEach
    void setUp() {
        s3Prefix = UUID.randomUUID().toString();
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

        verifyNoInteractions(binlogClientFactory, pluginConfigObservable);
    }

    @Test
    void test_given_stream_partition_then_start_stream() throws InterruptedException {
        final StreamPartition streamPartition = mock(StreamPartition.class);
        when(sourceCoordinator.acquireAvailablePartition(StreamPartition.PARTITION_TYPE)).thenReturn(Optional.of(streamPartition));

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> {
            try (MockedStatic<StreamWorkerTaskRefresher> streamWorkerTaskRefresherMockedStatic = mockStatic(StreamWorkerTaskRefresher.class)) {
                streamWorkerTaskRefresherMockedStatic.when(() -> StreamWorkerTaskRefresher.create(eq(sourceCoordinator), eq(streamPartition), any(StreamCheckpointer.class),
                                eq(s3Prefix), eq(binlogClientFactory), eq(buffer), eq(acknowledgementSetManager), any(ExecutorService.class), eq(pluginMetrics)))
                        .thenReturn(streamWorkerTaskRefresher);
                objectUnderTest.run();
            }

        });
        await().atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> verify(sourceCoordinator).acquireAvailablePartition(StreamPartition.PARTITION_TYPE));
        Thread.sleep(100);
        executorService.shutdownNow();

        verify(streamWorkerTaskRefresher).initialize(sourceConfig);
        verify(pluginConfigObservable).addPluginConfigObserver(any(PluginConfigObserver.class));
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
        return new StreamScheduler(
                sourceCoordinator, sourceConfig, s3Prefix, binlogClientFactory, buffer, pluginMetrics, acknowledgementSetManager, pluginConfigObservable);
    }
}