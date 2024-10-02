/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.stream;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.StreamPartition;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.rds.stream.StreamWorkerTaskRefresher.CREDENTIALS_CHANGED;
import static org.opensearch.dataprepper.plugins.source.rds.stream.StreamWorkerTaskRefresher.TASK_REFRESH_ERRORS;

@ExtendWith(MockitoExtension.class)
class StreamWorkerTaskRefresherTest {

    @Mock
    private EnhancedSourceCoordinator sourceCoordinator;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private StreamPartition streamPartition;

    @Mock
    private StreamCheckpointer streamCheckpointer;

    @Mock
    private BinlogClientFactory binlogClientFactory;

    @Mock
    private BinaryLogClient binlogClient;

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private ExecutorService executorService;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private RdsSourceConfig sourceConfig;

    @Mock
    private StreamWorker streamWorker;

    @Mock
    private Counter credentialsChangeCounter;

    @Mock
    private Counter taskRefreshErrorsCounter;

    @Mock
    private BinlogEventListener binlogEventListener;

    private StreamWorkerTaskRefresher streamWorkerTaskRefresher;

    @BeforeEach
    void setUp() {
        when(pluginMetrics.counter(CREDENTIALS_CHANGED)).thenReturn(credentialsChangeCounter);
        when(pluginMetrics.counter(TASK_REFRESH_ERRORS)).thenReturn(taskRefreshErrorsCounter);
        streamWorkerTaskRefresher = createObjectUnderTest();
    }

    @Test
    void test_initialize_then_process_stream() {
        when(binlogClientFactory.create()).thenReturn(binlogClient);
        try (MockedStatic<StreamWorker> streamWorkerMockedStatic = mockStatic(StreamWorker.class);
             MockedStatic<BinlogEventListener> binlogEventListenerMockedStatic = mockStatic(BinlogEventListener.class)) {
            streamWorkerMockedStatic.when(() -> StreamWorker.create(eq(sourceCoordinator), any(BinaryLogClient.class), eq(pluginMetrics)))
                    .thenReturn(streamWorker);
            binlogEventListenerMockedStatic.when(() -> BinlogEventListener.create(eq(buffer), any(RdsSourceConfig.class), any(String.class), eq(pluginMetrics), eq(binlogClient), eq(streamCheckpointer), eq(acknowledgementSetManager)))
                    .thenReturn(binlogEventListener);
            streamWorkerTaskRefresher.initialize(sourceConfig);
        }

        verify(binlogClientFactory).create();
        verify(binlogClient).registerEventListener(binlogEventListener);

        ArgumentCaptor<Runnable> runnableArgumentCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(executorService).submit(runnableArgumentCaptor.capture());

        Runnable capturedRunnable = runnableArgumentCaptor.getValue();
        capturedRunnable.run();
        verify(streamWorker).processStream(streamPartition);
    }

    @Test
    void test_update_when_credentials_changed_then_refresh_task() {
        final String username = UUID.randomUUID().toString();
        final String password = UUID.randomUUID().toString();
        when(sourceConfig.getAuthenticationConfig().getUsername()).thenReturn(username);
        when(sourceConfig.getAuthenticationConfig().getPassword()).thenReturn(password);

        RdsSourceConfig sourceConfig2 = mock(RdsSourceConfig.class, RETURNS_DEEP_STUBS);
        final String password2 = UUID.randomUUID().toString();
        when(sourceConfig2.getAuthenticationConfig().getUsername()).thenReturn(username);
        when(sourceConfig2.getAuthenticationConfig().getPassword()).thenReturn(password2);

        when(binlogClientFactory.create()).thenReturn(binlogClient).thenReturn(binlogClient);

        final ExecutorService newExecutorService = mock(ExecutorService.class);
        try (MockedStatic<StreamWorker> streamWorkerMockedStatic = mockStatic(StreamWorker.class);
             MockedStatic<Executors> executorsMockedStatic = mockStatic(Executors.class);
             MockedStatic<BinlogEventListener> binlogEventListenerMockedStatic = mockStatic(BinlogEventListener.class)) {
            streamWorkerMockedStatic.when(() -> StreamWorker.create(eq(sourceCoordinator), any(BinaryLogClient.class), eq(pluginMetrics)))
                    .thenReturn(streamWorker);
            executorsMockedStatic.when(Executors::newSingleThreadExecutor).thenReturn(newExecutorService);
            binlogEventListenerMockedStatic.when(() -> BinlogEventListener.create(eq(buffer), any(RdsSourceConfig.class), any(String.class), eq(pluginMetrics), eq(binlogClient), eq(streamCheckpointer), eq(acknowledgementSetManager)))
                    .thenReturn(binlogEventListener);
            streamWorkerTaskRefresher.initialize(sourceConfig);
            streamWorkerTaskRefresher.update(sourceConfig2);
        }

        verify(credentialsChangeCounter).increment();
        verify(executorService).shutdownNow();

        verify(binlogClientFactory, times(2)).create();
        verify(binlogClient, times(2)).registerEventListener(binlogEventListener);

        ArgumentCaptor<Runnable> runnableArgumentCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(newExecutorService).submit(runnableArgumentCaptor.capture());

        Runnable capturedRunnable = runnableArgumentCaptor.getValue();
        capturedRunnable.run();
        verify(streamWorker).processStream(streamPartition);
    }

    @Test
    void test_update_when_credentials_unchanged_then_do_nothing() {
        final String username = UUID.randomUUID().toString();
        final String password = UUID.randomUUID().toString();
        when(sourceConfig.getAuthenticationConfig().getUsername()).thenReturn(username);
        when(sourceConfig.getAuthenticationConfig().getPassword()).thenReturn(password);

        when(binlogClientFactory.create()).thenReturn(binlogClient);

        try (MockedStatic<StreamWorker> streamWorkerMockedStatic = mockStatic(StreamWorker.class);
             MockedStatic<BinlogEventListener> binlogEventListenerMockedStatic = mockStatic(BinlogEventListener.class)) {
            streamWorkerMockedStatic.when(() -> StreamWorker.create(eq(sourceCoordinator), any(BinaryLogClient.class), eq(pluginMetrics)))
                    .thenReturn(streamWorker);
            binlogEventListenerMockedStatic.when(() -> BinlogEventListener.create(eq(buffer), any(RdsSourceConfig.class), any(String.class), eq(pluginMetrics), eq(binlogClient), eq(streamCheckpointer), eq(acknowledgementSetManager)))
                    .thenReturn(binlogEventListener);
            streamWorkerTaskRefresher.initialize(sourceConfig);
            streamWorkerTaskRefresher.update(sourceConfig);
        }

        verify(credentialsChangeCounter, never()).increment();
        verify(executorService, never()).shutdownNow();
    }

    private StreamWorkerTaskRefresher createObjectUnderTest() {
        final String s3Prefix = UUID.randomUUID().toString();

        return new StreamWorkerTaskRefresher(
                sourceCoordinator, streamPartition, streamCheckpointer, s3Prefix, binlogClientFactory, buffer, acknowledgementSetManager, executorService, pluginMetrics);
    }
}
