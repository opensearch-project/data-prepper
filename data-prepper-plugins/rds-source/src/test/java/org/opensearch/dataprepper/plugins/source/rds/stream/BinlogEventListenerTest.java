/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.stream;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.EventType;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.rds.resync.CascadingActionDetector;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.rds.stream.BinlogEventListener.REPLICATION_LOG_EVENT_PROCESSING_TIME;

@ExtendWith(MockitoExtension.class)
class BinlogEventListenerTest {

    @Mock
    private StreamPartition streamPartition;

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private RdsSourceConfig sourceConfig;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private BinaryLogClient binaryLogClient;

    @Mock
    private StreamCheckpointer streamCheckpointer;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private CascadingActionDetector cascadingActionDetector;

    @Mock
    private ExecutorService eventListnerExecutorService;

    @Mock
    private ExecutorService checkpointManagerExecutorService;

    @Mock
    private ThreadFactory threadFactory;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private com.github.shyiko.mysql.binlog.event.Event binlogEvent;

    private String s3Prefix;

    private BinlogEventListener objectUnderTest;

    private Timer eventProcessingTimer;

    @BeforeEach
    void setUp() {
        s3Prefix = UUID.randomUUID().toString();
        eventProcessingTimer = Metrics.timer("test-timer");
        when(pluginMetrics.timer(REPLICATION_LOG_EVENT_PROCESSING_TIME)).thenReturn(eventProcessingTimer);
        try (final MockedStatic<Executors> executorsMockedStatic = mockStatic(Executors.class)) {
            executorsMockedStatic.when(() -> Executors.newFixedThreadPool(anyInt(), any(ThreadFactory.class))).thenReturn(eventListnerExecutorService);
            executorsMockedStatic.when(Executors::newSingleThreadExecutor).thenReturn(checkpointManagerExecutorService);
            executorsMockedStatic.when(Executors::defaultThreadFactory).thenReturn(threadFactory);
            objectUnderTest = spy(createObjectUnderTest());
        }
    }

    @Test
    void test_given_TableMap_event_then_calls_correct_handler() {
        when(binlogEvent.getHeader().getEventType()).thenReturn(EventType.TABLE_MAP);
        doNothing().when(objectUnderTest).handleTableMapEvent(binlogEvent);

        objectUnderTest.onEvent(binlogEvent);

        verifyHandlerCallHelper();
        verify(objectUnderTest).handleTableMapEvent(binlogEvent);
    }

    @Test
    void test_stopClient() throws IOException {
        objectUnderTest.stopClient();

        InOrder inOrder = inOrder(binaryLogClient, eventListnerExecutorService);
        inOrder.verify(binaryLogClient).disconnect();
        inOrder.verify(binaryLogClient).unregisterEventListener(objectUnderTest);
        inOrder.verify(eventListnerExecutorService).shutdownNow();
    }

    @ParameterizedTest
    @EnumSource(names = {"WRITE_ROWS", "EXT_WRITE_ROWS"})
    void test_given_WriteRows_event_then_calls_correct_handler(EventType eventType) {
        when(binlogEvent.getHeader().getEventType()).thenReturn(eventType);
        doNothing().when(objectUnderTest).handleInsertEvent(binlogEvent);

        objectUnderTest.onEvent(binlogEvent);

        verifyHandlerCallHelper();
        verify(objectUnderTest).handleInsertEvent(binlogEvent);
    }

    @ParameterizedTest
    @EnumSource(names = {"UPDATE_ROWS", "EXT_UPDATE_ROWS"})
    void test_given_UpdateRows_event_then_calls_correct_handler(EventType eventType) {
        when(binlogEvent.getHeader().getEventType()).thenReturn(eventType);
        doNothing().when(objectUnderTest).handleUpdateEvent(binlogEvent);

        objectUnderTest.onEvent(binlogEvent);

        verifyHandlerCallHelper();
        verify(objectUnderTest).handleUpdateEvent(binlogEvent);
    }

    @ParameterizedTest
    @EnumSource(names = {"DELETE_ROWS", "EXT_DELETE_ROWS"})
    void test_given_DeleteRows_event_then_calls_correct_handler(EventType eventType) {
        when(binlogEvent.getHeader().getEventType()).thenReturn(eventType);
        doNothing().when(objectUnderTest).handleDeleteEvent(binlogEvent);

        objectUnderTest.onEvent(binlogEvent);

        verifyHandlerCallHelper();
        verify(objectUnderTest).handleDeleteEvent(binlogEvent);
    }

    private BinlogEventListener createObjectUnderTest() {
        return BinlogEventListener.create(streamPartition, buffer, sourceConfig, s3Prefix, pluginMetrics, binaryLogClient,
                streamCheckpointer, acknowledgementSetManager, cascadingActionDetector);
    }

    private void verifyHandlerCallHelper() {
        ArgumentCaptor<Runnable> runnableArgumentCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(eventListnerExecutorService).submit(runnableArgumentCaptor.capture());

        Runnable capturedRunnable = runnableArgumentCaptor.getValue();
        capturedRunnable.run();
    }
}