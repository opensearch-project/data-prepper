/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.stream;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import io.micrometer.core.instrument.Counter;
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
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;
import org.opensearch.dataprepper.plugins.source.rds.model.DbTableMetadata;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.rds.model.TableMetadata;
import org.opensearch.dataprepper.plugins.source.rds.resync.CascadingActionDetector;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.rds.stream.BinlogEventListener.REPLICATION_LOG_EVENT_PROCESSING_TIME;
import static org.opensearch.dataprepper.plugins.source.rds.stream.BinlogEventListener.REPLICATION_LOG_PROCESSING_ERROR_COUNT;

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

    @Mock
    private DbTableMetadata dbTableMetadata;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private com.github.shyiko.mysql.binlog.event.Event binlogEvent;

    private String s3Prefix;

    private BinlogEventListener objectUnderTest;

    private Timer eventProcessingTimer;
    private Counter eventProcessingErrorCounter;
    private Counter defaultCounter;

    @BeforeEach
    void setUp() {
        s3Prefix = UUID.randomUUID().toString();
        eventProcessingTimer = Metrics.timer(REPLICATION_LOG_EVENT_PROCESSING_TIME);
        eventProcessingErrorCounter = Metrics.counter(REPLICATION_LOG_PROCESSING_ERROR_COUNT);
        defaultCounter = Metrics.counter("default-test-counter");
        when(pluginMetrics.timer(REPLICATION_LOG_EVENT_PROCESSING_TIME)).thenReturn(eventProcessingTimer);
        lenient().when(pluginMetrics.counter(REPLICATION_LOG_PROCESSING_ERROR_COUNT)).thenReturn(eventProcessingErrorCounter);
        lenient().when(pluginMetrics.counter(any())).thenReturn(defaultCounter);
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
    @EnumSource(names = {"UPDATE_ROWS", "EXT_UPDATE_ROWS"})
    void test_given_UpdateRows_event_when_primary_key_changes_then_generate_correct_events(EventType eventType) throws NoSuchFieldException, IllegalAccessException {
        final UpdateRowsEventData data = mock(UpdateRowsEventData.class);
        final Serializable[] oldCol1Data = new Serializable[]{1, "a"};
        final Serializable[] newCol1Data = new Serializable[]{2, "a"};
        final Serializable[] oldCol2Data = new Serializable[]{3, "b"};
        final Serializable[] newCol2Data = new Serializable[]{1, "b"};
        final List<Map.Entry<Serializable[], Serializable[]>> rows = List.of(
                Map.entry(oldCol1Data, newCol1Data),
                Map.entry(oldCol2Data, newCol2Data)
        );
        final long tableId = 1234L;
        when(binlogEvent.getHeader().getEventType()).thenReturn(eventType);
        when(binlogEvent.getData()).thenReturn(data);
        when(data.getTableId()).thenReturn(tableId);
        when(objectUnderTest.isValidTableId(tableId)).thenReturn(true);
        when(data.getRows()).thenReturn(rows);

        // Set tableMetadataMap reflectively
        final TableMetadata tableMetadata = mock(TableMetadata.class);
        final Map<Long, TableMetadata> tableMetadataMap = Map.of(tableId, tableMetadata);
        Field tableMetadataMapField = BinlogEventListener.class.getDeclaredField("tableMetadataMap");
        tableMetadataMapField.setAccessible(true);
        tableMetadataMapField.set(objectUnderTest, tableMetadataMap);
        when(tableMetadata.getPrimaryKeys()).thenReturn(List.of("col1"));
        when(tableMetadata.getColumnNames()).thenReturn(List.of("col1", "col2"));

        objectUnderTest.onEvent(binlogEvent);

        verifyHandlerCallHelper();
        verify(objectUnderTest).handleUpdateEvent(binlogEvent);

        // verify rowList and bulkActionList that were sent to handleRowChangeEvent() were correct
        ArgumentCaptor<List<Serializable[]>> rowListArgumentCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<List<OpenSearchBulkActions>> bulkActionListArgumentCaptor = ArgumentCaptor.forClass(List.class);
        verify(objectUnderTest).handleRowChangeEvent(eq(binlogEvent), eq(tableId), rowListArgumentCaptor.capture(), bulkActionListArgumentCaptor.capture());
        List<Serializable[]> rowList = rowListArgumentCaptor.getValue();
        List<OpenSearchBulkActions> bulkActionList = bulkActionListArgumentCaptor.getValue();

        assertThat(rowList.size(), is(4));
        assertThat(bulkActionList.size(), is(4));
        assertThat(rowList.get(0), is(oldCol1Data));
        assertThat(bulkActionList.get(0), is(OpenSearchBulkActions.DELETE));
        assertThat(rowList.get(1), is(newCol1Data));
        assertThat(bulkActionList.get(1), is(OpenSearchBulkActions.INDEX));
        assertThat(rowList.get(2), is(oldCol2Data));
        assertThat(bulkActionList.get(2), is(OpenSearchBulkActions.DELETE));
        assertThat(rowList.get(3), is(newCol2Data));
        assertThat(bulkActionList.get(3), is(OpenSearchBulkActions.INDEX));
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
                streamCheckpointer, acknowledgementSetManager, dbTableMetadata, cascadingActionDetector);
    }

    private void verifyHandlerCallHelper() {
        ArgumentCaptor<Runnable> runnableArgumentCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(eventListnerExecutorService).submit(runnableArgumentCaptor.capture());

        Runnable capturedRunnable = runnableArgumentCaptor.getValue();
        capturedRunnable.run();
    }
}
