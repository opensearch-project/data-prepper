/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.stream;

import com.github.shyiko.mysql.binlog.event.EventType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BinlogEventListenerTest {

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private RdsSourceConfig sourceConfig;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private com.github.shyiko.mysql.binlog.event.Event binlogEvent;

    private static BinlogEventListener objectUnderTest;

    @BeforeEach
    void setUp() {
        objectUnderTest = spy(createObjectUnderTest());
    }

    @Test
    void test_given_TableMap_event_then_calls_correct_handler() {
        when(binlogEvent.getHeader().getEventType()).thenReturn(EventType.TABLE_MAP);
        doNothing().when(objectUnderTest).handleTableMapEvent(binlogEvent);

        objectUnderTest.onEvent(binlogEvent);

        verify(objectUnderTest).handleTableMapEvent(binlogEvent);
    }

    @ParameterizedTest
    @EnumSource(names = {"WRITE_ROWS", "EXT_WRITE_ROWS"})
    void test_given_WriteRows_event_then_calls_correct_handler(EventType eventType) {
        when(binlogEvent.getHeader().getEventType()).thenReturn(eventType);
        doNothing().when(objectUnderTest).handleInsertEvent(binlogEvent);

        objectUnderTest.onEvent(binlogEvent);

        verify(objectUnderTest).handleInsertEvent(binlogEvent);
    }

    @ParameterizedTest
    @EnumSource(names = {"UPDATE_ROWS", "EXT_UPDATE_ROWS"})
    void test_given_UpdateRows_event_then_calls_correct_handler(EventType eventType) {
        when(binlogEvent.getHeader().getEventType()).thenReturn(eventType);
        doNothing().when(objectUnderTest).handleUpdateEvent(binlogEvent);

        objectUnderTest.onEvent(binlogEvent);

        verify(objectUnderTest).handleUpdateEvent(binlogEvent);
    }

    @ParameterizedTest
    @EnumSource(names = {"DELETE_ROWS", "EXT_DELETE_ROWS"})
    void test_given_DeleteRows_event_then_calls_correct_handler(EventType eventType) {
        when(binlogEvent.getHeader().getEventType()).thenReturn(eventType);
        doNothing().when(objectUnderTest).handleDeleteEvent(binlogEvent);

        objectUnderTest.onEvent(binlogEvent);

        verify(objectUnderTest).handleDeleteEvent(binlogEvent);
    }

    private BinlogEventListener createObjectUnderTest() {
        return new BinlogEventListener(buffer, sourceConfig);
    }
}