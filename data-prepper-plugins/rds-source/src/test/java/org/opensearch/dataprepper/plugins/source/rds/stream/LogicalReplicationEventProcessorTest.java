/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.rds.stream;

import io.micrometer.core.instrument.Metrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.rds.model.MessageType;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LogicalReplicationEventProcessorTest {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private StreamPartition streamPartition;

    @Mock
    private RdsSourceConfig sourceConfig;

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private LogicalReplicationClient logicalReplicationClient;

    @Mock
    private StreamCheckpointer streamCheckpointer;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    private ByteBuffer message;

    private String s3Prefix;

    private LogicalReplicationEventProcessor objectUnderTest;

    private Random random;

    @BeforeEach
    void setUp() {
        s3Prefix = UUID.randomUUID().toString();
        random = new Random();
        when(pluginMetrics.timer(anyString())).thenReturn(Metrics.timer("test-timer"));
        when(pluginMetrics.counter(anyString())).thenReturn(Metrics.counter("test-counter"));

        objectUnderTest = spy(createObjectUnderTest());
    }

    @Test
    void test_correct_process_method_invoked_for_begin_message() {
        setMessageType(MessageType.BEGIN);
        doNothing().when(objectUnderTest).processBeginMessage(message);

        objectUnderTest.process(message);

        verify(objectUnderTest).processBeginMessage(message);
    }

    @Test
    void test_correct_process_method_invoked_for_relation_message() {
        setMessageType(MessageType.RELATION);
        doNothing().when(objectUnderTest).processRelationMessage(message);

        objectUnderTest.process(message);

        verify(objectUnderTest).processRelationMessage(message);
    }

    @Test
    void test_correct_process_method_invoked_for_commit_message() {
        setMessageType(MessageType.COMMIT);
        doNothing().when(objectUnderTest).processCommitMessage(message);

        objectUnderTest.process(message);

        verify(objectUnderTest).processCommitMessage(message);
    }

    @Test
    void test_correct_process_method_invoked_for_insert_message() {
        setMessageType(MessageType.INSERT);
        doNothing().when(objectUnderTest).processInsertMessage(message);

        objectUnderTest.process(message);

        verify(objectUnderTest).processInsertMessage(message);
    }

    @Test
    void test_correct_process_method_invoked_for_update_message() {
        setMessageType(MessageType.UPDATE);
        doNothing().when(objectUnderTest).processUpdateMessage(message);

        objectUnderTest.process(message);

        verify(objectUnderTest).processUpdateMessage(message);
    }

    @Test
    void test_correct_process_method_invoked_for_delete_message() {
        setMessageType(MessageType.DELETE);
        doNothing().when(objectUnderTest).processDeleteMessage(message);

        objectUnderTest.process(message);

        verify(objectUnderTest).processDeleteMessage(message);
    }

    @Test
    void test_correct_process_method_invoked_for_type_message() {
        setMessageType(MessageType.TYPE);
        doNothing().when(objectUnderTest).processTypeMessage(message);

        objectUnderTest.process(message);

        verify(objectUnderTest).processTypeMessage(message);
    }

    @Test
    void test_unsupported_message_type_throws_exception() {
        message = ByteBuffer.allocate(1);
        message.put((byte) 'A');
        message.flip();

        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.process(message));
    }

    @Test
    void test_stopClient() {
        objectUnderTest.stopClient();
        verify(logicalReplicationClient).disconnect();
    }

    private LogicalReplicationEventProcessor createObjectUnderTest() {
        return new LogicalReplicationEventProcessor(streamPartition, sourceConfig, buffer, s3Prefix, pluginMetrics,
                logicalReplicationClient, streamCheckpointer, acknowledgementSetManager);
    }

    private void setMessageType(MessageType messageType) {
        message = ByteBuffer.allocate(1);
        message.put((byte) messageType.getValue());
        message.flip();
    }
}
