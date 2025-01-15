package org.opensearch.dataprepper.plugins.source.rds.stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.StreamPartition;

import java.nio.ByteBuffer;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LogicalReplicationEventProcessorTest {

    @Mock
    private StreamPartition streamPartition;

    @Mock
    private RdsSourceConfig sourceConfig;

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private ByteBuffer message;

    private String s3Prefix;

    private LogicalReplicationEventProcessor objectUnderTest;

    @BeforeEach
    void setUp() {
        s3Prefix = UUID.randomUUID().toString();
        objectUnderTest = spy(createObjectUnderTest());
    }

    @Test
    void test_correct_process_method_invoked_for_begin_message() {
        when(message.get()).thenReturn((byte) 'B');

        objectUnderTest.process(message);

        verify(objectUnderTest).processBeginMessage(message);
    }

    @Test
    void test_correct_process_method_invoked_for_relation_message() {
        when(message.get()).thenReturn((byte) 'R');

        objectUnderTest.process(message);

        verify(objectUnderTest).processRelationMessage(message);
    }

    @Test
    void test_correct_process_method_invoked_for_commit_message() {
        when(message.get()).thenReturn((byte) 'C');

        objectUnderTest.process(message);

        verify(objectUnderTest).processCommitMessage(message);
    }

    @Test
    void test_correct_process_method_invoked_for_insert_message() {
        when(message.get()).thenReturn((byte) 'I');
        doNothing().when(objectUnderTest).processInsertMessage(message);

        objectUnderTest.process(message);

        verify(objectUnderTest).processInsertMessage(message);
    }

    @Test
    void test_correct_process_method_invoked_for_update_message() {
        when(message.get()).thenReturn((byte) 'U');
        doNothing().when(objectUnderTest).processUpdateMessage(message);

        objectUnderTest.process(message);

        verify(objectUnderTest).processUpdateMessage(message);
    }

    @Test
    void test_correct_process_method_invoked_for_delete_message() {
        when(message.get()).thenReturn((byte) 'D');
        doNothing().when(objectUnderTest).processDeleteMessage(message);

        objectUnderTest.process(message);

        verify(objectUnderTest).processDeleteMessage(message);
    }

    @Test
    void test_unsupported_message_type_throws_exception() {
        when(message.get()).thenReturn((byte) 'A');

        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.process(message));
    }

    private LogicalReplicationEventProcessor createObjectUnderTest() {
        return new LogicalReplicationEventProcessor(streamPartition, sourceConfig, buffer, s3Prefix);
    }
}
