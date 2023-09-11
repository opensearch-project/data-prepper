package org.opensearch.dataprepper.plugins.source.sqssource.handler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.aws.sqs.common.SqsService;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RawSqsMessageHandlerTest {
    @Mock
    private SqsService sqsService;

    @Mock
    private BufferAccumulator bufferAccumulator;

    private AcknowledgementSet acknowledgementSet;
    private List<String> messageBodies;
    private List<Message> messages;

    @BeforeEach
    void setUp() {
        messageBodies = IntStream.range(0, 3).mapToObj(i -> UUID.randomUUID().toString())
                .collect(Collectors.toList());

        messages = messageBodies.stream()
                .map(body -> {
                    Message message = mock(Message.class);
                    when(message.body()).thenReturn(body);
                    return message;
                })
                .collect(Collectors.toList());


        acknowledgementSet = null;
    }

    private RawSqsMessageHandler createObjectUnderTest() {
        return new RawSqsMessageHandler(sqsService);
    }

    @Test
    void handleMessages_writes_to_buffer_and_flushes() throws Exception {
        createObjectUnderTest().handleMessages(messages, bufferAccumulator, acknowledgementSet);

        InOrder inOrder = inOrder(bufferAccumulator);

        ArgumentCaptor<Record<Event>> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);

        inOrder.verify(bufferAccumulator, times(messages.size())).add(recordArgumentCaptor.capture());
        inOrder.verify(bufferAccumulator).flush();

        List<Object> actualEventData = recordArgumentCaptor.getAllValues()
                .stream()
                .map(Record::getData)
                .map(e -> e.get("message", Object.class))
                .collect(Collectors.toList());

        assertThat(actualEventData.size(), equalTo(messages.size()));

        for (int i = 0; i < actualEventData.size(); i++){
            Object messageData = actualEventData.get(i);
            assertThat(messageData, instanceOf(String.class));
            assertThat(messageData, equalTo(messageBodies.get(i)));
        }
    }

    @Test
    void handleMessages_returns_deleteList() throws Exception {
        List<DeleteMessageBatchRequestEntry> stubbedDeleteList = List.of(mock(DeleteMessageBatchRequestEntry.class));
        when(sqsService.getDeleteMessageBatchRequestEntryList(messages))
                .thenReturn(stubbedDeleteList);

        List<DeleteMessageBatchRequestEntry> actualList = createObjectUnderTest().handleMessages(messages, bufferAccumulator, acknowledgementSet);

        assertThat(actualList, equalTo(stubbedDeleteList));
    }

    @Nested
    class WithAcknowledgementSet {
        @BeforeEach
        void setUp() {
            acknowledgementSet = mock(AcknowledgementSet.class);
        }

        @Test
        void handleMessages_with_acknowledgementSet_adds_events() throws Exception {
            createObjectUnderTest().handleMessages(messages, bufferAccumulator, acknowledgementSet);

            ArgumentCaptor<Event> eventArgumentCaptor = ArgumentCaptor.forClass(Event.class);

            verify(acknowledgementSet, times(messages.size())).add(eventArgumentCaptor.capture());

            List<Object> actualEventData = eventArgumentCaptor.getAllValues()
                    .stream()
                    .map(e -> e.get("message", Object.class))
                    .collect(Collectors.toList());

            assertThat(actualEventData.size(), equalTo(messages.size()));

            for (int i = 0; i < actualEventData.size(); i++) {
                Object messageData = actualEventData.get(i);
                assertThat(messageData, instanceOf(String.class));
                assertThat(messageData, equalTo(messageBodies.get(i)));
            }
        }
    }
}
