package org.opensearch.dataprepper.plugins.source.sqssource;

import com.linecorp.armeria.client.retry.Backoff;
import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.aws.sqs.common.SqsService;
import org.opensearch.dataprepper.plugins.aws.sqs.common.handler.SqsMessageHandler;
import org.opensearch.dataprepper.plugins.aws.sqs.common.metrics.SqsMetrics;
import org.opensearch.dataprepper.plugins.aws.sqs.common.model.SqsOptions;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SqsException;
import software.amazon.awssdk.services.sts.model.StsException;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class SqsSourceTaskTest {

    private SqsService sqsService;

    private SqsOptions sqsOptions;

    @Mock
    private SqsMetrics sqsMetrics;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    private boolean endToEndAcknowledgementsEnabled = false;

    @Mock
    private SqsMessageHandler sqsHandler;

    @Mock
    private SqsClient sqsClient;

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private Backoff backoff;

    private Counter messageReceivedCounter;

    private Counter messageDeletedCounter;

    private Counter sqsMessagesFailedCounter;

    private AcknowledgementSet acknowledgementSet;

    @BeforeEach
    public void setup(){
        messageReceivedCounter = mock(Counter.class);
        messageDeletedCounter = mock(Counter.class);
        sqsMessagesFailedCounter = mock(Counter.class);
        acknowledgementSet = mock(AcknowledgementSet.class);
    }

    private SqsSourceTask createObjectUnderTest() {
        sqsService = new SqsService(sqsMetrics,sqsClient,backoff);
        sqsOptions = new SqsOptions.Builder()
                .setSqsUrl("https://sqs.us-east-2.amazonaws.com/123456789012/MyQueue")
                .setVisibilityTimeout(Duration.ofSeconds(30))
                .setWaitTime(Duration.ofSeconds(20)).build();
        return new SqsSourceTask(buffer,1,Duration.ofSeconds(10),sqsService,
                sqsOptions,
                sqsMetrics,
                acknowledgementSetManager,
                endToEndAcknowledgementsEnabled,
                sqsHandler);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "'{\"S.No\":\"1\",\"name\":\"data-prep\",\"country\":\"USA\"}'",
            "Test Message",
            "'2023-05-30T13:25:11,889 [main] INFO  org.opensearch.dataprepper.pipeline.server.DataPrepperServer - Data Prepper server running at :4900'"})
    void processSqsMessages_test_with_different_types_of_messages(final String message) throws Exception {
        when(sqsMetrics.getSqsMessagesReceivedCounter()).thenReturn(messageReceivedCounter);
        when(sqsMetrics.getSqsMessagesDeletedCounter()).thenReturn(messageDeletedCounter);

        List<Message> messageList = List.of(Message.builder().body(message).messageId(UUID.randomUUID().toString()).receiptHandle(UUID.randomUUID().toString()).build());
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(ReceiveMessageResponse.builder().messages(messageList).build());
        when(sqsMetrics.getSqsMessagesReceivedCounter()).thenReturn(messageReceivedCounter);
        when(sqsMetrics.getSqsMessagesDeletedCounter()).thenReturn(messageDeletedCounter);

        when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class))).
                thenReturn(DeleteMessageBatchResponse.builder().successful(builder -> builder.id(UUID.randomUUID().toString()).build()).build());
        final SqsSourceTask sqsSourceTask = createObjectUnderTest();
        sqsSourceTask.processSqsMessages();

        verify(sqsHandler).handleMessages(eq(messageList), any(), isNull());

        verify(sqsMetrics.getSqsMessagesReceivedCounter()).increment();
        verify(sqsMetrics.getSqsMessagesDeletedCounter()).increment(1);
    }

    @Test
    void processSqsMessages_should_return_zero_messages_with_backoff() {
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenThrow(SqsException.class);
        when(sqsMetrics.getSqsReceiveMessagesFailedCounter()).thenReturn(sqsMessagesFailedCounter);
        createObjectUnderTest().processSqsMessages();
        verify(backoff).nextDelayMillis(1);
        verify(sqsMessagesFailedCounter).increment();
    }

    @Test
    void processSqsMessages_should_return_one_message_with_buffer_write_fail_with_backoff() {
        String message ="'{\"S.No\":\"1\",\"name\":\"data-prep\",\"country\":\"USA\"}'";
        List<Message> messageList = List.of(Message.builder().body(message).messageId(UUID.randomUUID().toString()).receiptHandle(UUID.randomUUID().toString()).build());
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(ReceiveMessageResponse.builder().messages(messageList).build());
        when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class))).thenThrow(mock(StsException.class));
        when(sqsMetrics.getSqsMessagesReceivedCounter()).thenReturn(messageReceivedCounter);
        when(sqsMetrics.getSqsMessagesDeleteFailedCounter()).thenReturn(messageDeletedCounter);
        createObjectUnderTest().processSqsMessages();
        verify(backoff).nextDelayMillis(1);
        verify(messageReceivedCounter).increment();
    }

    @Test
    void processSqsMessages_test_with_different_types_of_messages_with_end_to_end_ack() throws Exception {
        when(sqsMetrics.getSqsMessagesReceivedCounter()).thenReturn(messageReceivedCounter);
        when(sqsMetrics.getSqsMessagesDeletedCounter()).thenReturn(messageDeletedCounter);

        endToEndAcknowledgementsEnabled = true;

        String message = "'{\"S.No\":\"1\",\"name\":\"data-prep\",\"country\":\"USA\"}'";
        when(acknowledgementSetManager.create(any( Consumer.class), any(Duration.class))).thenReturn(acknowledgementSet);
        List<Message> messageList = List.of(Message.builder().body(message).messageId(UUID.randomUUID().toString()).receiptHandle(UUID.randomUUID().toString()).build());
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(ReceiveMessageResponse.builder().messages(messageList).build());

        createObjectUnderTest().processSqsMessages();

        verify(sqsHandler).handleMessages(eq(messageList), any(), eq(acknowledgementSet));

        verify(sqsMetrics.getSqsMessagesReceivedCounter()).increment();
        verify(acknowledgementSetManager).create(any(), any(Duration.class));
        verifyNoInteractions(sqsMetrics.getSqsMessagesDeletedCounter());
    }

}

