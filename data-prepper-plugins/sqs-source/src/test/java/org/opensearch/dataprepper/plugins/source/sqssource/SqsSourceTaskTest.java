package org.opensearch.dataprepper.plugins.source.sqssource;

import com.linecorp.armeria.client.retry.Backoff;
import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.aws.sqs.common.SqsService;
import org.opensearch.dataprepper.plugins.aws.sqs.common.handler.SqsMessageHandler;
import org.opensearch.dataprepper.plugins.aws.sqs.common.metrics.SqsMetrics;
import org.opensearch.dataprepper.plugins.aws.sqs.common.model.SqsOptions;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import org.opensearch.dataprepper.plugins.source.sqssource.handler.RawSqsMessageHandler;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SqsException;
import software.amazon.awssdk.services.sts.model.StsException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;


class SqsSourceTaskTest {

    private static final String TEST_PIPELINE_NAME = "pipeline";

    private static final String MESSAGE = "message";

    @Mock
    private SqsService sqsService;

    @Mock
    private SqsOptions sqsOptions;

    @Mock
    private SqsMetrics sqsMetrics;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    private final boolean endToEndAcknowledgementsEnabled = false;

    static final Duration BUFFER_TIMEOUT = Duration.ofSeconds(10);

    static final int NO_OF_RECORDS_TO_ACCUMULATE = 100;

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
        backoff = mock(Backoff.class);
        sqsClient = mock(SqsClient.class);
        sqsMetrics = mock(SqsMetrics.class);
        messageReceivedCounter = mock(Counter.class);
        messageDeletedCounter = mock(Counter.class);
        sqsMessagesFailedCounter = mock(Counter.class);
        buffer = getBuffer();
        acknowledgementSetManager = mock(AcknowledgementSetManager.class);
        acknowledgementSet = mock(AcknowledgementSet.class);
        when(sqsMetrics.getSqsMessagesReceivedCounter()).thenReturn(messageReceivedCounter);
        when(sqsMetrics.getSqsMessagesDeletedCounter()).thenReturn(messageDeletedCounter);
    }

    private BlockingBuffer<Record<Event>> getBuffer() {
        final HashMap<String, Object> integerHashMap = new HashMap<>();
        integerHashMap.put("buffer_size", 2);
        integerHashMap.put("batch_size", 2);
        final PluginSetting pluginSetting = new PluginSetting("blocking_buffer", integerHashMap);
        pluginSetting.setPipelineName(TEST_PIPELINE_NAME);
        return new BlockingBuffer<>(pluginSetting);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "'{\"S.No\":\"1\",\"name\":\"data-prep\",\"country\":\"USA\"}'",
            "Test Message",
            "'2023-05-30T13:25:11,889 [main] INFO  org.opensearch.dataprepper.pipeline.server.DataPrepperServer - Data Prepper server running at :4900'"})
    void processSqsMessages_test_with_different_types_of_messages(final String message) {
        List<Message> messageList = List.of(Message.builder().body(message).messageId(UUID.randomUUID().toString()).receiptHandle(UUID.randomUUID().toString()).build());
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(ReceiveMessageResponse.builder().messages(messageList).build());
        when(sqsMetrics.getSqsMessagesReceivedCounter()).thenReturn(messageReceivedCounter);
        when(sqsMetrics.getSqsMessagesDeletedCounter()).thenReturn(messageDeletedCounter);

        when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class))).
                thenReturn(DeleteMessageBatchResponse.builder().successful(builder -> builder.id(UUID.randomUUID().toString()).build()).build());
        final SqsSourceTask sqsSourceTask = createObjectUnderTest(buffer,endToEndAcknowledgementsEnabled);
        sqsSourceTask.processSqsMessages();

        final List<Record<Event>> bufferEvents = new ArrayList<>(buffer.read((int) Duration.ofSeconds(10).toMillis()).getKey());
        final String bufferMessage = bufferEvents.get(0).getData().get(MESSAGE, String.class);

        assertThat(bufferMessage,equalTo(message));
        verify(sqsMetrics.getSqsMessagesReceivedCounter()).increment();
        verify(sqsMetrics.getSqsMessagesDeletedCounter()).increment(1);
    }

    private SqsSourceTask createObjectUnderTest(Buffer<Record<Event>> buffer,boolean endToEndAckFlag) {
        final BufferAccumulator<Record<Event>> recordBufferAccumulator =
                BufferAccumulator.create(buffer, NO_OF_RECORDS_TO_ACCUMULATE, BUFFER_TIMEOUT);
        sqsService = new SqsService(sqsMetrics,sqsClient,backoff);
        sqsHandler = new RawSqsMessageHandler(sqsService);
        sqsOptions = new SqsOptions.Builder()
                .setSqsUrl("https://sqs.us-east-2.amazonaws.com/123456789012/MyQueue")
                .setVisibilityTimeout(Duration.ofSeconds(30))
                .setWaitTime(Duration.ofSeconds(20)).build();
        return new SqsSourceTask(buffer,100,Duration.ofSeconds(10),sqsService,
                sqsOptions,
                sqsMetrics,
                acknowledgementSetManager,
                endToEndAckFlag,
                sqsHandler);
    }

    @Test
    void processSqsMessages_should_return_zero_messages_with_backoff() {
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenThrow(SqsException.class);
        when(sqsMetrics.getSqsReceiveMessagesFailedCounter()).thenReturn(sqsMessagesFailedCounter);
        createObjectUnderTest(buffer,endToEndAcknowledgementsEnabled).processSqsMessages();
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
        createObjectUnderTest(mock(Buffer.class),endToEndAcknowledgementsEnabled).processSqsMessages();
        verify(backoff).nextDelayMillis(1);
        verify(messageReceivedCounter).increment();
    }

    @Test
    void processSqsMessages_test_with_different_types_of_messages_with_end_to_end_ack() {
        String message = "'{\"S.No\":\"1\",\"name\":\"data-prep\",\"country\":\"USA\"}'";
        when(acknowledgementSetManager.create(any( Consumer.class), any(Duration.class))).thenReturn(acknowledgementSet);
        List<Message> messageList = List.of(Message.builder().body(message).messageId(UUID.randomUUID().toString()).receiptHandle(UUID.randomUUID().toString()).build());
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(ReceiveMessageResponse.builder().messages(messageList).build());
        when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class))).
                thenReturn(DeleteMessageBatchResponse.builder().successful(builder -> builder.id(UUID.randomUUID().toString()).build()).build());

        createObjectUnderTest(buffer,true).processSqsMessages();

        final List<Record<Event>> bufferEvents = new ArrayList<>(buffer.read((int) Duration.ofSeconds(10).toMillis()).getKey());
        final String bufferMessage = bufferEvents.get(0).getData().get(MESSAGE, String.class);

        assertThat(bufferMessage,equalTo(message));
        verify(sqsMetrics.getSqsMessagesReceivedCounter()).increment();
        verify(acknowledgementSetManager).create(any(), any(Duration.class));
        verifyNoInteractions(sqsMetrics.getSqsMessagesDeletedCounter());
    }

}

