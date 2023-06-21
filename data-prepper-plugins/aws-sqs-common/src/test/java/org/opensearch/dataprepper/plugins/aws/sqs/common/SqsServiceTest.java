package org.opensearch.dataprepper.plugins.aws.sqs.common;

import com.linecorp.armeria.client.retry.Backoff;
import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.plugins.aws.sqs.common.exception.SqsRetriesExhaustedException;
import org.opensearch.dataprepper.plugins.aws.sqs.common.metrics.SqsMetrics;
import org.opensearch.dataprepper.plugins.aws.sqs.common.model.SqsOptions;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sts.model.StsException;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SqsServiceTest {

    private SqsService sqsService;

    private SqsMetrics sqsMetrics;
    private SqsClient sqsClient;
    private Backoff backoff;

    private SqsOptions sqsOptions;

    @BeforeEach
    void createSqsService(){
        sqsMetrics = mock(SqsMetrics.class);
        sqsClient = mock(SqsClient.class);
        backoff = mock(Backoff.class);
        this.sqsOptions = new SqsOptions.Builder()
                .setSqsUrl("https://sqs.us-east-2.amazonaws.com/123456789012/MyQueue")
                .setMaximumMessages(10)
                .setWaitTime(Duration.ofSeconds(10))
                .setVisibilityTimeout(Duration.ofSeconds(10))
                .setPollDelay(Duration.ZERO).build();
        this.sqsService = new SqsService(sqsMetrics,sqsClient,backoff);
    }

    @Test
    void createReceiveMessageRequestTest(){
        final ReceiveMessageRequest receiveMessageRequest
                = sqsService.createReceiveMessageRequest(sqsOptions);
        assertThat(receiveMessageRequest.queueUrl(),equalTo(sqsOptions.getSqsUrl()));
        assertThat(receiveMessageRequest.maxNumberOfMessages(),equalTo(sqsOptions.getMaximumMessages()));
    }

    @Test
    void read_message_from_sqs(){
        String message = UUID.randomUUID().toString();
        List<Message> messageList = List.of(Message.builder().body(message).messageId(UUID.randomUUID().toString()).receiptHandle(UUID.randomUUID().toString()).build());
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(ReceiveMessageResponse.builder().messages(messageList).build());

        when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class))).
                thenReturn(DeleteMessageBatchResponse.builder().successful(builder -> builder.id(UUID.randomUUID().toString()).build()).build());

        final List<Message> messagesFromSqs = sqsService.getMessagesFromSqs(sqsOptions);

        assertThat(messagesFromSqs.get(0).body(),equalTo(message));
    }

    @Test
    void error_while_read_message_from_sqs_with_backoff_flow(){
        Counter sqsMessagesFailedCounter = mock(Counter.class);
        when(sqsMetrics.getSqsReceiveMessagesFailedCounter()).thenReturn(sqsMessagesFailedCounter);
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenThrow(StsException.class);
        final List<Message> messagesFromSqs = sqsService.getMessagesFromSqs(sqsOptions);
        verify(backoff).nextDelayMillis(1);
        assertThat(messagesFromSqs,equalTo(Collections.emptyList()));
        verify(sqsMessagesFailedCounter).increment();
    }

    @Test
    void delete_message_from_sqs_queue_success_flow() {
        Counter sqsMessagesDeletedCounter = mock(Counter.class);
        final List<DeleteMessageBatchRequestEntry> deleteMsgBatchReqList =
                List.of(DeleteMessageBatchRequestEntry.builder()
                        .id(UUID.randomUUID().toString()).build());
        when(sqsMetrics.getSqsMessagesDeletedCounter()).thenReturn(sqsMessagesDeletedCounter);
        when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class))).
                thenReturn(DeleteMessageBatchResponse.builder().successful(builder -> builder.id(UUID.randomUUID().toString()).build()).build());
        sqsService.deleteMessagesFromQueue(deleteMsgBatchReqList,sqsOptions.getSqsUrl());

        verify(sqsMessagesDeletedCounter).increment(1);
    }

    @Test
    void delete_message_from_sqs_queue_failed_flow() {
        Counter sqsMessagesDeleteFailedCounter = mock(Counter.class);
        final List<DeleteMessageBatchRequestEntry> deleteMsgBatchReqList =
                List.of(DeleteMessageBatchRequestEntry.builder()
                        .id(UUID.randomUUID().toString()).build());
        when(sqsMetrics.getSqsMessagesDeleteFailedCounter()).thenReturn(sqsMessagesDeleteFailedCounter);
        when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class))).
                thenReturn(DeleteMessageBatchResponse.builder().failed(builder -> builder.id(UUID.randomUUID().toString()).build()).build());
        sqsService.deleteMessagesFromQueue(deleteMsgBatchReqList,sqsOptions.getSqsUrl());

        verify(sqsMessagesDeleteFailedCounter).increment();
    }

    @Test
    void delete_message_test_with_exception_flow() {
        Counter sqsMessagesDeleteFailedCounter = mock(Counter.class);
        final List<DeleteMessageBatchRequestEntry> deleteMsgBatchReqList =
                List.of(DeleteMessageBatchRequestEntry.builder()
                        .id(UUID.randomUUID().toString()).build());
        when(sqsMetrics.getSqsMessagesDeleteFailedCounter()).thenReturn(sqsMessagesDeleteFailedCounter);
        when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class))).thenThrow(SdkClientException.class);
        sqsService.deleteMessagesFromQueue(deleteMsgBatchReqList,sqsOptions.getSqsUrl());

        verify(sqsMessagesDeleteFailedCounter).increment(1);
    }

    @Test
    void delete_messages_batch_list_object_test(){
        String messageId = UUID.randomUUID().toString();
        String receiptId = UUID.randomUUID().toString();
        List<Message> messageList = List.of(Message.builder().messageId(messageId).receiptHandle(receiptId).build());
        final List<DeleteMessageBatchRequestEntry> deleteMessageBatchRequestEntryList = sqsService.getDeleteMessageBatchRequestEntryList(messageList);

        assertThat(deleteMessageBatchRequestEntryList.get(0).id(),equalTo(messageId));
        assertThat(deleteMessageBatchRequestEntryList.get(0).receiptHandle(),equalTo(receiptId));
    }

    @Test
    void backoff_should_throw_when_a_SqsException_is_thrown_with_max_retries() {
        when(backoff.nextDelayMillis(anyInt())).thenReturn((long) -1);
        assertThrows(SqsRetriesExhaustedException.class, () -> sqsService.applyBackoff());
    }

    @Test
    void create_acknowledgement_set_test() {
        List<DeleteMessageBatchRequestEntry> entry = mock(List.class);
        AcknowledgementSet acknowledgementSetMockObj = mock(AcknowledgementSet.class);
        final Counter counter = mock(Counter.class);
        AcknowledgementSetManager acknowledgementSetManager = mock(AcknowledgementSetManager.class);
        when(acknowledgementSetManager.create(any(Consumer.class),any(Duration.class))).thenReturn(acknowledgementSetMockObj);
        when(sqsMetrics.getAcknowledgementSetCallbackCounter()).thenReturn(counter);
        final AcknowledgementSet acknowledgementSet = sqsService.createAcknowledgementSet(
                UUID.randomUUID().toString(),
                acknowledgementSetManager,
                entry);
        assertThat(acknowledgementSet,notNullValue());
        verify(acknowledgementSetManager).create(any(), any(Duration.class));
    }

}
