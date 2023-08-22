/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.aws.sqs.common;

import com.linecorp.armeria.client.retry.Backoff;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.plugins.aws.sqs.common.exception.SqsRetriesExhaustedException;
import org.opensearch.dataprepper.plugins.aws.sqs.common.metrics.SqsMetrics;
import org.opensearch.dataprepper.plugins.aws.sqs.common.model.SqsOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResultEntry;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SqsException;
import software.amazon.awssdk.services.sts.model.StsException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * contains the sqs related common functionality for reading
 * the messages and processing
 */
public class SqsService {
    private static final Logger LOG = LoggerFactory.getLogger(SqsService.class);

    static final Duration END_TO_END_ACK_TIME_OUT = Duration.ofSeconds(10);

    private final SqsMetrics sqsMetrics;

    private final SqsClient sqsClient;

    private final Backoff backoff;

    private int failedAttemptCount;


    public SqsService(final SqsMetrics sqsMetrics,
                      final SqsClient sqsClient,
                      final Backoff backoff) {
        this.sqsMetrics = sqsMetrics;
        this.sqsClient = sqsClient;
        this.backoff =backoff;

    }

    /**
     * Create a sqs message request object with the help of queue url and max messages count.
     *
     * @param sqsOptions - required sqs option object
     * @return ReceiveMessageRequest - return the Aws Message Request object.
     */
    public ReceiveMessageRequest createReceiveMessageRequest(final SqsOptions sqsOptions) {
        return ReceiveMessageRequest.builder()
                .queueUrl(sqsOptions.getSqsUrl())
                .maxNumberOfMessages(sqsOptions.getMaximumMessages())
                .visibilityTimeout(getTimeOutValueByDuration(sqsOptions.getVisibilityTimeout()))
                .waitTimeSeconds(getTimeOutValueByDuration(sqsOptions.getWaitTime()))
                .messageAttributeNames("All")
                .build();
    }

    private static Integer getTimeOutValueByDuration(final Duration duration) {
        if(Objects.nonNull(duration))
            return (int) duration.toSeconds();
        return null;
    }

    /**
     *  fetch the sqs message from provided sqs queue url options
     *
     * @param sqsOptions - required sqs option object
     * @return Messages list - return the list of sqs messages from queue.
     */
    public List<Message> getMessagesFromSqs(final SqsOptions sqsOptions) {
        try {
            final ReceiveMessageRequest receiveMessageRequest = createReceiveMessageRequest(sqsOptions);
            final ReceiveMessageResponse receiveMessageResponse = sqsClient.receiveMessage(receiveMessageRequest);
            return receiveMessageResponse.messages();
        } catch (final SqsException | StsException e) {
            LOG.error("Error reading from SQS: {}. Retrying with exponential backoff.", e.getMessage());
            sqsMetrics.getSqsReceiveMessagesFailedCounter().increment();
            applyBackoff();
            return Collections.emptyList();
        }
    }

    /**
     *  contains a back off functionality.
     *
     */
    public void applyBackoff() {
        final long delayMillis = backoff.nextDelayMillis(++failedAttemptCount);
        if (delayMillis < 0) {
            Thread.currentThread().interrupt();
            throw new SqsRetriesExhaustedException("SQS retries exhausted. Make sure that SQS configuration is valid, SQS queue exists, and IAM role has required permissions.");
        }
        final Duration delayDuration = Duration.ofMillis(delayMillis);
        LOG.info("Pausing SQS processing for {}.{} seconds due to an error in processing.",
                delayDuration.getSeconds(), delayDuration.toMillisPart());
        try {
            Thread.sleep(delayMillis);
        } catch (final InterruptedException e){
            LOG.error("Thread is interrupted while polling SQS with retry.", e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     *  helps to delete the sqs messages and update the respective metrics.
     *
     * @param deleteMsgBatchReqList - required list deleteMsgBatchReqList object
     * @param queueUrl - required queue url for deleting messages
     */
    public void deleteMessagesFromQueue(final List<DeleteMessageBatchRequestEntry> deleteMsgBatchReqList,
                                        final String queueUrl) {
        try{
            final DeleteMessageBatchResponse deleteMessageBatchResponse =
                    sqsClient.deleteMessageBatch(DeleteMessageBatchRequest.builder()
                    .queueUrl(queueUrl)
                    .entries(deleteMsgBatchReqList)
                    .build());
            updateMetricsForDeletedMessages(deleteMessageBatchResponse);
            updateMetricsForUnDeletedMessages(deleteMessageBatchResponse);
        }catch(Exception e){
            final int failedMessageCount = deleteMsgBatchReqList.size();
            sqsMetrics.getSqsMessagesDeleteFailedCounter().increment(failedMessageCount);
            LOG.error("Failed to delete {} messages from SQS due to {}.", failedMessageCount, e.getMessage());
            if(e instanceof StsException) {
                applyBackoff();
            }
        }
    }

    /**
     *  helps to update the metrics for delete succeed messages.
     * @param deleteMessageBatchResponse - required list deleteMessageBatchResponse object
     */
    private void updateMetricsForDeletedMessages(DeleteMessageBatchResponse deleteMessageBatchResponse) {
        if (deleteMessageBatchResponse.hasSuccessful()) {
            final int deletedMessagesCount = deleteMessageBatchResponse.successful().size();
            if (deletedMessagesCount > 0) {
                final String successfullyDeletedMessages = deleteMessageBatchResponse.successful().stream()
                        .map(DeleteMessageBatchResultEntry::id)
                        .collect(Collectors.joining(", "));
                LOG.info("Deleted {} messages from SQS. [{}]", deletedMessagesCount, successfullyDeletedMessages);
                sqsMetrics.getSqsMessagesDeletedCounter().increment(deletedMessagesCount);
            }
        }
    }

    /**
     * helps to update the metrics for delete failed messages.
     * @param deleteMessageBatchResponse - required list deleteMessageBatchResponse object
     */
    private void updateMetricsForUnDeletedMessages(DeleteMessageBatchResponse deleteMessageBatchResponse) {
        if(deleteMessageBatchResponse.hasFailed()) {
            final int failedDeleteCount = deleteMessageBatchResponse.failed().size();
            sqsMetrics.getSqsMessagesDeleteFailedCounter().increment();
            if(LOG.isErrorEnabled()) {
                final String failedMessages = deleteMessageBatchResponse.failed().stream()
                        .map(failed -> toString())
                        .collect(Collectors.joining(", "));
                LOG.error("Failed to delete {} messages from SQS with errors: [{}].", failedDeleteCount, failedMessages);
            }
        }
    }

    /**
     * helps to create and fetch to delete message batch request entry list from messages list.
     * @param messages - required list deleteMessageBatchResponse object
     * @return DeleteMessageBatchRequestEntry list - provide the DeleteMessageBatchRequestEntry list
     */
    public List<DeleteMessageBatchRequestEntry> getDeleteMessageBatchRequestEntryList(final List<Message> messages){
        final List<DeleteMessageBatchRequestEntry> deleteMsgBatchReqList = new ArrayList<>(messages.size());
        messages.forEach(message ->
                deleteMsgBatchReqList.add(DeleteMessageBatchRequestEntry.builder()
                        .id(message.messageId()).receiptHandle(message.receiptHandle()).build()));
        return deleteMsgBatchReqList;
    }

    /**
     *  helps to send end to end acknowledgements after successful processing.
     *
     * @param queueUrl - queue url for deleting the messages from the queue
     * @param acknowledgementSetManager - required acknowledgementSetManager for creating acknowledgementSet
     * @param waitingForAcknowledgements  - will pass the processed messages batch in Delete message batch request.
     * @return AcknowledgementSet - will generate the AcknowledgementSet if endToEndAcknowledgementsEnabled is true.
     */
    public AcknowledgementSet createAcknowledgementSet(final String queueUrl,
                                                       final AcknowledgementSetManager acknowledgementSetManager,
                                                       final List<DeleteMessageBatchRequestEntry> waitingForAcknowledgements) {
        AcknowledgementSet acknowledgementSet = null;
            acknowledgementSet = acknowledgementSetManager.create(result -> {
                sqsMetrics.getAcknowledgementSetCallbackCounter().increment();
                if (result == true) {
                    deleteMessagesFromQueue(waitingForAcknowledgements,queueUrl);
                }
            }, END_TO_END_ACK_TIME_OUT);
        return acknowledgementSet;
    }
}
