/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.aws.sqs.common;

import com.linecorp.armeria.client.retry.Backoff;
import org.opensearch.dataprepper.plugins.aws.sqs.common.exception.SqsRetriesExhaustedException;
import org.opensearch.dataprepper.plugins.aws.sqs.common.metrics.SqsMetrics;
import org.opensearch.dataprepper.plugins.aws.sqs.common.model.SqsOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;
import software.amazon.awssdk.services.sts.model.StsException;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

public class SqsService {
    private static final Logger LOG = LoggerFactory.getLogger(SqsService.class);
    static final long INITIAL_DELAY = Duration.ofSeconds(20).toMillis();
    static final long MAXIMUM_DELAY = Duration.ofMinutes(5).toMillis();
    static final double JITTER_RATE = 0.20;
    private final SqsMetrics sqsMetrics;

    private final SqsClient sqsClient;

    private final Backoff backoff;

    private int failedAttemptCount;

    public SqsService(final SqsMetrics sqsMetrics,
                      final SqsClient sqsClient) {
        this.sqsMetrics = sqsMetrics;
        this.sqsClient = sqsClient;
        this.backoff = Backoff.exponential(INITIAL_DELAY, MAXIMUM_DELAY).withJitter(JITTER_RATE)
                .withMaxAttempts(Integer.MAX_VALUE);
    }

    public ReceiveMessageRequest createReceiveMessageRequest(final SqsOptions sqsOptions) {
        return ReceiveMessageRequest.builder()
                .queueUrl(sqsOptions.getSqsUrl())
                .maxNumberOfMessages(sqsOptions.getMaximumMessages())
                .build();
    }

    public List<Message> getMessagesFromSqs(final SqsOptions sqsOptions) {
        try {
            final ReceiveMessageRequest receiveMessageRequest = createReceiveMessageRequest(sqsOptions);
            final List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).messages();
            return messages;
        } catch (final SqsException | StsException e) {
            LOG.error("Error reading from SQS: {}. Retrying with exponential backoff.", e.getMessage());
            applyBackoff();
            return Collections.emptyList();
        }
    }

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

    public void deleteMessagesFromQueue(final List<DeleteMessageBatchRequestEntry> deleteMsgBatchReqList,
                                        final String queueUrl) {
        try{
            sqsClient.deleteMessageBatch(DeleteMessageBatchRequest.builder()
                    .queueUrl(queueUrl)
                    .entries(deleteMsgBatchReqList)
                    .build());
            sqsMetrics.getSqsMessagesDeletedCounter().increment();
            LOG.info("message deleted successfully : {}",deleteMsgBatchReqList);
        }catch(Exception e){
            sqsMetrics.getSqsMessagesDeleteFailedCounter().increment();
        }
    }
}
