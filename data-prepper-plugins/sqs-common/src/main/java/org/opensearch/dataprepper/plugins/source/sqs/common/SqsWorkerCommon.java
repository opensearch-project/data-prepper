/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.sqs.common;

import com.linecorp.armeria.client.retry.Backoff;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.KmsAccessDeniedException;
import software.amazon.awssdk.services.sqs.model.KmsNotFoundException;
import software.amazon.awssdk.services.sqs.model.KmsThrottledException;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;
import software.amazon.awssdk.services.sts.model.StsException;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

public class SqsWorkerCommon {
    private static final Logger LOG = LoggerFactory.getLogger(SqsWorkerCommon.class);
    public static final String ACKNOWLEDGEMENT_SET_CALLACK_METRIC_NAME = "acknowledgementSetCallbackCounter";
    public static final String SQS_MESSAGES_RECEIVED_METRIC_NAME = "sqsMessagesReceived";
    public static final String SQS_MESSAGES_DELETED_METRIC_NAME = "sqsMessagesDeleted";
    public static final String SQS_MESSAGES_FAILED_METRIC_NAME = "sqsMessagesFailed";
    public static final String SQS_MESSAGES_DELETE_FAILED_METRIC_NAME = "sqsMessagesDeleteFailed";
    public static final String SQS_VISIBILITY_TIMEOUT_CHANGED_COUNT_METRIC_NAME = "sqsVisibilityTimeoutChangedCount";
    public static final String SQS_VISIBILITY_TIMEOUT_CHANGE_FAILED_COUNT_METRIC_NAME = "sqsVisibilityTimeoutChangeFailedCount";
    public static final String SQS_MESSAGE_ACCESS_DENIED_METRIC_NAME = "sqsMessagesAccessDenied";
    public static final String SQS_MESSAGE_THROTTLED_METRIC_NAME = "sqsMessagesThrottled";
    public static final String SQS_RESOURCE_NOT_FOUND_METRIC_NAME = "sqsResourceNotFound";

    private final Backoff standardBackoff;
    private final PluginMetrics pluginMetrics;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private volatile boolean isStopped;
    private int failedAttemptCount;
    private final Counter sqsMessagesReceivedCounter;
    private final Counter sqsMessagesDeletedCounter;
    private final Counter sqsMessagesFailedCounter;
    private final Counter sqsMessagesDeleteFailedCounter;
    private final Counter acknowledgementSetCallbackCounter;
    private final Counter sqsVisibilityTimeoutChangedCount;
    private final Counter sqsVisibilityTimeoutChangeFailedCount;
    private final Counter sqsMessageAccessDeniedCounter;
    private final Counter sqsMessageThrottledCounter;
    private final Counter sqsResourceNotFoundCounter;

    public SqsWorkerCommon(final Backoff standardBackoff,
                           final PluginMetrics pluginMetrics,
                           final AcknowledgementSetManager acknowledgementSetManager) {

        this.standardBackoff = standardBackoff;
        this.pluginMetrics = pluginMetrics;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.isStopped = false;
        this.failedAttemptCount = 0;


        sqsMessagesReceivedCounter = pluginMetrics.counter(SQS_MESSAGES_RECEIVED_METRIC_NAME);
        sqsMessagesDeletedCounter = pluginMetrics.counter(SQS_MESSAGES_DELETED_METRIC_NAME);
        sqsMessagesFailedCounter = pluginMetrics.counter(SQS_MESSAGES_FAILED_METRIC_NAME);
        sqsMessagesDeleteFailedCounter = pluginMetrics.counter(SQS_MESSAGES_DELETE_FAILED_METRIC_NAME);
        acknowledgementSetCallbackCounter = pluginMetrics.counter(ACKNOWLEDGEMENT_SET_CALLACK_METRIC_NAME);
        sqsVisibilityTimeoutChangedCount = pluginMetrics.counter(SQS_VISIBILITY_TIMEOUT_CHANGED_COUNT_METRIC_NAME);
        sqsVisibilityTimeoutChangeFailedCount = pluginMetrics.counter(SQS_VISIBILITY_TIMEOUT_CHANGE_FAILED_COUNT_METRIC_NAME);
        sqsMessageAccessDeniedCounter = pluginMetrics.counter(SQS_MESSAGE_ACCESS_DENIED_METRIC_NAME);
        sqsMessageThrottledCounter = pluginMetrics.counter(SQS_MESSAGE_THROTTLED_METRIC_NAME);
        sqsResourceNotFoundCounter = pluginMetrics.counter(SQS_RESOURCE_NOT_FOUND_METRIC_NAME);
    }

    public List<Message> pollSqsMessages(final String queueUrl,
                                         final SqsClient sqsClient,
                                         final Integer maxNumberOfMessages,
                                         final Duration waitTime,
                                         final Duration visibilityTimeout) {
        try {
            final ReceiveMessageRequest request = createReceiveMessageRequest(queueUrl, maxNumberOfMessages, waitTime, visibilityTimeout);
            final List<Message> messages = sqsClient.receiveMessage(request).messages();
            failedAttemptCount = 0;
            if (!messages.isEmpty()) {
                sqsMessagesReceivedCounter.increment(messages.size());
            }
            return messages;
        }
        catch (SqsException | StsException e) {
            LOG.error("Error reading from SQS: {}. Retrying with exponential backoff.", e.getMessage());
            recordSqsException(e);
            applyBackoff();
            return Collections.emptyList();
        }
    }

    private ReceiveMessageRequest createReceiveMessageRequest(String queueUrl, Integer maxNumberOfMessages, Duration waitTime, Duration visibilityTimeout) {
        ReceiveMessageRequest.Builder requestBuilder = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .attributeNamesWithStrings("All")
                .messageAttributeNames("All");

        if (waitTime != null) {
            requestBuilder.waitTimeSeconds((int) waitTime.getSeconds());
        }
        if (maxNumberOfMessages != null) {
            requestBuilder.maxNumberOfMessages(maxNumberOfMessages);
        }
        if (visibilityTimeout != null) {
            requestBuilder.visibilityTimeout((int) visibilityTimeout.getSeconds());
        }
        return requestBuilder.build();
    }

    public void applyBackoff() {
        final long delayMillis = standardBackoff.nextDelayMillis(++failedAttemptCount);
        if (delayMillis < 0) {
            Thread.currentThread().interrupt();
            throw new SqsRetriesExhaustedException("SQS retries exhausted. Check your SQS configuration.");
        }

        final Duration delayDuration = Duration.ofMillis(delayMillis);
        LOG.info("Pausing SQS processing for {}.{} seconds due to an error.",
                delayDuration.getSeconds(), delayDuration.toMillisPart());

        try {
            Thread.sleep(delayMillis);
        } catch (InterruptedException e) {
            LOG.error("Thread interrupted during SQS backoff sleep.", e);
            Thread.currentThread().interrupt();
        }
    }

    public void deleteSqsMessages(final String queueUrl, final SqsClient sqsClient, final List<DeleteMessageBatchRequestEntry> entries) {
        if (entries == null || entries.isEmpty() || isStopped) {
            return;
        }

        try {
            final DeleteMessageBatchRequest request = DeleteMessageBatchRequest.builder()
                    .queueUrl(queueUrl)
                    .entries(entries)
                    .build();

            final DeleteMessageBatchResponse response = sqsClient.deleteMessageBatch(request);
            if (response.hasSuccessful()) {
                final int successCount = response.successful().size();
                sqsMessagesDeletedCounter.increment(successCount);
                LOG.debug("Deleted {} messages from SQS queue [{}]", successCount, queueUrl);
            }
            if (response.hasFailed()) {
                final int failCount = response.failed().size();
                sqsMessagesDeleteFailedCounter.increment(failCount);
                LOG.error("Failed to delete {} messages from SQS queue [{}].", failCount, queueUrl);
            }
        } catch (SdkException e) {
            sqsMessagesDeleteFailedCounter.increment(entries.size());
            LOG.error("Failed to delete messages from SQS queue [{}]: {}", queueUrl, e.getMessage());
        }
    }

    public void increaseVisibilityTimeout(final String queueUrl,
                                          final SqsClient sqsClient,
                                          final String receiptHandle,
                                          final int newVisibilityTimeoutSeconds,
                                          final String messageIdForLogging) {
        if (isStopped) {
            LOG.info("Skipping visibility timeout extension because worker is stopping. ID: {}", messageIdForLogging);
            return;
        }

        try {
            ChangeMessageVisibilityRequest request = ChangeMessageVisibilityRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(receiptHandle)
                    .visibilityTimeout(newVisibilityTimeoutSeconds)
                    .build();

            sqsClient.changeMessageVisibility(request);
            sqsVisibilityTimeoutChangedCount.increment();
            LOG.debug("Set visibility timeout for message {} to {} seconds", messageIdForLogging, newVisibilityTimeoutSeconds);
        }
        catch (Exception e) {
            sqsVisibilityTimeoutChangeFailedCount.increment();
            LOG.error("Failed to set visibility timeout for message {} to {}. Reason: {}",
                    messageIdForLogging, newVisibilityTimeoutSeconds, e.getMessage());
        }
    }

    public DeleteMessageBatchRequestEntry buildDeleteMessageBatchRequestEntry(final String messageId,
                                                                              final String receiptHandle) {
        return DeleteMessageBatchRequestEntry.builder()
                .id(messageId)
                .receiptHandle(receiptHandle)
                .build();
    }

    public Timer createTimer(final String timerName) {
        return pluginMetrics.timer(timerName);
    }

    public Counter getSqsMessagesFailedCounter() {
        return sqsMessagesFailedCounter;
    }

    public Counter getSqsMessagesDeletedCounter() {
        return sqsMessagesDeletedCounter;
    }

    public void stop() {
        isStopped = true;
    }

    public void recordSqsException(final AwsServiceException e) {
        // AWS SQS emits some of their exceptions without the matching HTTP code. As we want to generate an aggregate version of
        // these exceptions, we have to explicitly catch the type alongside the status code for the ones that leverage the status
        // code (i.e. InvalidAddressException)
        if (e.statusCode() == 403 ||
                e instanceof KmsAccessDeniedException) {
            sqsMessageAccessDeniedCounter.increment();
        } else if (e.statusCode() == 404 ||
                e instanceof QueueDoesNotExistException ||
                e instanceof KmsNotFoundException) {
            sqsResourceNotFoundCounter.increment();
        } else if (e.isThrottlingException() ||
                e instanceof KmsThrottledException) {
            sqsMessageThrottledCounter.increment();
        }
    }
}
