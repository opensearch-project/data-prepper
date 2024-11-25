/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.sqs;

import com.linecorp.armeria.client.retry.Backoff;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResultEntry;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;
import software.amazon.awssdk.services.sts.model.StsException;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;


public class SqsWorker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(SqsWorker.class);
    static final String SQS_MESSAGES_RECEIVED_METRIC_NAME = "sqsMessagesReceived";
    static final String SQS_MESSAGES_DELETED_METRIC_NAME = "sqsMessagesDeleted";
    static final String SQS_MESSAGES_FAILED_METRIC_NAME = "sqsMessagesFailed";
    static final String SQS_MESSAGES_DELETE_FAILED_METRIC_NAME = "sqsMessagesDeleteFailed";
    static final String SQS_MESSAGE_DELAY_METRIC_NAME = "sqsMessageDelay";
    static final String SQS_VISIBILITY_TIMEOUT_CHANGED_COUNT_METRIC_NAME = "sqsVisibilityTimeoutChangedCount";
    static final String SQS_VISIBILITY_TIMEOUT_CHANGE_FAILED_COUNT_METRIC_NAME = "sqsVisibilityTimeoutChangeFailedCount";
    static final String ACKNOWLEDGEMENT_SET_CALLACK_METRIC_NAME = "acknowledgementSetCallbackCounter";

    private final SqsClient sqsClient;
    private final SqsEventProcessor sqsEventProcessor;
    private final Counter sqsMessagesReceivedCounter;
    private final Counter sqsMessagesDeletedCounter;
    private final Counter sqsMessagesFailedCounter;
    private final Counter sqsMessagesDeleteFailedCounter;
    private final Counter acknowledgementSetCallbackCounter;
    private final Counter sqsVisibilityTimeoutChangedCount;
    private final Counter sqsVisibilityTimeoutChangeFailedCount;
    private final Timer sqsMessageDelayTimer;
    private final Backoff standardBackoff;
    private final QueueConfig queueConfig;
    private int failedAttemptCount;
    private final boolean endToEndAcknowledgementsEnabled;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private volatile boolean isStopped = false;
    private final BufferAccumulator<Record<Event>> bufferAccumulator;
    private Map<Message, Integer> messageVisibilityTimesMap;

    public SqsWorker(final BufferAccumulator<Record<Event>> bufferAccumulator,
                     final AcknowledgementSetManager acknowledgementSetManager,
                     final SqsClient sqsClient,
                     final SqsEventProcessor sqsEventProcessor,
                     final SqsSourceConfig sqsSourceConfig,
                     final QueueConfig queueConfig,
                     final PluginMetrics pluginMetrics,
                     final Backoff backoff) {

        this.bufferAccumulator = bufferAccumulator;
        this.sqsClient = sqsClient;
        this.sqsEventProcessor = sqsEventProcessor;
        this.queueConfig = queueConfig;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.standardBackoff = backoff;
        this.endToEndAcknowledgementsEnabled = sqsSourceConfig.getAcknowledgements();
        messageVisibilityTimesMap = new HashMap<>();

        failedAttemptCount = 0;

        sqsMessagesReceivedCounter = pluginMetrics.counter(SQS_MESSAGES_RECEIVED_METRIC_NAME);
        sqsMessagesDeletedCounter = pluginMetrics.counter(SQS_MESSAGES_DELETED_METRIC_NAME);
        sqsMessagesFailedCounter = pluginMetrics.counter(SQS_MESSAGES_FAILED_METRIC_NAME);
        sqsMessagesDeleteFailedCounter = pluginMetrics.counter(SQS_MESSAGES_DELETE_FAILED_METRIC_NAME);
        sqsMessageDelayTimer = pluginMetrics.timer(SQS_MESSAGE_DELAY_METRIC_NAME);
        acknowledgementSetCallbackCounter = pluginMetrics.counter(ACKNOWLEDGEMENT_SET_CALLACK_METRIC_NAME);
        sqsVisibilityTimeoutChangedCount = pluginMetrics.counter(SQS_VISIBILITY_TIMEOUT_CHANGED_COUNT_METRIC_NAME);
        sqsVisibilityTimeoutChangeFailedCount = pluginMetrics.counter(SQS_VISIBILITY_TIMEOUT_CHANGE_FAILED_COUNT_METRIC_NAME);
    }

    @Override
    public void run() {
        while (!isStopped) {
            int messagesProcessed = 0;
            try {
                messagesProcessed = processSqsMessages();

            } catch (final Exception e) {
                LOG.error("Unable to process SQS messages. Processing error due to: {}", e.getMessage());
                // There shouldn't be any exceptions caught here, but added backoff just to control the amount of logging in case of an exception is thrown.
                applyBackoff();
            }

            if (messagesProcessed > 0 && queueConfig.getPollDelay().toMillis() > 0) {
                try {
                    Thread.sleep(queueConfig.getPollDelay().toMillis());
                } catch (final InterruptedException e) {
                    LOG.error("Thread is interrupted while polling SQS.", e);
                }
            }
        }
    }

    int processSqsMessages() {
        final List<Message> messages = getMessagesFromSqs();
        if (!messages.isEmpty()) {
            sqsMessagesReceivedCounter.increment(messages.size());
            final List<DeleteMessageBatchRequestEntry> deleteMessageBatchRequestEntries = processSqsEvents(messages);
            if (!deleteMessageBatchRequestEntries.isEmpty()) {
                deleteSqsMessages(deleteMessageBatchRequestEntries);
            }
        }
        return messages.size();
    }

    private List<Message> getMessagesFromSqs() {
        try {
            final ReceiveMessageRequest request = createReceiveMessageRequest();
            final List<Message> messages = sqsClient.receiveMessage(request).messages();
            failedAttemptCount = 0;
            return messages;
        } catch (final SqsException | StsException e) {
            LOG.error("Error reading from SQS: {}. Retrying with exponential backoff.", e.getMessage());
            applyBackoff();
            return Collections.emptyList();
        }
    }

    private void applyBackoff() {
        final long delayMillis = standardBackoff.nextDelayMillis(++failedAttemptCount);
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
        }
    }


    private ReceiveMessageRequest createReceiveMessageRequest() {
        return ReceiveMessageRequest.builder()
                .queueUrl(queueConfig.getUrl())
                .maxNumberOfMessages(queueConfig.getMaximumMessages())
                .visibilityTimeout((int) queueConfig.getVisibilityTimeout().getSeconds())
                .waitTimeSeconds((int) queueConfig.getWaitTime().getSeconds())
                .build();
    }

    private List<DeleteMessageBatchRequestEntry> processSqsEvents(final List<Message> messages) {
        final List<DeleteMessageBatchRequestEntry> deleteMessageBatchRequestEntryCollection = new ArrayList<>();
        final Map<Message, AcknowledgementSet> messageAcknowledgementSetMap = new HashMap<>();
        final Map<Message, List<DeleteMessageBatchRequestEntry>> messageWaitingForAcknowledgementsMap = new HashMap<>();
        
        for (Message message : messages) {
            List<DeleteMessageBatchRequestEntry> waitingForAcknowledgements = new ArrayList<>();
            AcknowledgementSet acknowledgementSet = null;
            final int visibilityTimeout = (int)queueConfig.getVisibilityTimeout().getSeconds();
            final int maxVisibilityTimeout = (int)queueConfig.getVisibilityDuplicateProtectionTimeout().getSeconds();
            final int progressCheckInterval = visibilityTimeout/2 - 1;
            if (endToEndAcknowledgementsEnabled) {
                int expiryTimeout = visibilityTimeout - 2;
                final boolean visibilityDuplicateProtectionEnabled = queueConfig.getVisibilityDuplicateProtection();
                if (visibilityDuplicateProtectionEnabled) {
                    expiryTimeout = maxVisibilityTimeout;
                }
                acknowledgementSet = acknowledgementSetManager.create(
                    (result) -> {
                        acknowledgementSetCallbackCounter.increment();
                        // Delete only if this is positive acknowledgement
                        if (visibilityDuplicateProtectionEnabled) {
                            messageVisibilityTimesMap.remove(message);
                        }
                        if (result == true) {
                            deleteSqsMessages(waitingForAcknowledgements);
                        }
                    },
                    Duration.ofSeconds(expiryTimeout));
                if (visibilityDuplicateProtectionEnabled) {
                    acknowledgementSet.addProgressCheck(
                        (ratio) -> {
                            final int newVisibilityTimeoutSeconds = visibilityTimeout;
                            int newValue = messageVisibilityTimesMap.getOrDefault(message, visibilityTimeout) + progressCheckInterval;
                            if (newValue >= maxVisibilityTimeout) {
                                return;
                            }
                            messageVisibilityTimesMap.put(message, newValue);
                            increaseVisibilityTimeout(message, newVisibilityTimeoutSeconds);
                        },
                        Duration.ofSeconds(progressCheckInterval));
                }
                messageAcknowledgementSetMap.put(message, acknowledgementSet);
                messageWaitingForAcknowledgementsMap.put(message, waitingForAcknowledgements);
            }
        }
        
        if (endToEndAcknowledgementsEnabled) {
            LOG.debug("Created acknowledgement sets for {} messages.", messages.size());
        }

        for (Message message : messages) {
            final AcknowledgementSet acknowledgementSet = messageAcknowledgementSetMap.get(message);
            final List<DeleteMessageBatchRequestEntry> waitingForAcknowledgements = messageWaitingForAcknowledgementsMap.get(message);
            final Optional<DeleteMessageBatchRequestEntry> deleteMessageBatchRequestEntry = processSqsObject(message, acknowledgementSet);
            if (endToEndAcknowledgementsEnabled) {
                deleteMessageBatchRequestEntry.ifPresent(waitingForAcknowledgements::add);
                acknowledgementSet.complete();
            } else {
                deleteMessageBatchRequestEntry.ifPresent(deleteMessageBatchRequestEntryCollection::add);
            }
        }         

        return deleteMessageBatchRequestEntryCollection;
    }


    private Optional<DeleteMessageBatchRequestEntry> processSqsObject(
            final Message message,
            final AcknowledgementSet acknowledgementSet) {
        try {
            sqsEventProcessor.addSqsObject(message, queueConfig.getUrl(), bufferAccumulator, acknowledgementSet);
            // TODO: see implementation in s3
            return Optional.of(buildDeleteMessageBatchRequestEntry(message));
        } catch (final Exception e) {
            LOG.error("Error processing from SQS: {}. Retrying with exponential backoff.", e.getMessage());
            applyBackoff();
            return Optional.empty();
        }
    }

    private void increaseVisibilityTimeout(final Message message, final int newVisibilityTimeoutSeconds) {
        if(isStopped) {
            LOG.info("Some messages are pending completion of acknowledgments. Data Prepper will not increase the visibility timeout because it is shutting down. {}", message);
            return;
        }
        final ChangeMessageVisibilityRequest changeMessageVisibilityRequest = ChangeMessageVisibilityRequest.builder()
                .visibilityTimeout(newVisibilityTimeoutSeconds)
                .queueUrl(queueConfig.getUrl())
                .receiptHandle(message.receiptHandle()) 
                .build();

        try {
            sqsClient.changeMessageVisibility(changeMessageVisibilityRequest);
            sqsVisibilityTimeoutChangedCount.increment();
            LOG.debug("Set visibility timeout for message {} to {}", message.messageId(), newVisibilityTimeoutSeconds);
        } catch (Exception e) {
            LOG.error("Failed to set visibility timeout for message {} to {}", message.messageId(), newVisibilityTimeoutSeconds, e);
            sqsVisibilityTimeoutChangeFailedCount.increment();
        }
    }


    private DeleteMessageBatchRequestEntry buildDeleteMessageBatchRequestEntry(Message message) {
        return DeleteMessageBatchRequestEntry.builder()
                .id(message.messageId())
                .receiptHandle(message.receiptHandle())
                .build();
    }

    private void deleteSqsMessages(final List<DeleteMessageBatchRequestEntry> deleteEntries) {
        if (deleteEntries.isEmpty()) return;

        try {
            DeleteMessageBatchRequest deleteRequest = DeleteMessageBatchRequest.builder()
                    .queueUrl(queueConfig.getUrl())
                    .entries(deleteEntries)
                    .build();
            DeleteMessageBatchResponse response = sqsClient.deleteMessageBatch(deleteRequest);

            if (response.hasSuccessful()) {
                int successfulDeletes = response.successful().size();
                sqsMessagesDeletedCounter.increment(successfulDeletes);
            }
            if (response.hasFailed()) {
                int failedDeletes = response.failed().size();
                sqsMessagesDeleteFailedCounter.increment(failedDeletes);
                LOG.error("Failed to delete {} messages from SQS.", failedDeletes);
            }
        } catch (SdkException e) {
            LOG.error("Failed to delete messages from SQS: {}", e.getMessage());
            sqsMessagesDeleteFailedCounter.increment(deleteEntries.size());
        }
    }

    void stop() {
        isStopped = true;
    }
}