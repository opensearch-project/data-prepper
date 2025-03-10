/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3;

import com.linecorp.armeria.client.retry.Backoff;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.plugins.source.s3.configuration.NotificationSourceOption;
import org.opensearch.dataprepper.plugins.source.s3.configuration.OnErrorOption;
import org.opensearch.dataprepper.plugins.source.s3.configuration.SqsOptions;
import org.opensearch.dataprepper.plugins.source.s3.exception.SqsRetriesExhaustedException;
import org.opensearch.dataprepper.plugins.source.s3.filter.EventBridgeObjectCreatedFilter;
import org.opensearch.dataprepper.plugins.source.s3.filter.S3EventFilter;
import org.opensearch.dataprepper.plugins.source.s3.filter.S3ObjectCreatedFilter;
import org.opensearch.dataprepper.plugins.source.s3.parser.ParsedMessage;
import org.opensearch.dataprepper.plugins.source.s3.parser.SqsMessageParser;
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
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;
import software.amazon.awssdk.services.sts.model.StsException;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class SqsWorker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(SqsWorker.class);
    static final String SQS_MESSAGES_RECEIVED_METRIC_NAME = "sqsMessagesReceived";
    static final String SQS_MESSAGES_DELETED_METRIC_NAME = "sqsMessagesDeleted";
    static final String SQS_MESSAGES_FAILED_METRIC_NAME = "sqsMessagesFailed";
    static final String SQS_MESSAGES_DELETE_FAILED_METRIC_NAME = "sqsMessagesDeleteFailed";
    static final String S3_OBJECTS_EMPTY_METRIC_NAME = "s3ObjectsEmpty";
    static final String SQS_MESSAGE_DELAY_METRIC_NAME = "sqsMessageDelay";
    static final String SQS_VISIBILITY_TIMEOUT_CHANGED_COUNT_METRIC_NAME = "sqsVisibilityTimeoutChangedCount";
    static final String SQS_VISIBILITY_TIMEOUT_CHANGE_FAILED_COUNT_METRIC_NAME = "sqsVisibilityTimeoutChangeFailedCount";
    static final String ACKNOWLEDGEMENT_SET_CALLACK_METRIC_NAME = "acknowledgementSetCallbackCounter";

    private final S3SourceConfig s3SourceConfig;
    private final SqsClient sqsClient;
    private final S3Service s3Service;
    private final SqsOptions sqsOptions;
    private final S3EventFilter objectCreatedFilter;
    private final S3EventFilter evenBridgeObjectCreatedFilter;
    private final Counter sqsMessagesReceivedCounter;
    private final Counter sqsMessagesDeletedCounter;
    private final Counter sqsMessagesFailedCounter;
    private final Counter s3ObjectsEmptyCounter;
    private final Counter sqsMessagesDeleteFailedCounter;
    private final Counter acknowledgementSetCallbackCounter;
    private final Counter sqsVisibilityTimeoutChangedCount;
    private final Counter sqsVisibilityTimeoutChangeFailedCount;
    private final Timer sqsMessageDelayTimer;
    private final Backoff standardBackoff;
    private final SqsMessageParser sqsMessageParser;
    private int failedAttemptCount;
    private final boolean endToEndAcknowledgementsEnabled;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private volatile boolean isStopped = false;
    private Map<ParsedMessage, Integer> parsedMessageVisibilityTimesMap;

    public SqsWorker(final AcknowledgementSetManager acknowledgementSetManager,
                     final SqsClient sqsClient,
                     final S3Service s3Service,
                     final S3SourceConfig s3SourceConfig,
                     final PluginMetrics pluginMetrics,
                     final Backoff backoff) {
        this.sqsClient = sqsClient;
        this.s3Service = s3Service;
        this.s3SourceConfig = s3SourceConfig;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.standardBackoff = backoff;
        this.endToEndAcknowledgementsEnabled = s3SourceConfig.getAcknowledgements();
        sqsOptions = s3SourceConfig.getSqsOptions();
        objectCreatedFilter = new S3ObjectCreatedFilter();
        evenBridgeObjectCreatedFilter = new EventBridgeObjectCreatedFilter();
        sqsMessageParser = new SqsMessageParser(s3SourceConfig);
        failedAttemptCount = 0;
        parsedMessageVisibilityTimesMap = new HashMap<>();
        sqsMessagesReceivedCounter = pluginMetrics.counter(SQS_MESSAGES_RECEIVED_METRIC_NAME);
        sqsMessagesDeletedCounter = pluginMetrics.counter(SQS_MESSAGES_DELETED_METRIC_NAME);
        sqsMessagesFailedCounter = pluginMetrics.counter(SQS_MESSAGES_FAILED_METRIC_NAME);
        s3ObjectsEmptyCounter = pluginMetrics.counter(S3_OBJECTS_EMPTY_METRIC_NAME);
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

            if (messagesProcessed > 0 && s3SourceConfig.getSqsOptions().getPollDelay().toMillis() > 0) {
                try {
                    Thread.sleep(s3SourceConfig.getSqsOptions().getPollDelay().toMillis());
                } catch (final InterruptedException e) {
                    LOG.error("Thread is interrupted while polling SQS.", e);
                }
            }
        }
    }

    int processSqsMessages() {
        final List<Message> sqsMessages = getMessagesFromSqs();
        if (!sqsMessages.isEmpty()) {
            sqsMessagesReceivedCounter.increment(sqsMessages.size());

            final Collection<ParsedMessage> s3MessageEventNotificationRecords = sqsMessageParser.parseSqsMessages(sqsMessages);

            // build s3ObjectReference from S3EventNotificationRecord if event name starts with ObjectCreated
            final List<DeleteMessageBatchRequestEntry> deleteMessageBatchRequestEntries = processS3EventNotificationRecords(s3MessageEventNotificationRecords);

            // delete sqs messages
            if (!deleteMessageBatchRequestEntries.isEmpty()) {
                deleteSqsMessages(deleteMessageBatchRequestEntries);
            }
        }
        return sqsMessages.size();
    }

    private List<Message> getMessagesFromSqs() {
        try {
            final ReceiveMessageRequest receiveMessageRequest = createReceiveMessageRequest();
            final List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).messages();
            failedAttemptCount = 0;
            if (messages.isEmpty()) {
                sqsMessageDelayTimer.record(Duration.ZERO);
            }
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
                .queueUrl(sqsOptions.getSqsUrl())
                .maxNumberOfMessages(sqsOptions.getMaximumMessages())
                .visibilityTimeout((int) sqsOptions.getVisibilityTimeout().getSeconds())
                .waitTimeSeconds((int) sqsOptions.getWaitTime().getSeconds())
                .attributeNamesWithStrings(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT.toString())
                .build();
    }

    private List<DeleteMessageBatchRequestEntry> processS3EventNotificationRecords(final Collection<ParsedMessage> s3EventNotificationRecords) {
        final List<DeleteMessageBatchRequestEntry> deleteMessageBatchRequestEntryCollection = new ArrayList<>();
        final List<ParsedMessage> parsedMessagesToRead = new ArrayList<>();
        final Map<ParsedMessage, AcknowledgementSet> messageAcknowledgementSetMap = new HashMap<>();
        final Map<ParsedMessage, List<DeleteMessageBatchRequestEntry>> messageWaitingForAcknowledgementsMap = new HashMap<>();
        final Map<ParsedMessage, List<S3ObjectReference>> messagesWaitingForS3ObjectDeletion = new HashMap<>();
        for (ParsedMessage parsedMessage : s3EventNotificationRecords) {
            if (parsedMessage.isFailedParsing()) {
                sqsMessagesFailedCounter.increment();
                if (s3SourceConfig.getOnErrorOption().equals(OnErrorOption.DELETE_MESSAGES)) {
                    deleteMessageBatchRequestEntryCollection.add(buildDeleteMessageBatchRequestEntry(parsedMessage.getMessage()));
                }
            } else if (parsedMessage.getObjectSize() == 0L) {
                s3ObjectsEmptyCounter.increment();
                LOG.debug("Received empty S3 object: {} in the SQS message. " +
                                "The S3 object is skipped and the SQS message will be deleted.",
                        parsedMessage.getObjectKey());
                deleteMessageBatchRequestEntryCollection.add(buildDeleteMessageBatchRequestEntry(parsedMessage.getMessage()));
            } else {
                if (s3SourceConfig.getNotificationSource().equals(NotificationSourceOption.S3)
                        && !parsedMessage.isEmptyNotification()
                        && isS3EventNameCreated(parsedMessage)) {
                    parsedMessagesToRead.add(parsedMessage);
                }
                else if (s3SourceConfig.getNotificationSource().equals(NotificationSourceOption.EVENTBRIDGE)
                        && isEventBridgeEventTypeCreated(parsedMessage)) {
                    parsedMessagesToRead.add(parsedMessage);
                }
                else {
                    // TODO: Delete these only if on_error is configured to delete_messages.
                    LOG.debug("Received SQS message other than s3:ObjectCreated:*. Deleting message from SQS queue.");
                    deleteMessageBatchRequestEntryCollection.add(buildDeleteMessageBatchRequestEntry(parsedMessage.getMessage()));
                }
            }
        }

        LOG.info("Received {} messages from SQS. Processing {} messages.", s3EventNotificationRecords.size(), parsedMessagesToRead.size());
        
        for (ParsedMessage parsedMessage : parsedMessagesToRead) {

            final int approximateReceiveCount = getApproximateReceiveCount(parsedMessage.getMessage());
            if (s3SourceConfig.getSqsOptions().getMaxReceiveAttempts() != null &&
                    approximateReceiveCount > s3SourceConfig.getSqsOptions().getMaxReceiveAttempts()) {
                deleteSqsMessages(List.of(buildDeleteMessageBatchRequestEntry(parsedMessage.getMessage())));
                parsedMessage.setShouldSkipProcessing(true);
                continue;
            }
            if (approximateReceiveCount <= 1) {
                sqsMessageDelayTimer.record(Duration.between(
                        Instant.ofEpochMilli(parsedMessage.getEventTime().toInstant().getMillis()),
                        Instant.now()
                ));
            }
            List<DeleteMessageBatchRequestEntry> waitingForAcknowledgements = new ArrayList<>();
            List<S3ObjectReference> s3ObjectDeletionWaitingForAcknowledgments = new ArrayList<>();
            AcknowledgementSet acknowledgementSet = null;
            final int visibilityTimeout = (int)sqsOptions.getVisibilityTimeout().getSeconds();
            final int maxVisibilityTimeout = (int)sqsOptions.getVisibilityDuplicateProtectionTimeout().getSeconds();
            final int progressCheckInterval = visibilityTimeout/2 - 1;
            if (endToEndAcknowledgementsEnabled) {
                int expiryTimeout = visibilityTimeout - 2;
                final boolean visibilityDuplicateProtectionEnabled = sqsOptions.getVisibilityDuplicateProtection();
                if (visibilityDuplicateProtectionEnabled) {
                    expiryTimeout = maxVisibilityTimeout;
                }
                acknowledgementSet = acknowledgementSetManager.create(
                    (result) -> {
                        acknowledgementSetCallbackCounter.increment();
                        // Delete only if this is positive acknowledgement
                        if (visibilityDuplicateProtectionEnabled) {
                            parsedMessageVisibilityTimesMap.remove(parsedMessage);
                        }
                        if (result == true) {
                            final boolean successfullyDeletedAllMessages = deleteSqsMessages(waitingForAcknowledgements);
                            if (successfullyDeletedAllMessages && s3SourceConfig.isDeleteS3ObjectsOnRead()) {
                                deleteS3Objects(s3ObjectDeletionWaitingForAcknowledgments);
                            }
                        }
                    },
                    Duration.ofSeconds(expiryTimeout));
                if (visibilityDuplicateProtectionEnabled) {
                    acknowledgementSet.addProgressCheck(
                        (ratio) -> {
                            final int newVisibilityTimeoutSeconds = visibilityTimeout;
                            int newValue = parsedMessageVisibilityTimesMap.getOrDefault(parsedMessage, visibilityTimeout) + progressCheckInterval;
                            if (newValue >= maxVisibilityTimeout) {
                                return;
                            }
                            parsedMessageVisibilityTimesMap.put(parsedMessage, newValue);
                            increaseVisibilityTimeout(parsedMessage, newVisibilityTimeoutSeconds);
                        },
                        Duration.ofSeconds(progressCheckInterval));
                }
                messageAcknowledgementSetMap.put(parsedMessage, acknowledgementSet);
                messageWaitingForAcknowledgementsMap.put(parsedMessage, waitingForAcknowledgements);
                messagesWaitingForS3ObjectDeletion.put(parsedMessage, s3ObjectDeletionWaitingForAcknowledgments);
            }
        }
        
        if (endToEndAcknowledgementsEnabled) {
            LOG.debug("Created acknowledgement sets for {} messages.", parsedMessagesToRead.size());
        }
        // Use a separate loop for processing the S3 objects
        for (ParsedMessage parsedMessage : parsedMessagesToRead) {
            if (parsedMessage.isShouldSkipProcessing()) {
                continue;
            }

            final AcknowledgementSet acknowledgementSet = messageAcknowledgementSetMap.get(parsedMessage);
            final List<DeleteMessageBatchRequestEntry> waitingForAcknowledgements = messageWaitingForAcknowledgementsMap.get(parsedMessage);
            final List<S3ObjectReference> s3ObjectDeletionsWaitingForAcknowledgments = messagesWaitingForS3ObjectDeletion.get(parsedMessage);
            final S3ObjectReference s3ObjectReference = populateS3Reference(parsedMessage.getBucketName(), parsedMessage.getObjectKey());
            final Optional<DeleteMessageBatchRequestEntry> deleteMessageBatchRequestEntry = processS3Object(parsedMessage, s3ObjectReference, acknowledgementSet);
            if (endToEndAcknowledgementsEnabled) {
                deleteMessageBatchRequestEntry.ifPresent(waitingForAcknowledgements::add);
                if (deleteMessageBatchRequestEntry.isPresent() && s3SourceConfig.isDeleteS3ObjectsOnRead()) {
                    s3ObjectDeletionsWaitingForAcknowledgments.add(s3ObjectReference);
                }
                acknowledgementSet.complete();
            } else {
                deleteMessageBatchRequestEntry.ifPresent(deleteMessageBatchRequestEntryCollection::add);
            }
        }         

        return deleteMessageBatchRequestEntryCollection;
    }

    private void increaseVisibilityTimeout(final ParsedMessage parsedMessage, final int newVisibilityTimeoutSeconds) {
        if(isStopped) {
            LOG.info("Some messages are pending completion of acknowledgments. Data Prepper will not increase the visibility timeout because it is shutting down. {}", parsedMessage);
            return;
        }
        final ChangeMessageVisibilityRequest changeMessageVisibilityRequest = ChangeMessageVisibilityRequest.builder()
                .visibilityTimeout(newVisibilityTimeoutSeconds)
                .queueUrl(sqsOptions.getSqsUrl())
                .receiptHandle(parsedMessage.getMessage().receiptHandle())
                .build();

        try {
            sqsClient.changeMessageVisibility(changeMessageVisibilityRequest);
            sqsVisibilityTimeoutChangedCount.increment();
            LOG.debug("Set visibility timeout for message {} to {}", parsedMessage.getMessage().messageId(), newVisibilityTimeoutSeconds);
        } catch (Exception e) {
            LOG.error("Failed to set visibility timeout for message {} to {}", parsedMessage.getMessage().messageId(), newVisibilityTimeoutSeconds, e);
            sqsVisibilityTimeoutChangeFailedCount.increment();
        }
    }

    private Optional<DeleteMessageBatchRequestEntry> processS3Object(
            final ParsedMessage parsedMessage,
            final S3ObjectReference s3ObjectReference,
            final AcknowledgementSet acknowledgementSet) {
        // SQS messages won't be deleted if we are unable to process S3Objects because of an exception
        try {
            s3Service.addS3Object(s3ObjectReference, acknowledgementSet);
            return Optional.of(buildDeleteMessageBatchRequestEntry(parsedMessage.getMessage()));
        } catch (final Exception e) {
            LOG.error("Error processing from S3: {}. Retrying with exponential backoff.", e.getMessage());
            applyBackoff();
            return Optional.empty();
        }
    }

    private boolean deleteSqsMessages(final List<DeleteMessageBatchRequestEntry> deleteMessageBatchRequestEntryCollection) {
        if(isStopped)
            return false;
        if (deleteMessageBatchRequestEntryCollection.size() == 0) {
            return false;
        }
        final DeleteMessageBatchRequest deleteMessageBatchRequest = buildDeleteMessageBatchRequest(deleteMessageBatchRequestEntryCollection);
        try {
            final DeleteMessageBatchResponse deleteMessageBatchResponse = sqsClient.deleteMessageBatch(deleteMessageBatchRequest);
            if (deleteMessageBatchResponse.hasSuccessful()) {
                final int deletedMessagesCount = deleteMessageBatchResponse.successful().size();
                if (deletedMessagesCount > 0) {
                    final String successfullyDeletedMessages = deleteMessageBatchResponse.successful().stream()
                            .map(DeleteMessageBatchResultEntry::id)
                            .collect(Collectors.joining(", "));
                    LOG.info("Deleted {} messages from SQS. [{}]", deletedMessagesCount, successfullyDeletedMessages);
                    sqsMessagesDeletedCounter.increment(deletedMessagesCount);
                }
            }
            if(deleteMessageBatchResponse.hasFailed()) {
                final int failedDeleteCount = deleteMessageBatchResponse.failed().size();
                sqsMessagesDeleteFailedCounter.increment(failedDeleteCount);

                if(LOG.isErrorEnabled() && failedDeleteCount > 0) {
                    final String failedMessages = deleteMessageBatchResponse.failed().stream()
                            .map(failed -> failed.toString())
                            .collect(Collectors.joining(", "));
                    LOG.error("Failed to delete {} messages from SQS with errors: [{}].", failedDeleteCount, failedMessages);
                    return false;
                }
            }

        } catch (final SdkException e) {
            final int failedMessageCount = deleteMessageBatchRequestEntryCollection.size();
            sqsMessagesDeleteFailedCounter.increment(failedMessageCount);
            LOG.error("Failed to delete {} messages from SQS due to {}.", failedMessageCount, e.getMessage());
            if(e instanceof StsException) {
                applyBackoff();
            }

            return false;
        }

        return true;
    }

    private void deleteS3Objects(final List<S3ObjectReference> objectsToDelete) {
        for (final S3ObjectReference s3ObjectReference : objectsToDelete) {
            try {
                s3Service.deleteS3Object(s3ObjectReference);
            } catch (final Exception e) {
                LOG.error("Received an exception while attempting to delete object {} from bucket {}: {}",
                        s3ObjectReference.getKey(), s3ObjectReference.getBucketName(), e.getMessage());
            }
        }
    }

    private DeleteMessageBatchRequestEntry buildDeleteMessageBatchRequestEntry(Message message) {
        return DeleteMessageBatchRequestEntry.builder()
                .id(message.messageId())
                .receiptHandle(message.receiptHandle())
                .build();
    }

    private DeleteMessageBatchRequest buildDeleteMessageBatchRequest(List<DeleteMessageBatchRequestEntry> deleteMessageBatchRequestEntryCollection) {
        return DeleteMessageBatchRequest.builder()
                .queueUrl(s3SourceConfig.getSqsOptions().getSqsUrl())
                .entries(deleteMessageBatchRequestEntryCollection)
                .build();
    }

    private boolean isS3EventNameCreated(final ParsedMessage parsedMessage) {
        return objectCreatedFilter.filter(parsedMessage).isPresent();
    }

    private boolean isEventBridgeEventTypeCreated(final ParsedMessage parsedMessage) {
        return evenBridgeObjectCreatedFilter.filter(parsedMessage).isPresent();
    }

    private S3ObjectReference populateS3Reference(final String bucketName, final String objectKey) {
        return S3ObjectReference
                .bucketAndKey(bucketName, objectKey)
                .build();
    }

    private int getApproximateReceiveCount(final Message message) {
        return message.attributes() != null && message.attributes().get(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT) != null ?
                Integer.parseInt(message.attributes().get(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT)) : 0;
    }

    void stop() {
        isStopped = true;
    }
}
