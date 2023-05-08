/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.linecorp.armeria.client.retry.Backoff;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.source.configuration.OnErrorOption;
import org.opensearch.dataprepper.plugins.source.configuration.SqsOptions;
import org.opensearch.dataprepper.plugins.source.exception.SqsRetriesExhaustedException;
import org.opensearch.dataprepper.plugins.source.filter.ObjectCreatedFilter;
import org.opensearch.dataprepper.plugins.source.filter.S3EventFilter;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResultEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;
import software.amazon.awssdk.services.sts.model.StsException;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class SqsWorker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(SqsWorker.class);
    static final String SQS_MESSAGES_RECEIVED_METRIC_NAME = "sqsMessagesReceived";
    static final String SQS_MESSAGES_DELETED_METRIC_NAME = "sqsMessagesDeleted";
    static final String SQS_MESSAGES_FAILED_METRIC_NAME = "sqsMessagesFailed";
    static final String SQS_MESSAGES_DELETE_FAILED_METRIC_NAME = "sqsMessagesDeleteFailed";
    static final String SQS_MESSAGE_DELAY_METRIC_NAME = "sqsMessageDelay";
    static final String ACKNOWLEDGEMENT_SET_CALLACK_METRIC_NAME = "acknowledgementSetCallbackCounter";

    private final S3SourceConfig s3SourceConfig;
    private final SqsClient sqsClient;
    private final S3Service s3Service;
    private final SqsOptions sqsOptions;
    private final S3EventFilter objectCreatedFilter;
    private final Counter sqsMessagesReceivedCounter;
    private final Counter sqsMessagesDeletedCounter;
    private final Counter sqsMessagesFailedCounter;
    private final Counter sqsMessagesDeleteFailedCounter;
    private final Counter acknowledgementSetCallbackCounter;
    private final Timer sqsMessageDelayTimer;
    private final S3EventMessageParser s3EventMessageParser;
    private final Backoff standardBackoff;
    private int failedAttemptCount;
    private boolean endToEndAcknowledgementsEnabled;
    private final AcknowledgementSetManager acknowledgementSetManager;

    public SqsWorker(final AcknowledgementSetManager acknowledgementSetManager,
                     final SqsClient sqsClient,
                     final S3Service s3Service,
                     final S3SourceConfig s3SourceConfig,
                     final PluginMetrics pluginMetrics,
                     final S3EventMessageParser s3EventMessageParser,
                     final Backoff backoff) {
        this.sqsClient = sqsClient;
        this.s3Service = s3Service;
        this.s3SourceConfig = s3SourceConfig;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.s3EventMessageParser = s3EventMessageParser;
        this.standardBackoff = backoff;
        this.endToEndAcknowledgementsEnabled = s3SourceConfig.getAcknowledgements();
        sqsOptions = s3SourceConfig.getSqsOptions();
        objectCreatedFilter = new ObjectCreatedFilter();
        failedAttemptCount = 0;

        sqsMessagesReceivedCounter = pluginMetrics.counter(SQS_MESSAGES_RECEIVED_METRIC_NAME);
        sqsMessagesDeletedCounter = pluginMetrics.counter(SQS_MESSAGES_DELETED_METRIC_NAME);
        sqsMessagesFailedCounter = pluginMetrics.counter(SQS_MESSAGES_FAILED_METRIC_NAME);
        sqsMessagesDeleteFailedCounter = pluginMetrics.counter(SQS_MESSAGES_DELETE_FAILED_METRIC_NAME);
        sqsMessageDelayTimer = pluginMetrics.timer(SQS_MESSAGE_DELAY_METRIC_NAME);
        acknowledgementSetCallbackCounter = pluginMetrics.counter(ACKNOWLEDGEMENT_SET_CALLACK_METRIC_NAME);
    }

    @Override
    public void run() {

        while (!Thread.currentThread().isInterrupted()) {
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

            final Collection<ParsedMessage> s3MessageEventNotificationRecords = getS3MessageEventNotificationRecords(sqsMessages);

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
                .build();
    }

    private Collection<ParsedMessage> getS3MessageEventNotificationRecords(final List<Message> sqsMessages) {
        return sqsMessages.stream()
                .map(this::convertS3EventMessages)
                .collect(Collectors.toList());
    }

    private ParsedMessage convertS3EventMessages(final Message message) {
        try {
            final S3EventNotification s3EventNotification = s3EventMessageParser.parseMessage(message.body());
            if (s3EventNotification.getRecords() != null)
                return new ParsedMessage(message, s3EventNotification.getRecords());
            else {
                LOG.debug("SQS message with ID:{} does not have any S3 event notification records.", message.messageId());
                return new ParsedMessage(message, true);
            }

        } catch (SdkClientException | JsonProcessingException e) {
            if (message.body().contains("s3:TestEvent") && message.body().contains("Amazon S3")) {
                LOG.info("Received s3:TestEvent message. Deleting from SQS queue.");
                return new ParsedMessage(message, false);
            } else {
                LOG.error("SQS message with message ID:{} has invalid body which cannot be parsed into S3EventNotification. {}.", message.messageId(), e.getMessage());
            }
        }
        return new ParsedMessage(message, true);
    }

    private List<DeleteMessageBatchRequestEntry> processS3EventNotificationRecords(final Collection<ParsedMessage> s3EventNotificationRecords) {
        final List<DeleteMessageBatchRequestEntry> deleteMessageBatchRequestEntryCollection = new ArrayList<>();
        final List<ParsedMessage> parsedMessagesToRead = new ArrayList<>();

        for (ParsedMessage parsedMessage : s3EventNotificationRecords) {
            if (parsedMessage.failedParsing) {
                sqsMessagesFailedCounter.increment();
                if (s3SourceConfig.getOnErrorOption().equals(OnErrorOption.DELETE_MESSAGES)) {
                    deleteMessageBatchRequestEntryCollection.add(buildDeleteMessageBatchRequestEntry(parsedMessage.message));
                }
            } else {
                final List<S3EventNotification.S3EventNotificationRecord> notificationRecords = parsedMessage.notificationRecords;
                if (!notificationRecords.isEmpty() && isEventNameCreated(notificationRecords.get(0))) {
                    parsedMessagesToRead.add(parsedMessage);
                } else {
                    // TODO: Delete these only if on_error is configured to delete_messages.
                    LOG.debug("Received SQS message other than s3:ObjectCreated:*. Deleting message from SQS queue.");
                    deleteMessageBatchRequestEntryCollection.add(buildDeleteMessageBatchRequestEntry(parsedMessage.message));
                }
            }
        }

        LOG.info("Received {} messages from SQS. Processing {} messages.", s3EventNotificationRecords.size(), parsedMessagesToRead.size());

        for (ParsedMessage parsedMessage : parsedMessagesToRead) {
            List<DeleteMessageBatchRequestEntry> waitingForAcknowledgements = new ArrayList<>();
            AcknowledgementSet acknowledgementSet = null;
            if (endToEndAcknowledgementsEnabled) {
                // Acknowledgement Set timeout is slightly smaller than the visibility timeout;
                int timeout = (int) sqsOptions.getVisibilityTimeout().getSeconds() - 2;
                acknowledgementSet = acknowledgementSetManager.create((result) -> {
                    acknowledgementSetCallbackCounter.increment();
                    // Delete only if this is positive acknowledgement
                    if (result == true) {
                        deleteSqsMessages(waitingForAcknowledgements);
                    }
                }, Duration.ofSeconds(timeout));
            }
            final List<S3EventNotification.S3EventNotificationRecord> notificationRecords = parsedMessage.notificationRecords;
            final S3ObjectReference s3ObjectReference = populateS3Reference(notificationRecords.get(0));
            final Optional<DeleteMessageBatchRequestEntry> deleteMessageBatchRequestEntry = processS3Object(parsedMessage, s3ObjectReference, acknowledgementSet);
            if (endToEndAcknowledgementsEnabled) {
                deleteMessageBatchRequestEntry.ifPresent(waitingForAcknowledgements::add);
            } else {
                deleteMessageBatchRequestEntry.ifPresent(deleteMessageBatchRequestEntryCollection::add);
            }
        }

        return deleteMessageBatchRequestEntryCollection;
    }

    private Optional<DeleteMessageBatchRequestEntry> processS3Object(
            final ParsedMessage parsedMessage,
            final S3ObjectReference s3ObjectReference,
            final AcknowledgementSet acknowledgementSet) {
        // SQS messages won't be deleted if we are unable to process S3Objects because of S3Exception: Access Denied
        try {
            s3Service.addS3Object(s3ObjectReference, acknowledgementSet);
            sqsMessageDelayTimer.record(Duration.between(
                    Instant.ofEpochMilli(parsedMessage.notificationRecords.get(0).getEventTime().toInstant().getMillis()),
                    Instant.now()
            ));
            return Optional.of(buildDeleteMessageBatchRequestEntry(parsedMessage.message));
        } catch (final S3Exception | StsException e) {
            LOG.error("Error processing from S3: {}. Retrying with exponential backoff.", e.getMessage());
            applyBackoff();
            return Optional.empty();
        } catch (final Exception e) {
            return Optional.empty();
        }
    }

    private void deleteSqsMessages(final List<DeleteMessageBatchRequestEntry> deleteMessageBatchRequestEntryCollection) {
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

                if(LOG.isErrorEnabled()) {
                    final String failedMessages = deleteMessageBatchResponse.failed().stream()
                            .map(failed -> toString())
                            .collect(Collectors.joining(", "));
                    LOG.error("Failed to delete {} messages from SQS with errors: [{}].", failedDeleteCount, failedMessages);
                }
            }

        } catch (final SdkException e) {
            final int failedMessageCount = deleteMessageBatchRequestEntryCollection.size();
            sqsMessagesDeleteFailedCounter.increment(failedMessageCount);
            LOG.error("Failed to delete {} messages from SQS due to {}.", failedMessageCount, e.getMessage());
            if(e instanceof StsException) {
                applyBackoff();
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

    private boolean isEventNameCreated(final S3EventNotification.S3EventNotificationRecord s3EventNotificationRecord) {
        return objectCreatedFilter.filter(s3EventNotificationRecord)
                .isPresent();
    }

    private S3ObjectReference populateS3Reference(final S3EventNotification.S3EventNotificationRecord s3EventNotificationRecord) {
        final S3EventNotification.S3Entity s3Entity = s3EventNotificationRecord.getS3();
        return S3ObjectReference.bucketAndKey(s3Entity.getBucket().getName(),
                        s3Entity.getObject().getUrlDecodedKey())
                .build();
    }

    private static class ParsedMessage {
        private final Message message;
        private final List<S3EventNotification.S3EventNotificationRecord> notificationRecords;
        private final boolean failedParsing;

        private ParsedMessage(final Message message, final List<S3EventNotification.S3EventNotificationRecord> notificationRecords) {
            this(message, notificationRecords, false);
        }

        private ParsedMessage(final Message message, final boolean failedParsing) {
            this(message, Collections.emptyList(), failedParsing);
        }

        private ParsedMessage(final Message message, final List<S3EventNotification.S3EventNotificationRecord> notificationRecords, final boolean failedParsing) {
            this.message = message;
            this.notificationRecords = notificationRecords;
            this.failedParsing = failedParsing;
        }
    }
}
