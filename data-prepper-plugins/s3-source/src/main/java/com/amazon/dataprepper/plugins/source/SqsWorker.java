/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.plugins.source.configuration.OnErrorOption;
import com.amazon.dataprepper.plugins.source.configuration.SqsOptions;
import com.amazon.dataprepper.plugins.source.filter.ObjectCreatedFilter;
import com.amazon.dataprepper.plugins.source.filter.S3EventFilter;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.event.S3EventNotification;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class SqsWorker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(SqsWorker.class);
    static final String SQS_MESSAGES_RECEIVED_METRIC_NAME = "sqsMessagesReceived";
    static final String SQS_MESSAGES_DELETED_METRIC_NAME = "sqsMessagesDeleted";
    static final String SQS_MESSAGES_FAILED_METRIC_NAME = "sqsMessagesFailed";
    static final String SQS_MESSAGE_DELAY_METRIC_NAME = "sqsMessageDelay";

    private final S3SourceConfig s3SourceConfig;
    private final SqsClient sqsClient;
    private final S3Service s3Service;
    private final SqsOptions sqsOptions;
    private final S3EventFilter objectCreatedFilter;
    private final Counter sqsMessagesReceivedCounter;
    private final Counter sqsMessagesDeletedCounter;
    private final Counter sqsMessagesFailedCounter;
    private final Timer sqsMessageDelayTimer;

    public SqsWorker(final SqsClient sqsClient,
                     final S3Service s3Service,
                     final S3SourceConfig s3SourceConfig,
                     final PluginMetrics pluginMetrics) {
        this.sqsClient = sqsClient;
        this.s3Service = s3Service;
        this.s3SourceConfig = s3SourceConfig;
        sqsOptions = s3SourceConfig.getSqsOptions();
        objectCreatedFilter = new ObjectCreatedFilter();

        sqsMessagesReceivedCounter = pluginMetrics.counter(SQS_MESSAGES_RECEIVED_METRIC_NAME);
        sqsMessagesDeletedCounter = pluginMetrics.counter(SQS_MESSAGES_DELETED_METRIC_NAME);
        sqsMessagesFailedCounter = pluginMetrics.counter(SQS_MESSAGES_FAILED_METRIC_NAME);
        sqsMessageDelayTimer = pluginMetrics.timer(SQS_MESSAGE_DELAY_METRIC_NAME);
    }

    @Override
    public void run() {

        while(true) {
            final int messagesProcessed = processSqsMessages();

            if (messagesProcessed > 0 && s3SourceConfig.getSqsOptions().getPollDelay().toMillis() > 0) {
                try {
                    Thread.sleep(s3SourceConfig.getSqsOptions().getPollDelay().toMillis());
                } catch (InterruptedException e) {
                    LOG.error("Thread is interrupted while polling SQS.", e);
                }
            }
        }
    }

    int processSqsMessages() {
        final List<Message> sqsMessages = getMessagesFromSqs();
        if (!sqsMessages.isEmpty())
            sqsMessagesReceivedCounter.increment(sqsMessages.size());

        // convert each message to S3EventNotificationRecord
        // TODO: eliminate map with extended S3ObjectReference classes
        final Map<Message, List<S3EventNotification.S3EventNotificationRecord>> s3EventNotificationRecords =
                getS3MessageEventNotificationRecordMap(sqsMessages);

        // build s3ObjectReference from S3EventNotificationRecord if event name starts with ObjectCreated
        final List<DeleteMessageBatchRequestEntry> deleteMessageBatchRequestEntries = processS3EventNotificationRecords(s3EventNotificationRecords);

        // delete sqs messages
        if (!deleteMessageBatchRequestEntries.isEmpty()) {
            deleteSqsMessages(deleteMessageBatchRequestEntries);
        }

        return sqsMessages.size();
    }

    private List<Message> getMessagesFromSqs() {
        try {
            final ReceiveMessageRequest receiveMessageRequest = createReceiveMessageRequest();
            return sqsClient.receiveMessage(receiveMessageRequest).messages();
        } catch (SqsException e) {
            LOG.error("Error reading from SQS: {}", e.getMessage());
            return Collections.emptyList();
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

    private Map<Message, List<S3EventNotification.S3EventNotificationRecord>> getS3MessageEventNotificationRecordMap(final List<Message> sqsMessages) {
        return sqsMessages.stream().collect(Collectors.toMap(message -> message, this::convertS3EventMessages));
    }

    private List<S3EventNotification.S3EventNotificationRecord> convertS3EventMessages(final Message message) {
        try {
            final S3EventNotification s3EventNotification = S3EventNotification.parseJson(message.body());
            if (s3EventNotification.getRecords() != null)
                return s3EventNotification.getRecords();
            else {
                LOG.debug("S3 notification message with ID:{} cannot be parsed into S3EventNotificationRecord.", message.messageId());
                return Collections.emptyList();
            }

        } catch (SdkClientException e) {
            LOG.error("Invalid JSON string in message body of {}", message.messageId(), e);
        }
        return Collections.emptyList();
    }

    private List<DeleteMessageBatchRequestEntry> processS3EventNotificationRecords(final Map<Message, List<S3EventNotification.S3EventNotificationRecord>> s3EventNotificationRecords) {
        List<DeleteMessageBatchRequestEntry> deleteMessageBatchRequestEntryCollection = new ArrayList<>();
        for (Map.Entry<Message, List<S3EventNotification.S3EventNotificationRecord>> entry: s3EventNotificationRecords.entrySet()) {
            if (entry.getValue().isEmpty()) {
                sqsMessagesFailedCounter.increment();
                if (s3SourceConfig.getOnErrorOption().equals(OnErrorOption.DELETE_MESSAGES)) {
                    deleteMessageBatchRequestEntryCollection.add(buildDeleteMessageBatchRequestEntry(entry.getKey()));
                }
            }
            else if (isEventNameCreated(entry.getValue().get(0))) {
                final S3ObjectReference s3ObjectReference = populateS3Reference(entry.getValue().get(0));
                final Optional<DeleteMessageBatchRequestEntry> deleteMessageBatchRequestEntry = processS3Object(entry, s3ObjectReference);
                deleteMessageBatchRequestEntry.ifPresent(deleteMessageBatchRequestEntryCollection::add);
            }
            else {
                // Add SQS message to delete collection if the eventName is not ObjectCreated
                deleteMessageBatchRequestEntryCollection.add(buildDeleteMessageBatchRequestEntry(entry.getKey()));
            }
        }
        return deleteMessageBatchRequestEntryCollection;
    }

    private Optional<DeleteMessageBatchRequestEntry> processS3Object(
            final Map.Entry<Message, List<S3EventNotification.S3EventNotificationRecord>> entry,
            final S3ObjectReference s3ObjectReference) {
        Optional<DeleteMessageBatchRequestEntry> deleteMessageBatchRequestEntry = Optional.empty();
        // SQS messages won't be deleted if we are unable to process S3Objects because of S3Exception: Access Denied
        try {
            s3Service.addS3Object(s3ObjectReference);
            deleteMessageBatchRequestEntry = Optional.of(buildDeleteMessageBatchRequestEntry(entry.getKey()));
            sqsMessageDelayTimer.record(Duration.between(
                    Instant.ofEpochMilli(entry.getValue().get(0).getEventTime().toInstant().getMillis()),
                    Instant.now()
            ));
        } catch (final Exception e) {
            LOG.warn("Unable to process S3Object: s3ObjectReference={}.", s3ObjectReference, e);
        }
        return deleteMessageBatchRequestEntry;
    }

    private void deleteSqsMessages(final List<DeleteMessageBatchRequestEntry> deleteMessageBatchRequestEntryCollection) {
        final DeleteMessageBatchRequest deleteMessageBatchRequest = buildDeleteMessageBatchRequest(deleteMessageBatchRequestEntryCollection);
        final DeleteMessageBatchResponse deleteMessageBatchResponse = sqsClient.deleteMessageBatch(deleteMessageBatchRequest);
        if (deleteMessageBatchResponse.hasSuccessful()) {
            final int deletedMessagesCount = deleteMessageBatchResponse.successful().size();
            sqsMessagesDeletedCounter.increment(deletedMessagesCount);
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
                s3Entity.getObject().getKey())
                .build();
    }
}
