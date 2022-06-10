/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.plugins.source.configuration.SqsOptions;
import com.amazon.dataprepper.plugins.source.filter.S3EventFilter;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.event.S3EventNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class SqsWorker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(SqsWorker.class);

    private final S3SourceConfig s3SourceConfig;
    private final SqsClient sqsClient;
    private final S3Service s3Service;
    private final SqsOptions sqsOptions;
    private final S3EventFilter s3EventFilter;

    public SqsWorker(final SqsClient sqsClient, final S3Service s3Service, final S3SourceConfig s3SourceConfig, final S3EventFilter s3EventFilter) {
        this.s3SourceConfig = s3SourceConfig;
        this.sqsClient = sqsClient;
        this.s3Service = s3Service;
        sqsOptions = s3SourceConfig.getSqsOptions();
        this.s3EventFilter = s3EventFilter;
    }

    @Override
    public void run() {

        while (true) {
            final int messagesProcessed = processSqsMessages();

            if (messagesProcessed < sqsOptions.getMaximumMessages() && s3SourceConfig.getSqsOptions().getPollDelay().toMillis() > 0) {
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

        // convert each message to S3EventNotificationRecord
        // TODO: eliminate map with extended S3ObjectReference classes
        final Map<Message, List<S3EventNotification.S3EventNotificationRecord>> s3EventNotificationRecords =
                getS3MessageEventNotificationRecordMap(sqsMessages);

        // build s3ObjectReference from S3EventNotificationRecord if event name starts with ObjectCreated
        processS3ObjectAndDeleteSqsMessages(s3EventNotificationRecords);

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

    // on_error: delete_from_queue|visibility_expiration
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

    private void processS3ObjectAndDeleteSqsMessages(final Map<Message, List<S3EventNotification.S3EventNotificationRecord>> s3EventNotificationRecords) {
        for (Map.Entry<Message, List<S3EventNotification.S3EventNotificationRecord>> entry : s3EventNotificationRecords.entrySet()) {
            if (entry.getValue().isEmpty()) {
                // TODO: delete or ignore based on configuration
            } else {
                entry.getValue()
                        .stream()
                        .map(s3EventFilter::filter)
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .map(this::populateS3Reference)
                        .forEach(s3Service::addS3Object);

                //final S3ObjectReference s3ObjectReference = populateS3Reference(entry.getValue().get(0));
                //s3Service.addS3Object(s3ObjectReference);
                // TODO: delete sqsMessages which are successfully processed
            }
        }
    }

    private boolean isEventNameCreated(final S3EventNotification.S3EventNotificationRecord s3EventNotificationRecord) {
        return s3EventFilter.filter(s3EventNotificationRecord)
                .isPresent();
    }

    private S3ObjectReference populateS3Reference(final S3EventNotification.S3EventNotificationRecord s3EventNotificationRecord) {
        return S3ObjectReference.fromBucketAndKey(s3EventNotificationRecord.getS3().getBucket().getName(),
                s3EventNotificationRecord.getS3().getObject().getKey());
    }
}
