/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.plugins.source.configuration.SqsOptions;
import com.amazon.dataprepper.plugins.source.filter.ObjectCreatedFilter;
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
import java.util.HashMap;

public class SqsWorker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(SqsWorker.class);

    private final S3SourceConfig s3SourceConfig;
    private final SqsClient sqsClient;
    private final S3Service s3Service;
    private final SqsOptions sqsOptions;
    private final S3EventFilter objectCreatedFilter;

    public SqsWorker(final SqsClient sqsClient, final S3Service s3Service, final S3SourceConfig s3SourceConfig) {
        this.s3SourceConfig = s3SourceConfig;
        this.sqsClient = sqsClient;
        this.s3Service = s3Service;
        sqsOptions = s3SourceConfig.getSqsOptions();
        objectCreatedFilter = new ObjectCreatedFilter();
    }

    @Override
    public void run() {

        while(true) {

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
        final Map<Message, S3EventNotification.S3EventNotificationRecord> s3EventNotificationRecords =
                getS3MessageEventNotificationRecordMap(sqsMessages);

        // build s3ObjectReference from S3EventNotificationRecord if event name starts with ObjectCreated
        processS3ObjectAndDeleteSqsMessages(s3EventNotificationRecords);

        return sqsMessages.size();
    }

    List<Message> getMessagesFromSqs() {
        try {
            final ReceiveMessageRequest receiveMessageRequest = createReceiveMessageRequest();
            return sqsClient.receiveMessage(receiveMessageRequest).messages();
        } catch (SqsException e) {
            LOG.error("Error reading from SQS: {}", e.getMessage());
            return Collections.EMPTY_LIST;
        }
    }

    ReceiveMessageRequest createReceiveMessageRequest() {
        return ReceiveMessageRequest.builder()
                .queueUrl(sqsOptions.getSqsUrl())
                .maxNumberOfMessages(sqsOptions.getMaximumMessages())
                .visibilityTimeout((int) sqsOptions.getVisibilityTimeout().getSeconds())
                .waitTimeSeconds((int) sqsOptions.getWaitTime().getSeconds())
                .build();
    }

    private Map<Message, S3EventNotification.S3EventNotificationRecord> getS3MessageEventNotificationRecordMap(final List<Message> sqsMessages) {
        return sqsMessages.stream()
                .collect(HashMap::new,
                        (messageRecordMap, message) -> messageRecordMap.put(message, convertS3EventMessages(message)),
                        HashMap::putAll);
    }

    S3EventNotification.S3EventNotificationRecord convertS3EventMessages(final Message message) {
        try {
            return S3EventNotification.parseJson(message.body()).getRecords().get(0);
        } catch (SdkClientException e) {
            LOG.error("Invalid JSON string in message body of {}", message.messageId());
        } catch (NullPointerException e) {
            LOG.error("Unable to convert message body of {} to S3EventNotificationRecord", message.messageId());
        }
        return null;
    }

    private void processS3ObjectAndDeleteSqsMessages(final Map<Message, S3EventNotification.S3EventNotificationRecord> s3EventNotificationRecords) {
        for (Map.Entry<Message, S3EventNotification.S3EventNotificationRecord> entry: s3EventNotificationRecords.entrySet()) {
            if (entry.getValue() == null) {
                // TODO: delete sqsMessages whose S3EventNotificationRecord is null
            }
            else if (isEventNameCreated(entry.getValue())) {
                final S3ObjectReference s3ObjectReference = populateS3Reference(entry.getValue());
                s3Service.addS3Object(s3ObjectReference);
                // TODO: delete sqsMessages which are successfully processed
            }
        }
    }

    boolean isEventNameCreated(final S3EventNotification.S3EventNotificationRecord s3EventNotificationRecord) {
        return objectCreatedFilter.filter(s3EventNotificationRecord)
                .isPresent();
    }

    S3ObjectReference populateS3Reference(final S3EventNotification.S3EventNotificationRecord s3EventNotificationRecord) {
        return S3ObjectReference.fromBucketAndKey(s3EventNotificationRecord.getS3().getBucket().getName(),
                s3EventNotificationRecord.getS3().getObject().getKey());
    }
}
