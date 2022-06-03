/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.plugins.source.configuration.SqsOptions;
import com.amazon.dataprepper.plugins.source.filter.ObjectCreatedFilter;
import com.amazon.dataprepper.plugins.source.filter.S3EventFilter;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class SqsWorker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(SqsWorker.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private S3SourceConfig s3SourceConfig;
    private SqsClient sqsClient;
    private S3Service s3Service;
    private SqsOptions sqsOptions;
    private S3EventFilter objectCreatedFilter;

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
            List<Message> messages = new ArrayList<>();

            try {
                ReceiveMessageRequest receiveMessageRequest = createReceiveMessageRequest();
                messages.addAll(sqsClient.receiveMessage(receiveMessageRequest).messages());
            } catch (SqsException e) {
                LOG.error("Error reading from SQS: {}", e.awsErrorDetails().errorMessage());
            }

            // read each message as S3 event message
            Map<Message, S3EventNotification.S3EventNotificationRecord> s3EventNotificationRecords = messages.stream()
                    .collect(Collectors.toMap(message -> message, this::convertS3EventMessages));

            // build s3ObjectPointer from S3EventNotificationRecord if event name starts with ObjectCreated
            List<S3ObjectReference> addedObjects = new ArrayList<>();
            for (Map.Entry<Message, S3EventNotification.S3EventNotificationRecord> entry: s3EventNotificationRecords.entrySet()) {
                Optional<S3EventNotification.S3EventNotificationRecord> filter = objectCreatedFilter.filter(entry.getValue());
                if(filter.isPresent()) {
                    S3ObjectReference s3ObjectPointer = populateS3Reference(entry.getValue());
                    addedObjects.add(s3Service.addS3Object(s3ObjectPointer));
                }
            }

            // TODO: delete messages which are successfully processed

            if (messages.size() < sqsOptions.getMaximumMessages() && s3SourceConfig.getSqsOptions().getPollDelay().toMillis() > 0) {
                try {
                    Thread.sleep(s3SourceConfig.getSqsOptions().getPollDelay().toMillis());
                } catch (InterruptedException e) {
                    LOG.error("Thread is interrupted while polling SQS.", e);
                }
            }
        }
    }

    S3ObjectReference populateS3Reference(final S3EventNotification.S3EventNotificationRecord s3EventNotificationRecord) {
        return new S3ObjectReference(s3EventNotificationRecord.getS3().getBucket().getName(),
                s3EventNotificationRecord.getS3().getObject().getKey());
    }

    ReceiveMessageRequest createReceiveMessageRequest() {
        return ReceiveMessageRequest.builder()
                .queueUrl(sqsOptions.getSqsUrl())
                .maxNumberOfMessages(sqsOptions.getMaximumMessages())
                .visibilityTimeout((int) sqsOptions.getVisibilityTimeout().getSeconds())
                .waitTimeSeconds((int) sqsOptions.getWaitTime().getSeconds())
                .build();
    }

    private S3EventNotification.S3EventNotificationRecord convertS3EventMessages(final Message message) {
        return objectMapper.convertValue(message, S3EventNotification.S3EventNotificationRecord.class);
    }
}
