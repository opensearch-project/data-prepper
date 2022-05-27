/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.plugins.source.configuration.SqsOptions;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SqsWorker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(SqsWorker.class);

    private static final int NUMBER_OF_RETRIES = 5;
    private static final long TIME_TO_WAIT = 5000;

    private final S3SourceConfig s3SourceConfig;
    private final SqsClient sqsClient;
    private final S3Client s3Client;
    private final ObjectMapper objectMapper;

    SqsOptions sqsOptions;
    private BackoffUtils sqsBackoff;

    public SqsWorker(final SqsClient sqsClient, final S3Client s3Client, final S3SourceConfig s3SourceConfig) {
        this.s3SourceConfig = s3SourceConfig;
        this.sqsClient = sqsClient;
        this.s3Client = s3Client;
        objectMapper = new ObjectMapper();
        sqsOptions = s3SourceConfig.getSqsOptions();
    }

    @Override
    public void run() {

        sqsBackoff = new BackoffUtils(NUMBER_OF_RETRIES, TIME_TO_WAIT);

        do {
            // read messages from sqs
            List<Message> messages = new ArrayList<>();

            while (sqsBackoff.shouldRetry()) {
                try {
                    ReceiveMessageRequest receiveMessageRequest = createReceiveMessageRequest();
                    messages.addAll(sqsClient.receiveMessage(receiveMessageRequest).messages());

                    sqsBackoff.doNotRetry();
                } catch (SqsException e) {
                    sqsBackoff.errorOccurred();
                    if (sqsBackoff.getNumberOfTriesLeft() == 0)
                        LOG.error("Error reading from SQS: {}", e.awsErrorDetails().errorMessage());
                }
            }

            // read each message as S3 event message
            List<S3EventNotification.S3EventNotificationRecord> s3EventNotificationRecords = messages.stream()
                    .map(this::convertS3EventMessages)
                    .collect(Collectors.toList());


            // TODO: get bucket name and object key from message where eventName is s3:ObjectCreated


            // TODO: download S3 object for eventName s3:ObjectCreated


            // TODO: Decompress if required using codec into Event object


            // TODO: Write to buffer


            //TODO: delete messages which are successfully processed

            if (messages.size() < sqsOptions.getMaximumMessages() && s3SourceConfig.getSqsOptions().getPollDelay().toMillis() > 0) {
                try {
                    Thread.sleep(s3SourceConfig.getSqsOptions().getPollDelay().toMillis());
                } catch (InterruptedException e) {
                    LOG.error("Thread is interrupted while polling.", e);
                }
            }
        } while (true);
    }

    private ReceiveMessageRequest createReceiveMessageRequest() {
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
