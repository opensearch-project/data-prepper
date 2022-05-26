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

    private final S3SourceConfig s3SourceConfig;
    private final SqsClient sqsClient;
    private final S3Client s3Client;
    private final ObjectMapper objectMapper;

    public SqsWorker(SqsClient sqsClient, S3Client s3Client, S3SourceConfig s3SourceConfig) {
        this.s3SourceConfig = s3SourceConfig;
        this.sqsClient = sqsClient;
        this.s3Client = s3Client;
        objectMapper = new ObjectMapper();
    }

    @Override
    public void run() {
        SqsOptions sqsOptions = s3SourceConfig.getSqsOptions();

        do {
            // read messages from sqs
            List<Message> messages = new ArrayList<>();

            try {
                ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                        .queueUrl(sqsOptions.getSqsUrl())
                        .maxNumberOfMessages(sqsOptions.getMaximumMessages())
                        .visibilityTimeout((int) sqsOptions.getVisibilityTimeout().getSeconds())
                        .waitTimeSeconds((int) sqsOptions.getWaitTime().getSeconds())
                        .build();

                messages.addAll(sqsClient.receiveMessage(receiveMessageRequest).messages());
            } catch (SqsException e) {
                LOG.error(e.awsErrorDetails().errorMessage());
                System.exit(1);
            }

            // read each message as S3 event message
            List<S3EventNotification.S3EventNotificationRecord> s3EventNotificationRecords = messages.stream()
                    .map(this::getS3EventMessages)
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
                    e.printStackTrace();
                }
            }
        } while (true);
    }

    private S3EventNotification.S3EventNotificationRecord getS3EventMessages(Message message) {
        return objectMapper.convertValue(message, S3EventNotification.S3EventNotificationRecord.class);
    }

}
