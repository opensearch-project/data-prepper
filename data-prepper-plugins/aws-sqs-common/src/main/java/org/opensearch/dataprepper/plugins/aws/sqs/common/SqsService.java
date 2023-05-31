/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.aws.sqs.common;

import com.linecorp.armeria.client.retry.Backoff;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.aws.sqs.common.codec.Codec;
import org.opensearch.dataprepper.plugins.aws.sqs.common.exception.SqsRetriesExhaustedException;
import org.opensearch.dataprepper.plugins.aws.sqs.common.metrics.SqsMetrics;
import org.opensearch.dataprepper.plugins.aws.sqs.common.model.SqsOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;
import software.amazon.awssdk.services.sts.model.StsException;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class SqsService {
    private static final Logger LOG = LoggerFactory.getLogger(SqsService.class);
    static final long INITIAL_DELAY = Duration.ofSeconds(20).toMillis();
    static final long MAXIMUM_DELAY = Duration.ofMinutes(5).toMillis();
    static final double JITTER_RATE = 0.20;
    private final SqsMetrics sqsMetrics;

    private final SqsClient sqsClient;

    private final Backoff backoff;

    private int failedAttemptCount;

    private BufferAccumulator<Record<Event>> bufferAccumulator;

    private final Codec codec;

    public SqsService(final SqsMetrics sqsMetrics,
                      final Region region,
                      final AwsCredentialsProvider awsCredentialsProvider,
                      final BufferAccumulator<Record<Event>> bufferAccumulator,
                      final Codec codec) {
        this.sqsMetrics = sqsMetrics;
        this.sqsClient = createSqsClient(region,awsCredentialsProvider);
        this.backoff = Backoff.exponential(INITIAL_DELAY, MAXIMUM_DELAY).withJitter(JITTER_RATE)
                .withMaxAttempts(Integer.MAX_VALUE);
        this.bufferAccumulator = bufferAccumulator;
        this.codec = codec;
    }

    SqsClient createSqsClient(final Region region, final AwsCredentialsProvider awsCredentialsProvider) {
        LOG.info("Creating SQS client");
        return SqsClient.builder()
                .region(region)
                .credentialsProvider(awsCredentialsProvider)
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .retryPolicy(builder -> builder.numRetries(5).build())
                        .build())
                .build();
    }

    public ReceiveMessageRequest createReceiveMessageRequest(final SqsOptions sqsOptions) {
        return ReceiveMessageRequest.builder()
                .queueUrl(sqsOptions.getSqsUrl())
                .maxNumberOfMessages(sqsOptions.getMaximumMessages())
                .build();
    }

    public List<Message> getMessagesFromSqs(final SqsOptions sqsOptions) {
        try {
            final ReceiveMessageRequest receiveMessageRequest = createReceiveMessageRequest(sqsOptions);
            final List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).messages();
            if(!messages.isEmpty())
                sqsMetrics.getSqsMessagesReceivedCounter().increment(messages.size());
            failedAttemptCount = 0;
            return messages;
        } catch (final SqsException | StsException e) {
            LOG.error("Error reading from SQS: {}. Retrying with exponential backoff.", e.getMessage());
            applyBackoff();
            return Collections.emptyList();
        }
    }

    public void applyBackoff() {
        final long delayMillis = backoff.nextDelayMillis(++failedAttemptCount);
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

    public Optional<String> parseMessage(final Message message,final AcknowledgementSet acknowledgementSet) throws IOException {
        pushToBuffer(codec.parse(message.body()),acknowledgementSet);
        return Optional.of(message.receiptHandle());
    }

    public boolean pushToBuffer(final Record<Event> eventRecord, final AcknowledgementSet acknowledgementSet) {
            try {
                bufferAccumulator.add(eventRecord);
                bufferAccumulator.flush();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            if (acknowledgementSet != null) {
                acknowledgementSet.add(eventRecord.getData());
            }
            return true;
    }

    public void deleteMessageFromQueue(final String receiptHandle,
                                       final SqsOptions sqsOptions) {
        try{
            sqsClient.deleteMessage(DeleteMessageRequest.builder()
                    .queueUrl(sqsOptions.getSqsUrl())
                    .receiptHandle(receiptHandle)
                    .build());
            sqsMetrics.getSqsMessagesDeletedCounter().increment();
            LOG.info("message deleted successfully : {}",receiptHandle);
        }catch(Exception e){
            sqsMetrics.getSqsMessagesDeleteFailedCounter().increment();
        }
    }
}
