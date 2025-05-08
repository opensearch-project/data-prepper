/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kinesis.source;

import com.linecorp.armeria.client.retry.Backoff;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.dataprepper.plugins.kinesis.source.exceptions.KinesisConsumerNotFoundException;
import org.opensearch.dataprepper.plugins.kinesis.source.exceptions.KinesisRetriesExhaustedException;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.StreamDescriptionSummary;
import software.amazon.kinesis.common.StreamIdentifier;

import java.time.Duration;
import java.util.concurrent.CompletionException;

@Slf4j
public class KinesisClientApiHandler {
    private static final String COLON = ":";

    private final Backoff backoff;
    private final KinesisAsyncClient kinesisClient;
    private int maxRetryCount;

    public KinesisClientApiHandler(final KinesisAsyncClient kinesisClient, final Backoff backoff, final int maxRetryCount) {
        this.kinesisClient = kinesisClient;
        this.backoff = backoff;
        if (maxRetryCount <= 0) {
            throw new IllegalArgumentException("Maximum Retry count should be strictly greater than zero.");
        }
        this.maxRetryCount = maxRetryCount;
    }

    public StreamIdentifier getStreamIdentifierFromStreamArn(final String streamArnString) {
        Arn streamArn = Arn.fromString(streamArnString);
        DescribeStreamSummaryRequest describeStreamSummaryRequest = DescribeStreamSummaryRequest.builder()
                .streamName(streamArn.resource().resource()).streamARN(streamArnString).build();
        DescribeStreamSummaryResponse response = getStreamDescriptionSummary(describeStreamSummaryRequest);
        long creationEpochSecond = response.streamDescriptionSummary().streamCreationTimestamp().getEpochSecond();
        return StreamIdentifier.multiStreamInstance(streamArn, creationEpochSecond);
    }

    public StreamIdentifier getStreamIdentifier(final String streamName) {
        DescribeStreamSummaryRequest describeStreamSummaryRequest = DescribeStreamSummaryRequest.builder()
                .streamName(streamName).build();
        DescribeStreamSummaryResponse response = getStreamDescriptionSummary(describeStreamSummaryRequest);
        String streamIdentifierString = getStreamIdentifierString(response.streamDescriptionSummary());
        return StreamIdentifier.multiStreamInstance(streamIdentifierString);
    }

    public String getConsumerArnForStream(final String streamArn, final String consumerName) {
        DescribeStreamConsumerResponse response = describeStreamConsumer(streamArn, consumerName);
        if (response == null) {
            throw new KinesisConsumerNotFoundException(String.format("Kinesis stream consumer not found for %s", consumerName));
        }
        return response.consumerDescription().consumerARN();
    }

    private DescribeStreamSummaryResponse getStreamDescriptionSummary(DescribeStreamSummaryRequest describeStreamSummaryRequest) {
        for (int attempt = 0; attempt < maxRetryCount; attempt++) {
            try {
                return kinesisClient.describeStreamSummary(describeStreamSummaryRequest).join();
            } catch (CompletionException ex) {
                Throwable cause = ex.getCause();
                if (cause instanceof KinesisException || cause instanceof com.amazonaws.SdkClientException) {
                    log.error("Failed to describe stream summary for stream {} with error {}. The kinesis source will retry.",
                            describeStreamSummaryRequest.streamName(), ex.getMessage());
                } else {
                    log.error("Failed to describe stream summary for stream {} with error {}. The kinesis source will retry.",
                            describeStreamSummaryRequest.streamName(), ex);
                }
            }
            applyBackoff(attempt);
        }
        throw new KinesisRetriesExhaustedException(String.format("Failed to get Kinesis stream summary for stream %s after %d retries",
                describeStreamSummaryRequest.streamName(), maxRetryCount));
    }

    private String getStreamIdentifierString(StreamDescriptionSummary streamDescriptionSummary) {
        String accountId = Arn.fromString(streamDescriptionSummary.streamARN()).accountId().get();
        long creationEpochSecond = streamDescriptionSummary.streamCreationTimestamp().getEpochSecond();
        return String.join(COLON, accountId, streamDescriptionSummary.streamName(), String.valueOf(creationEpochSecond));
    }

    private DescribeStreamConsumerResponse describeStreamConsumer(final String streamArn, final String consumerName) {
        DescribeStreamConsumerRequest request = DescribeStreamConsumerRequest.builder()
                .streamARN(streamArn).consumerName(consumerName).build();
        for (int attempt = 0; attempt < maxRetryCount; attempt++) {
            try {
                return kinesisClient.describeStreamConsumer(request).join();
            } catch (CompletionException ex) {
                handleException(ex, streamArn, attempt);
            }
            applyBackoff(attempt);
        }
        throw new KinesisRetriesExhaustedException(String.format("Failed to get Kinesis stream consumer for stream arn %s after %d retries", streamArn, maxRetryCount));
    }

    private void applyBackoff(int attempt) {
        final long delayMillis = backoff.nextDelayMillis(attempt);
        if (delayMillis < 0) {
            throw new KinesisRetriesExhaustedException("Kinesis DescribeStreamSummary request retries exhausted. Make sure that Kinesis configuration is valid, Kinesis stream exists, and IAM role has required permissions.");
        }
        final Duration delayDuration = Duration.ofMillis(delayMillis);
        log.info("Pausing Kinesis DescribeStreamSummary request for {}.{} seconds due to an error in processing.",
                delayDuration.getSeconds(), delayDuration.toMillisPart());
        try {
            Thread.sleep(delayMillis);
        } catch (final InterruptedException e){
            Thread.currentThread().interrupt();
            log.error("Thread is interrupted while polling Kinesis with retry.", e);
        }
    }


    private void handleException(CompletionException ex, String streamName, int attempt) {
        Throwable cause = ex.getCause();
        if (cause instanceof KinesisException || cause instanceof com.amazonaws.SdkClientException) {
            log.error("Failed to describe stream summary for stream {} with error {}. Attempt {} of {}.", streamName, ex.getMessage(), attempt + 1, maxRetryCount);
        } else {
            log.error("Failed to describe stream summary for stream {} with error. Attempt {} of {}.", streamName, attempt + 1, maxRetryCount, ex);
        }
    }
}
