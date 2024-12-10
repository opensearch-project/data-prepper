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

import com.amazonaws.arn.Arn;
import com.linecorp.armeria.client.retry.Backoff;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.dataprepper.plugins.kinesis.source.exceptions.KinesisRetriesExhaustedException;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
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
    private int failedAttemptCount;
    private int maxRetryCount;

    public KinesisClientApiHandler(final KinesisAsyncClient kinesisClient, final Backoff backoff, final int maxRetryCount) {
        this.kinesisClient = kinesisClient;
        this.backoff = backoff;
        this.failedAttemptCount = 0;
        if (maxRetryCount <= 0) {
            throw new IllegalArgumentException("Maximum Retry count should be strictly greater than zero.");
        }
        this.maxRetryCount = maxRetryCount;
    }

    public StreamIdentifier getStreamIdentifier(final String streamName) {
        failedAttemptCount = 0;
        DescribeStreamSummaryRequest describeStreamSummaryRequest = DescribeStreamSummaryRequest.builder()
                .streamName(streamName).build();
        while (failedAttemptCount < maxRetryCount) {
            try {
                DescribeStreamSummaryResponse response = kinesisClient.describeStreamSummary(describeStreamSummaryRequest).join();
                String streamIdentifierString = getStreamIdentifierString(response.streamDescriptionSummary());
                return StreamIdentifier.multiStreamInstance(streamIdentifierString);
            } catch (CompletionException ex) {
                Throwable cause = ex.getCause();
                if (cause instanceof KinesisException || cause instanceof com.amazonaws.SdkClientException) {
                    log.error("Failed to describe stream summary for stream {} with error {}. The kinesis source will retry.", streamName, ex.getMessage());
                } else {
                    log.error("Failed to describe stream summary for stream {} with error {}. The kinesis source will retry.", streamName, ex);
                }
            }
            applyBackoff();
            ++failedAttemptCount;
        }
        throw new KinesisRetriesExhaustedException(String.format("Failed to get Kinesis stream summary for stream %s after %d retries", streamName, maxRetryCount));
    }

    private void applyBackoff() {
        final long delayMillis = backoff.nextDelayMillis(failedAttemptCount);
        if (delayMillis < 0) {
            throw new KinesisRetriesExhaustedException("Kinesis DescribeStreamSummary request retries exhausted. Make sure that Kinesis configuration is valid, Kinesis stream exists, and IAM role has required permissions.");
        }
        final Duration delayDuration = Duration.ofMillis(delayMillis);
        log.info("Pausing Kinesis DescribeStreamSummary request for {}.{} seconds due to an error in processing.",
                delayDuration.getSeconds(), delayDuration.toMillisPart());
        try {
            Thread.sleep(delayMillis);
        } catch (final InterruptedException e){
            log.error("Thread is interrupted while polling Kinesis with retry.", e);
        }
    }

    private String getStreamIdentifierString(StreamDescriptionSummary streamDescriptionSummary) {
        String accountId = Arn.fromString(streamDescriptionSummary.streamARN()).getAccountId();
        long creationEpochSecond = streamDescriptionSummary.streamCreationTimestamp().getEpochSecond();
        return String.join(COLON, accountId, streamDescriptionSummary.streamName(), String.valueOf(creationEpochSecond));
    }
}
