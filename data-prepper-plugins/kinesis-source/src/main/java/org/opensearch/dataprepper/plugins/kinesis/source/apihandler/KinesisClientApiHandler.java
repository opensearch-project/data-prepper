/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kinesis.source.apihandler;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.dataprepper.plugins.kinesis.source.exceptions.KinesisConsumerNotFoundException;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.StreamDescriptionSummary;
import software.amazon.kinesis.common.StreamIdentifier;

import java.util.Objects;
import java.util.concurrent.CompletionException;

@Slf4j
public class KinesisClientApiHandler {
    private static final String COLON = ":";

    private final KinesisAsyncClient kinesisClient;
    private final KinesisClientApiRetryHandler kinesisClientApiRetryHandler;

    public KinesisClientApiHandler(@NonNull final KinesisAsyncClient kinesisClient, @NonNull final KinesisClientApiRetryHandler kinesisClientApiRetryHandler) {
        this.kinesisClient = kinesisClient;
        this.kinesisClientApiRetryHandler = kinesisClientApiRetryHandler;
    }

    public StreamIdentifier getStreamIdentifier(final String streamNameOrArn) {
        if (isArn(streamNameOrArn)) {
            return getStreamIdentifierFromArn(streamNameOrArn);
        }
        return getStreamIdentifierFromName(streamNameOrArn);
    }

    public String getConsumerArnForStream(final String streamArn, final String consumerName) {
        if (Objects.isNull(streamArn) || streamArn.trim().isEmpty()) {
            throw new IllegalArgumentException("Stream ARN cannot be null or empty");
        }
        if (Objects.isNull(consumerName) || consumerName.trim().isEmpty()) {
            throw new IllegalArgumentException("Consumer name cannot be null or empty");
        }
        DescribeStreamConsumerResponse response = describeStreamConsumer(streamArn, consumerName);
        if (Objects.isNull(response)) {
            throw new KinesisConsumerNotFoundException(
                    String.format("Kinesis stream consumer not found for %s", consumerName));
        }
        return response.consumerDescription().consumerARN();
    }

    private boolean isArn(final String streamNameOrArn) {
        try {
            Arn.fromString(streamNameOrArn);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    private StreamIdentifier getStreamIdentifierFromArn(final String streamArnString) {
        Arn streamArn = Arn.fromString(streamArnString);
        String streamName = streamArn.resource().resource();
        DescribeStreamSummaryResponse response = getStreamDescriptionSummary(
                buildStreamSummaryRequest(streamName, streamArnString));
        return StreamIdentifier.multiStreamInstance(
                streamArn,
                response.streamDescriptionSummary().streamCreationTimestamp().getEpochSecond());
    }

    private StreamIdentifier getStreamIdentifierFromName(final String streamName) {
        DescribeStreamSummaryResponse response = getStreamDescriptionSummary(
                buildStreamSummaryRequest(streamName, null));
        StreamDescriptionSummary summary = response.streamDescriptionSummary();
        return StreamIdentifier.multiStreamInstance(createStreamIdentifierString(summary));
    }

    private DescribeStreamSummaryRequest buildStreamSummaryRequest(String streamName, String streamArn) {
        return DescribeStreamSummaryRequest.builder()
                .streamName(streamName)
                .streamARN(streamArn)
                .build();
    }

    private String createStreamIdentifierString(StreamDescriptionSummary summary) {
        String accountId = Arn.fromString(summary.streamARN()).accountId().get();
        long creationEpochSecond = summary.streamCreationTimestamp().getEpochSecond();
        return String.join(COLON, accountId, summary.streamName(),
                String.valueOf(creationEpochSecond));
    }

    private DescribeStreamSummaryResponse getStreamDescriptionSummary(
            DescribeStreamSummaryRequest request) {
        return kinesisClientApiRetryHandler.executeWithRetry(
                "getStreamDescriptionSummary",
                () -> kinesisClient.describeStreamSummary(request).join(),
                (ex, attempt) -> handleStreamSummaryException((CompletionException) ex, request.streamName())
        );
    }

    private void handleStreamSummaryException(CompletionException ex, String streamName) {
        Throwable cause = ex.getCause();
        if (cause instanceof KinesisException) {
            log.error("Kinesis API error while describing stream summary for stream {}: {}",
                    streamName, ex.getMessage());
        } else if (cause instanceof com.amazonaws.SdkClientException) {
            log.error("AWS SDK client error while describing stream summary for stream {}: {}",
                    streamName, ex.getMessage());
        } else {
            log.error("Unexpected error while describing stream summary for stream {}",
                    streamName, ex);
        }
    }

    private DescribeStreamConsumerResponse describeStreamConsumer(
            final String streamArn, final String consumerName) {
        DescribeStreamConsumerRequest request = DescribeStreamConsumerRequest.builder()
                .streamARN(streamArn)
                .consumerName(consumerName)
                .build();

        return kinesisClientApiRetryHandler.executeWithRetry(
                "describeStreamConsumer",
                () -> kinesisClient.describeStreamConsumer(request).join(),
                (ex, attempt) -> handleConsumerException((CompletionException) ex, streamArn, attempt)
        );
    }

    private void handleConsumerException(CompletionException ex, String streamArn, int attempt) {
        Throwable cause = ex.getCause();
        if (cause instanceof KinesisException) {
            log.error("Kinesis API error while describing stream consumer for stream {}: {}. Attempt {}.",
                    streamArn, ex.getMessage(), attempt + 1);
        } else if (cause instanceof com.amazonaws.SdkClientException) {
            log.error("AWS SDK client error while describing stream consumer for stream {}: {}. Attempt {}.",
                    streamArn, ex.getMessage(), attempt + 1);
        } else {
            log.error("Unexpected error while describing stream consumer for stream {}. Attempt {} of {}.",
                    streamArn, attempt + 1, ex);
        }
    }
}

