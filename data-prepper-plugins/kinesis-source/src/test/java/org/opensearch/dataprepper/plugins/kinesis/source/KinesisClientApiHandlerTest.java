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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.dataprepper.plugins.kinesis.source.exceptions.KinesisRetriesExhaustedException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.StreamDescriptionSummary;
import software.amazon.kinesis.common.StreamIdentifier;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KinesisClientApiHandlerTest {
    private static final String awsAccountId = "1234";
    private static final String streamArnFormat = "arn:aws:kinesis:us-east-1:%s:stream/%s";
    private static final Instant streamCreationTime = Instant.now();
    private static final int NUM_OF_RETRIES = 3;
    private String streamName;

    @Mock
    private KinesisAsyncClient kinesisClient;

    @Mock
    private Backoff backoff;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        streamName = UUID.randomUUID().toString();
    }

    private KinesisClientApiHandler createObjectUnderTest() {
        return new KinesisClientApiHandler(kinesisClient, backoff, NUM_OF_RETRIES);
    }

    @Test
    public void testGetStreamIdentifierThrowsException() {
        DescribeStreamSummaryRequest describeStreamSummaryRequest = DescribeStreamSummaryRequest.builder()
                .streamName(streamName)
                .build();

        when(kinesisClient.describeStreamSummary(describeStreamSummaryRequest)).thenThrow(new KinesisRetriesExhaustedException("exception"));

        KinesisClientApiHandler kinesisClientAPIHandler = createObjectUnderTest();

        assertThrows(KinesisRetriesExhaustedException.class, () -> kinesisClientAPIHandler.getStreamIdentifier(streamName));
    }

    @Test
    public void testGetStreamIdentifierSuccess() {
        DescribeStreamSummaryRequest describeStreamSummaryRequest = DescribeStreamSummaryRequest.builder()
                .streamName(streamName)
                .build();
        StreamDescriptionSummary streamDescriptionSummary = StreamDescriptionSummary.builder()
                .streamARN(String.format(streamArnFormat, awsAccountId, streamName))
                .streamCreationTimestamp(streamCreationTime)
                .streamName(streamName)
                .build();

        DescribeStreamSummaryResponse describeStreamSummaryResponse = DescribeStreamSummaryResponse.builder()
                .streamDescriptionSummary(streamDescriptionSummary)
                .build();
        final CompletableFuture<DescribeStreamSummaryResponse> successFuture = CompletableFuture.completedFuture(describeStreamSummaryResponse);

        given(kinesisClient.describeStreamSummary(describeStreamSummaryRequest)).willReturn(successFuture);

        KinesisClientApiHandler kinesisClientAPIHandler = createObjectUnderTest();

        StreamIdentifier streamIdentifier = kinesisClientAPIHandler.getStreamIdentifier(streamName);
        assertEquals(streamIdentifier, getStreamIdentifier(streamName));
    }

    @Test
    public void testGetStreamIdentifierSuccessWithRetries() {
        DescribeStreamSummaryRequest describeStreamSummaryRequest = DescribeStreamSummaryRequest.builder()
                .streamName(streamName)
                .build();
        StreamDescriptionSummary streamDescriptionSummary = StreamDescriptionSummary.builder()
                .streamARN(String.format(streamArnFormat, awsAccountId, streamName))
                .streamCreationTimestamp(streamCreationTime)
                .streamName(streamName)
                .build();

        DescribeStreamSummaryResponse describeStreamSummaryResponse = DescribeStreamSummaryResponse.builder()
                .streamDescriptionSummary(streamDescriptionSummary)
                .build();
        final CompletableFuture<DescribeStreamSummaryResponse> successFuture = CompletableFuture.completedFuture(describeStreamSummaryResponse);

        final CompletableFuture<DescribeStreamSummaryResponse> failedFuture1 = new CompletableFuture<>();
        failedFuture1.completeExceptionally(mock(Throwable.class));
        final CompletableFuture<DescribeStreamSummaryResponse> failedFuture2 = new CompletableFuture<>();
        failedFuture2.completeExceptionally(mock(Throwable.class));

        given(kinesisClient.describeStreamSummary(describeStreamSummaryRequest))
                .willReturn(failedFuture1)
                .willReturn(failedFuture2)
                .willReturn(successFuture);

        KinesisClientApiHandler kinesisClientAPIHandler = createObjectUnderTest();

        StreamIdentifier streamIdentifier = kinesisClientAPIHandler.getStreamIdentifier(streamName);
        assertEquals(streamIdentifier, getStreamIdentifier(streamName));
    }

    @Test
    public void testGetStreamIdentifierSuccessWithMultipleRetries() {
        DescribeStreamSummaryRequest describeStreamSummaryRequest = DescribeStreamSummaryRequest.builder()
                .streamName(streamName)
                .build();
        StreamDescriptionSummary streamDescriptionSummary = StreamDescriptionSummary.builder()
                .streamARN(String.format(streamArnFormat, awsAccountId, streamName))
                .streamCreationTimestamp(streamCreationTime)
                .streamName(streamName)
                .build();

        DescribeStreamSummaryResponse describeStreamSummaryResponse = DescribeStreamSummaryResponse.builder()
                .streamDescriptionSummary(streamDescriptionSummary)
                .build();
        final CompletableFuture<DescribeStreamSummaryResponse> successFuture = CompletableFuture.completedFuture(describeStreamSummaryResponse);

        final CompletableFuture<DescribeStreamSummaryResponse> failedFuture1 = new CompletableFuture<>();
        failedFuture1.completeExceptionally(mock(Throwable.class));
        final CompletableFuture<DescribeStreamSummaryResponse> failedFuture2 = new CompletableFuture<>();
        failedFuture2.completeExceptionally(mock(Throwable.class));

        given(kinesisClient.describeStreamSummary(describeStreamSummaryRequest))
                .willReturn(failedFuture1)
                .willReturn(failedFuture2)
                .willReturn(successFuture);

        KinesisClientApiHandler kinesisClientAPIHandler = createObjectUnderTest();

        StreamIdentifier streamIdentifier = kinesisClientAPIHandler.getStreamIdentifier(streamName);
        assertEquals(streamIdentifier, getStreamIdentifier(streamName));
    }

    @Test
    public void testGetStreamIdentifierSuccessWithMultipleRetriesForRetryableExceptions() {
        DescribeStreamSummaryRequest describeStreamSummaryRequest = DescribeStreamSummaryRequest.builder()
                .streamName(streamName)
                .build();
        StreamDescriptionSummary streamDescriptionSummary = StreamDescriptionSummary.builder()
                .streamARN(String.format(streamArnFormat, awsAccountId, streamName))
                .streamCreationTimestamp(streamCreationTime)
                .streamName(streamName)
                .build();

        DescribeStreamSummaryResponse describeStreamSummaryResponse = DescribeStreamSummaryResponse.builder()
                .streamDescriptionSummary(streamDescriptionSummary)
                .build();
        final CompletableFuture<DescribeStreamSummaryResponse> successFuture = CompletableFuture.completedFuture(describeStreamSummaryResponse);

        final CompletableFuture<DescribeStreamSummaryResponse> failedFuture1 = new CompletableFuture<>();
        failedFuture1.completeExceptionally(KinesisException.builder().build());
        final CompletableFuture<DescribeStreamSummaryResponse> failedFuture2 = new CompletableFuture<>();
        failedFuture2.completeExceptionally(SdkClientException.builder().build());

        given(kinesisClient.describeStreamSummary(describeStreamSummaryRequest))
                .willReturn(failedFuture1)
                .willReturn(failedFuture2)
                .willReturn(successFuture);

        KinesisClientApiHandler kinesisClientAPIHandler = createObjectUnderTest();

        StreamIdentifier streamIdentifier = kinesisClientAPIHandler.getStreamIdentifier(streamName);
        assertEquals(streamIdentifier, getStreamIdentifier(streamName));
    }

    @Test
    public void testGetStreamIdentifierFailureWithMultipleRetries() {
        DescribeStreamSummaryRequest describeStreamSummaryRequest = DescribeStreamSummaryRequest.builder()
                .streamName(streamName)
                .build();
        StreamDescriptionSummary streamDescriptionSummary = StreamDescriptionSummary.builder()
                .streamARN(String.format(streamArnFormat, awsAccountId, streamName))
                .streamCreationTimestamp(streamCreationTime)
                .streamName(streamName)
                .build();

        DescribeStreamSummaryResponse describeStreamSummaryResponse = DescribeStreamSummaryResponse.builder()
                .streamDescriptionSummary(streamDescriptionSummary)
                .build();

        final CompletableFuture<DescribeStreamSummaryResponse> failedFuture1 = new CompletableFuture<>();
        failedFuture1.completeExceptionally(mock(Throwable.class));
        final CompletableFuture<DescribeStreamSummaryResponse> failedFuture2 = new CompletableFuture<>();
        failedFuture2.completeExceptionally(mock(Throwable.class));
        final CompletableFuture<DescribeStreamSummaryResponse> failedFuture3 = new CompletableFuture<>();
        failedFuture3.completeExceptionally(mock(Throwable.class));

        given(kinesisClient.describeStreamSummary(describeStreamSummaryRequest))
                .willReturn(failedFuture1)
                .willReturn(failedFuture2)
                .willReturn(failedFuture3);

        when(backoff.nextDelayMillis(eq(2))).thenReturn(-10L);

        KinesisClientApiHandler kinesisClientAPIHandler = createObjectUnderTest();

        assertThrows(KinesisRetriesExhaustedException.class, ()->kinesisClientAPIHandler.getStreamIdentifier(streamName));
    }

    @Test
    public void testCreateFailureInvalidMaxRetry() {
        assertThrows(IllegalArgumentException.class, () -> new KinesisClientApiHandler(kinesisClient, backoff, -1));
    }

    private StreamIdentifier getStreamIdentifier(final String streamName) {
        return StreamIdentifier.multiStreamInstance(String.join(":", awsAccountId, streamName, String.valueOf(streamCreationTime.getEpochSecond())));
    }
}
