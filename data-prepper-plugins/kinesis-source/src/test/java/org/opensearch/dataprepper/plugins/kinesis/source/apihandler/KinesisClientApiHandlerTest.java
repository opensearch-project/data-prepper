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

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.dataprepper.plugins.kinesis.source.exceptions.KinesisConsumerNotFoundException;
import org.opensearch.dataprepper.plugins.kinesis.source.exceptions.KinesisRetriesExhaustedException;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.awssdk.services.kinesis.model.StreamDescriptionSummary;
import software.amazon.kinesis.common.StreamIdentifier;

import java.time.Instant;
import java.util.UUID;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

public class KinesisClientApiHandlerTest {
    private static final String awsAccountId = RandomStringUtils.randomNumeric(12);
    private static final String streamArnFormat = "arn:aws:kinesis:us-east-1:%s:stream/%s";
    private static final Instant streamCreationTime = Instant.now();
    private static final int NUM_OF_RETRIES = 3;
    private String streamName;

    @Mock
    private KinesisAsyncClient kinesisClient;

    @Mock
    private KinesisClientApiRetryHandler KinesisClientApiRetryHandler;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        streamName = UUID.randomUUID().toString();
    }

    private KinesisClientApiHandler createObjectUnderTest() {
        return new KinesisClientApiHandler(kinesisClient, KinesisClientApiRetryHandler);
    }

    @Nested
    class KinesisClientApiHandlerConstructor {
        @Test
        void withNullKinesisClient_throwsException() {
            assertThrows(NullPointerException.class,
                    () -> new KinesisClientApiHandler(null, KinesisClientApiRetryHandler));
        }

        @Test
        void withNullKinesisClientApiRetryHandler_throwsException() {
            assertThrows(NullPointerException.class,
                    () -> new KinesisClientApiHandler(kinesisClient, null));
        }
    }

    @Nested
    class GetStreamIdentifier {
        @Test
        void withValidStreamName_returnsStreamIdentifier() {
            DescribeStreamSummaryResponse response = createStreamSummaryResponse(streamName);
            when(KinesisClientApiRetryHandler.executeWithRetry(
                    eq("getStreamDescriptionSummary"),
                    any(Supplier.class),
                    any(KinesisClientApiRetryHandler.ExceptionHandler.class)
            )).thenReturn(response);

            StreamIdentifier result = createObjectUnderTest().getStreamIdentifier(streamName);
            assertEquals(getStreamIdentifier(streamName), result);
        }

        @Test
        void withValidStreamArn_returnsStreamIdentifier() {
            String streamArn = String.format(streamArnFormat, awsAccountId, streamName);
            DescribeStreamSummaryResponse response = createStreamSummaryResponse(streamName);
            when(KinesisClientApiRetryHandler.executeWithRetry(
                    eq("getStreamDescriptionSummary"),
                    any(Supplier.class),
                    any(KinesisClientApiRetryHandler.ExceptionHandler.class)
            )).thenReturn(response);

            StreamIdentifier result = createObjectUnderTest().getStreamIdentifier(streamArn);
            StreamIdentifier expected = StreamIdentifier.multiStreamInstance(
                    Arn.fromString(streamArn),
                    streamCreationTime.getEpochSecond());
            assertEquals(expected, result);
        }

        @Test
        void whenKinesisClientApiRetryHandlerThrowsException_propagatesException() {
            when(KinesisClientApiRetryHandler.executeWithRetry(
                    anyString(),
                    any(Supplier.class),
                    any(KinesisClientApiRetryHandler.ExceptionHandler.class)
            )).thenThrow(new KinesisRetriesExhaustedException("Test exception"));

            assertThrows(KinesisRetriesExhaustedException.class,
                    () -> createObjectUnderTest().getStreamIdentifier(streamName));
        }
    }

    @Nested
    class GetConsumerArnForStream {
        private final String streamArn = String.format(streamArnFormat, awsAccountId, streamName);
        private final String consumerName = "testConsumer";

        @Test
        void withNullStreamArn_throwsException() {
            assertThrows(IllegalArgumentException.class,
                    () -> createObjectUnderTest().getConsumerArnForStream(null, consumerName));
        }

        @Test
        void withEmptyStreamArn_throwsException() {
            assertThrows(IllegalArgumentException.class,
                    () -> createObjectUnderTest().getConsumerArnForStream("", consumerName));
        }

        @Test
        void withNullConsumerName_throwsException() {
            assertThrows(IllegalArgumentException.class,
                    () -> createObjectUnderTest().getConsumerArnForStream(streamArn, null));
        }

        @Test
        void withEmptyConsumerName_throwsException() {
            assertThrows(IllegalArgumentException.class,
                    () -> createObjectUnderTest().getConsumerArnForStream(streamArn, ""));
        }

        @Test
        void withValidInput_returnsConsumerArn() {
            String consumerArn = streamArn + "/consumer/" + consumerName;
            DescribeStreamConsumerResponse response = createConsumerResponse(streamArn, consumerName, consumerArn);

            when(KinesisClientApiRetryHandler.executeWithRetry(
                    eq("describeStreamConsumer"),
                    any(Supplier.class),
                    any(KinesisClientApiRetryHandler.ExceptionHandler.class)
            )).thenReturn(response);

            String result = createObjectUnderTest().getConsumerArnForStream(streamArn, consumerName);
            assertEquals(consumerArn, result);
        }

        @Test
        void whenResponseIsNull_throwsNotFoundException() {
            when(KinesisClientApiRetryHandler.executeWithRetry(
                    eq("describeStreamConsumer"),
                    any(Supplier.class),
                    any(KinesisClientApiRetryHandler.ExceptionHandler.class)
            )).thenReturn(null);

            assertThrows(KinesisConsumerNotFoundException.class,
                    () -> createObjectUnderTest().getConsumerArnForStream(streamArn, consumerName));
        }
    }

    // Helper methods
    private StreamIdentifier getStreamIdentifier(final String streamName) {
        return StreamIdentifier.multiStreamInstance(
                String.join(":", awsAccountId, streamName,
                        String.valueOf(streamCreationTime.getEpochSecond())));
    }

    private DescribeStreamSummaryResponse createStreamSummaryResponse(String streamName) {
        StreamDescriptionSummary summary = StreamDescriptionSummary.builder()
                .streamARN(String.format(streamArnFormat, awsAccountId, streamName))
                .streamCreationTimestamp(streamCreationTime)
                .streamName(streamName)
                .build();
        return DescribeStreamSummaryResponse.builder()
                .streamDescriptionSummary(summary)
                .build();
    }

    private DescribeStreamConsumerResponse createConsumerResponse(
            String streamArn, String consumerName, String consumerArn) {
        return DescribeStreamConsumerResponse.builder()
                .consumerDescription(builder -> builder
                        .consumerARN(consumerArn)
                        .consumerName(consumerName)
                        .streamARN(streamArn))
                .build();
    }
}
