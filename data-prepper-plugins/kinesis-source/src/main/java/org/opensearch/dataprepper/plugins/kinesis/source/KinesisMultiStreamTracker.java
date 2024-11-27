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
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.KinesisSourceConfig;
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.KinesisStreamConfig;
import org.opensearch.dataprepper.plugins.kinesis.source.exceptions.KinesisRetriesExhaustedException;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.awssdk.services.kinesis.model.StreamDescriptionSummary;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.processor.FormerStreamsLeasesDeletionStrategy;
import software.amazon.kinesis.processor.MultiStreamTracker;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class KinesisMultiStreamTracker implements MultiStreamTracker {
    private static final String COLON = ":";
    private static final int NUM_OF_RETRIES = 3;
    private final KinesisAsyncClient kinesisClient;
    private final KinesisSourceConfig sourceConfig;
    private final String applicationName;
    private final Backoff backoff;
    private int failedAttemptCount;

    public KinesisMultiStreamTracker(KinesisAsyncClient kinesisClient, final KinesisSourceConfig sourceConfig, final String applicationName, final Backoff backoff) {
        this.kinesisClient = kinesisClient;
        this.sourceConfig = sourceConfig;
        this.applicationName = applicationName;
        this.backoff = backoff;
        this.failedAttemptCount = 0;
    }

    @Override
    public List<StreamConfig> streamConfigList()  {
        List<StreamConfig> streamConfigList = new ArrayList<>();
        for (KinesisStreamConfig kinesisStreamConfig : sourceConfig.getStreams()) {
            StreamConfig streamConfig = getStreamConfig(kinesisStreamConfig);
            streamConfigList.add(streamConfig);
        }
        return streamConfigList;
    }

    private StreamConfig getStreamConfig(KinesisStreamConfig kinesisStreamConfig) {
        StreamIdentifier sourceStreamIdentifier = getStreamIdentifier(kinesisStreamConfig);
        return new StreamConfig(sourceStreamIdentifier,
                InitialPositionInStreamExtended.newInitialPosition(kinesisStreamConfig.getInitialPosition()));
    }

    private StreamIdentifier getStreamIdentifier(final KinesisStreamConfig kinesisStreamConfig) {
        failedAttemptCount = 0;
        while (failedAttemptCount <= NUM_OF_RETRIES) {
            try {
                DescribeStreamSummaryRequest describeStreamSummaryRequest = DescribeStreamSummaryRequest.builder()
                        .streamName(kinesisStreamConfig.getName()).build();
                DescribeStreamSummaryResponse response = kinesisClient.describeStreamSummary(describeStreamSummaryRequest).join();
                String streamIdentifierString = getStreamIdentifierString(response.streamDescriptionSummary());
                return StreamIdentifier.multiStreamInstance(streamIdentifierString);
            } catch (Exception ex) {
                log.error("Failed to get stream summary for stream {}, retrying", kinesisStreamConfig.getName(), ex);
            }
            applyBackoff();
            ++failedAttemptCount;
        }
        throw new RuntimeException(String.format("Failed to get Kinesis stream summary for stream %s after %d retries", kinesisStreamConfig.getName(), NUM_OF_RETRIES));
    }

    private void applyBackoff() {
        final long delayMillis = backoff.nextDelayMillis(failedAttemptCount);
        if (delayMillis < 0) {
            Thread.currentThread().interrupt();
            throw new KinesisRetriesExhaustedException("Kinesis DescribeStreamSummary request retries exhausted. Make sure that Kinesis configuration is valid, Kinesis stream exists, and IAM role has required permissions.");
        }
        final Duration delayDuration = Duration.ofMillis(delayMillis);
        log.info("Pausing Kinesis DescribeStreamSummary request for {}.{} seconds due to an error in processing.",
                delayDuration.getSeconds(), delayDuration.toMillisPart());
        try {
            Thread.sleep(delayMillis);
        } catch (final InterruptedException e){
            log.error("Thread is interrupted while polling SQS with retry.", e);
        }
    }


    private String getStreamIdentifierString(StreamDescriptionSummary streamDescriptionSummary) {
        String accountId = Arn.fromString(streamDescriptionSummary.streamARN()).getAccountId();
        long creationEpochSecond = streamDescriptionSummary.streamCreationTimestamp().getEpochSecond();
        return String.join(COLON, accountId, streamDescriptionSummary.streamName(), String.valueOf(creationEpochSecond));
    }

    /**
     * Setting the deletion policy as autodetect and release shard lease with a wait time of 10 sec
     */
    @Override
    public FormerStreamsLeasesDeletionStrategy formerStreamsLeasesDeletionStrategy() {
        return new FormerStreamsLeasesDeletionStrategy.AutoDetectionAndDeferredDeletionStrategy() {
            @Override
            public Duration waitPeriodToDeleteFormerStreams() {
                return Duration.ofSeconds(10);
            }
        };

    }
}
