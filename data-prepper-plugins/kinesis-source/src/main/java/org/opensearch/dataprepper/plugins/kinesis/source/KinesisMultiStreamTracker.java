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

import org.opensearch.dataprepper.plugins.kinesis.source.apihandler.KinesisClientApiHandler;
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.ConsumerStrategy;
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.KinesisSourceConfig;
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.KinesisStreamConfig;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.processor.FormerStreamsLeasesDeletionStrategy;
import software.amazon.kinesis.processor.MultiStreamTracker;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class KinesisMultiStreamTracker implements MultiStreamTracker {
    private final KinesisSourceConfig sourceConfig;
    private final String applicationName;
    private final KinesisClientApiHandler kinesisClientAPIHandler;

    public KinesisMultiStreamTracker(final KinesisSourceConfig sourceConfig, final String applicationName, final KinesisClientApiHandler kinesisClientAPIHandler) {
        this.sourceConfig = sourceConfig;
        this.applicationName = applicationName;
        this.kinesisClientAPIHandler = kinesisClientAPIHandler;
    }

    @Override
    public List<StreamConfig> streamConfigList() {
        return sourceConfig.getStreams().stream()
                .map(this::createStreamConfig)
                .collect(Collectors.toList());
    }

    private StreamConfig createStreamConfig(KinesisStreamConfig kinesisStreamConfig) {
        StreamIdentifier streamIdentifier = getStreamIdentifier(kinesisStreamConfig);
        InitialPositionInStreamExtended initialPosition = getInitialPositionExtended(kinesisStreamConfig);

        // if the consumer strategy is polling, skip look up for consumer
        if (sourceConfig.getConsumerStrategy() == ConsumerStrategy.POLLING) {
            return new StreamConfig(streamIdentifier, initialPosition);
        }

        // If stream arn and consumer arn is present, create a stream config based on the configured values
        if (Objects.nonNull(kinesisStreamConfig.getStreamArn()) && Objects.nonNull(kinesisStreamConfig.getConsumerArn())) {
            return new StreamConfig(streamIdentifier, initialPosition, kinesisStreamConfig.getConsumerArn());
        }

        // If stream arn is provided, lookup consumer arn based on the consumer name which is the data prepper application name
        if (Objects.nonNull(kinesisStreamConfig.getStreamArn())) {
            String consumerArn = kinesisClientAPIHandler.getConsumerArnForStream(kinesisStreamConfig.getStreamArn(), this.applicationName);
            return new StreamConfig(streamIdentifier, initialPosition, consumerArn);
        }
        // Default case
        return new StreamConfig(streamIdentifier, initialPosition);
    }

    private StreamIdentifier getStreamIdentifier(final KinesisStreamConfig kinesisStreamConfig) {
        final String streamArn = kinesisStreamConfig.getStreamArn();
        final String streamName = kinesisStreamConfig.getName();

        if (Objects.isNull(streamArn) && Objects.isNull(streamName)) {
            throw new IllegalArgumentException("Either ARN or name must be specified for Kinesis stream configuration");
        }

        return kinesisClientAPIHandler.getStreamIdentifier(streamArn != null ? streamArn : streamName);
    }

    private InitialPositionInStreamExtended getInitialPositionExtended(KinesisStreamConfig kinesisStreamConfig) {
        if (kinesisStreamConfig.getInitialPosition() == InitialPositionInStream.AT_TIMESTAMP) {
            Instant timestamp;
            if (Objects.nonNull(kinesisStreamConfig.getInitialTimestamp())) {
                timestamp = kinesisStreamConfig.getInitialTimestamp().atOffset(ZoneOffset.UTC).toInstant();
            } else if (Objects.nonNull(kinesisStreamConfig.getRange())) {
                timestamp = Instant.now().minus(kinesisStreamConfig.getRange());
            } else {
                throw new IllegalArgumentException("Either initial_timestamp or range must be specified when using AT_TIMESTAMP initial_position");
            }
            return InitialPositionInStreamExtended.newInitialPositionAtTimestamp(Date.from(timestamp));
        }
        return InitialPositionInStreamExtended.newInitialPosition(kinesisStreamConfig.getInitialPosition());
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
