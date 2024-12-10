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

import org.opensearch.dataprepper.plugins.kinesis.source.configuration.KinesisSourceConfig;
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.KinesisStreamConfig;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.processor.FormerStreamsLeasesDeletionStrategy;
import software.amazon.kinesis.processor.MultiStreamTracker;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

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
    public List<StreamConfig> streamConfigList()  {
        List<StreamConfig> streamConfigList = new ArrayList<>();
        for (KinesisStreamConfig kinesisStreamConfig : sourceConfig.getStreams()) {
            StreamConfig streamConfig = getStreamConfig(kinesisStreamConfig);
            streamConfigList.add(streamConfig);
        }
        return streamConfigList;
    }

    private StreamConfig getStreamConfig(KinesisStreamConfig kinesisStreamConfig) {
        StreamIdentifier sourceStreamIdentifier = kinesisClientAPIHandler.getStreamIdentifier(kinesisStreamConfig.getName());
        return new StreamConfig(sourceStreamIdentifier,
                InitialPositionInStreamExtended.newInitialPosition(kinesisStreamConfig.getInitialPosition()));
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
