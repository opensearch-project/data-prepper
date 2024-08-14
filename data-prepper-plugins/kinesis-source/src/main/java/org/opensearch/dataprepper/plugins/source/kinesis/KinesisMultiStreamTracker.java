package org.opensearch.dataprepper.plugins.source.kinesis;

import org.opensearch.dataprepper.plugins.source.kinesis.configuration.KinesisSourceConfig;
import org.opensearch.dataprepper.plugins.source.kinesis.configuration.KinesisStreamConfig;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.StreamDescription;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.processor.FormerStreamsLeasesDeletionStrategy;
import software.amazon.kinesis.processor.MultiStreamTracker;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;


public class KinesisMultiStreamTracker implements MultiStreamTracker {
    private static final String COLON = ":";

    private final KinesisAsyncClient kinesisClient;
    private final KinesisSourceConfig sourceConfig;
    private final String applicationName;

    public KinesisMultiStreamTracker(KinesisAsyncClient kinesisClient, final KinesisSourceConfig sourceConfig, final String applicationName) {
        this.kinesisClient = kinesisClient;
        this.sourceConfig = sourceConfig;
        this.applicationName = applicationName;
    }

    @Override
    public List<StreamConfig> streamConfigList()  {
        List<StreamConfig> streamConfigList = new ArrayList<>();
        for (KinesisStreamConfig kinesisStreamConfig : sourceConfig.getStreams()) {
            StreamConfig streamConfig;
            try {
                streamConfig = getStreamConfig(kinesisStreamConfig);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            streamConfigList.add(streamConfig);
        }
        return streamConfigList;
    }

    private StreamConfig getStreamConfig(KinesisStreamConfig kinesisStreamConfig) throws Exception {
        StreamIdentifier sourceStreamIdentifier = getStreamIdentifier(kinesisStreamConfig);
        return new StreamConfig(sourceStreamIdentifier,
                InitialPositionInStreamExtended.newInitialPosition(kinesisStreamConfig.getInitialPosition()));
    }

    private StreamIdentifier getStreamIdentifier(KinesisStreamConfig kinesisStreamConfig) throws Exception {
        DescribeStreamRequest describeStreamRequest = DescribeStreamRequest.builder()
                .streamName(kinesisStreamConfig.getName())
                .build();
        DescribeStreamResponse describeStreamResponse = kinesisClient.describeStream(describeStreamRequest).get();
        String streamIdentifierString = getStreamIdentifierString(describeStreamResponse.streamDescription());
        return StreamIdentifier.multiStreamInstance(streamIdentifierString);
    }

    private String getStreamIdentifierString(StreamDescription streamDescription) {
        String accountId = streamDescription.streamARN().split(COLON)[4];
        long creationEpochSecond = streamDescription.streamCreationTimestamp().getEpochSecond();
        return String.join(COLON, accountId, streamDescription.streamName(), String.valueOf(creationEpochSecond));
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
