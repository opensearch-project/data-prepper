package org.opensearch.dataprepper.plugins.source.kinesis;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.dataprepper.plugins.source.kinesis.configuration.KinesisSourceConfig;
import org.opensearch.dataprepper.plugins.source.kinesis.configuration.KinesisStreamConfig;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.StreamDescription;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.processor.FormerStreamsLeasesDeletionStrategy;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KinesisMultiStreamTrackerTest {
    private static final String APPLICATION_NAME = "multi-stream-application";
    private static final String awsAccountId = "1234";
    private static final String streamArnFormat = "arn:aws:kinesis:us-east-1:%s:stream/%s";
    private static final Instant streamCreationTime = Instant.now();
    private static final List<String> STREAMS_LIST = ImmutableList.of("stream-1", "stream-2", "stream-3");

    private KinesisMultiStreamTracker kinesisMultiStreamTracker;
    @Mock
    private KinesisAsyncClient kinesisClient;
    private List<StreamConfig> streamConfigList;

    private Map<String, KinesisStreamConfig> streamConfigMap;

    @Mock
    KinesisSourceConfig kinesisSourceConfig;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        List<KinesisStreamConfig> kinesisStreamConfigs = new ArrayList<>();
        streamConfigMap = new HashMap<>();
        STREAMS_LIST.forEach(stream -> {
            KinesisStreamConfig kinesisStreamConfig = mock(KinesisStreamConfig.class);
            when(kinesisStreamConfig.getName()).thenReturn(stream);
            when(kinesisStreamConfig.getInitialPosition()).thenReturn(InitialPositionInStream.LATEST);

            StreamDescription streamDescription = StreamDescription.builder()
                    .streamARN(String.format(streamArnFormat, awsAccountId, stream))
                    .streamCreationTimestamp(streamCreationTime)
                    .streamName(stream)
                    .build();

            DescribeStreamRequest describeStreamRequest = DescribeStreamRequest.builder()
                    .streamName(stream)
                    .build();

            DescribeStreamResponse describeStreamResponse = DescribeStreamResponse.builder()
                    .streamDescription(streamDescription)
                    .build();

            when(kinesisClient.describeStream(describeStreamRequest)).thenReturn(CompletableFuture.completedFuture(describeStreamResponse));
            kinesisStreamConfigs.add(kinesisStreamConfig);

            streamConfigMap.put(stream, kinesisStreamConfig);
        });

        when(kinesisSourceConfig.getStreams()).thenReturn(kinesisStreamConfigs);
        kinesisMultiStreamTracker = new KinesisMultiStreamTracker(kinesisClient, kinesisSourceConfig, APPLICATION_NAME);
    }

    @Test
    public void testStreamConfigList() {
        streamConfigList = kinesisMultiStreamTracker.streamConfigList();
        assertEquals(kinesisSourceConfig.getStreams().size(), streamConfigList.size());

        int totalStreams = streamConfigList.size();
        for (int i=0; i<totalStreams; i++) {
            final StreamIdentifier streamIdentifier = streamConfigList.get(i).streamIdentifier();
            final String stream = streamIdentifier.streamName();
            final InitialPositionInStreamExtended initialPositionInStreamExtended = streamConfigList.get(i).initialPositionInStreamExtended();
            assertEquals(streamIdentifier, getStreamIdentifier(stream));
            assertEquals(initialPositionInStreamExtended, InitialPositionInStreamExtended.newInitialPosition(streamConfigMap.get(stream).getInitialPosition()));
        }
    }

    @Test
    public void testStreamConfigListWithException() {

        MockitoAnnotations.openMocks(this);
        List<KinesisStreamConfig> kinesisStreamConfigs = new ArrayList<>();
        streamConfigMap = new HashMap<>();
        STREAMS_LIST.forEach(stream -> {
            KinesisStreamConfig kinesisStreamConfig = mock(KinesisStreamConfig.class);
            when(kinesisStreamConfig.getName()).thenReturn(stream);
            when(kinesisStreamConfig.getInitialPosition()).thenReturn(InitialPositionInStream.LATEST);

            DescribeStreamRequest describeStreamRequest = DescribeStreamRequest.builder()
                    .streamName(stream)
                    .build();

            when(kinesisClient.describeStream(describeStreamRequest)).thenThrow(new RuntimeException());
            kinesisStreamConfigs.add(kinesisStreamConfig);

            streamConfigMap.put(stream, kinesisStreamConfig);
        });

        when(kinesisSourceConfig.getStreams()).thenReturn(kinesisStreamConfigs);
        kinesisMultiStreamTracker = new KinesisMultiStreamTracker(kinesisClient, kinesisSourceConfig, APPLICATION_NAME);

        assertThrows(RuntimeException.class, () -> kinesisMultiStreamTracker.streamConfigList());
    }

    @Test
    public void formerStreamsLeasesDeletionStrategy() {

        FormerStreamsLeasesDeletionStrategy formerStreamsLeasesDeletionStrategy =
                kinesisMultiStreamTracker.formerStreamsLeasesDeletionStrategy();

        Duration duration = formerStreamsLeasesDeletionStrategy.waitPeriodToDeleteFormerStreams();

        Assertions.assertTrue(formerStreamsLeasesDeletionStrategy instanceof
                FormerStreamsLeasesDeletionStrategy.AutoDetectionAndDeferredDeletionStrategy);
        assertEquals(10, duration.getSeconds());
    }

    private StreamIdentifier getStreamIdentifier(final String streamName) {
        return StreamIdentifier.multiStreamInstance(String.join(":", awsAccountId, streamName, String.valueOf(streamCreationTime.getEpochSecond())));
    }
}
