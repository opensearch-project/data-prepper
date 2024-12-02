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

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.KinesisSourceConfig;
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.KinesisStreamConfig;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KinesisMultiStreamTrackerTest {
    private static final String APPLICATION_NAME = "multi-stream-application";
    private static final String awsAccountId = "1234";
    private static final Instant streamCreationTime = Instant.now();
    private static final List<String> STREAMS_LIST = ImmutableList.of("stream-1", "stream-2", "stream-3");

    private List<StreamConfig> streamConfigList;

    private Map<String, KinesisStreamConfig> streamConfigMap;

    @Mock
    KinesisSourceConfig kinesisSourceConfig;

    @Mock
    private KinesisStreamBackoffStrategy kinesisStreamBackoffStrategy;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        List<KinesisStreamConfig> kinesisStreamConfigs = new ArrayList<>();
        streamConfigMap = new HashMap<>();
        STREAMS_LIST.forEach(stream -> {
            KinesisStreamConfig kinesisStreamConfig = mock(KinesisStreamConfig.class);
            when(kinesisStreamConfig.getName()).thenReturn(stream);
            when(kinesisStreamConfig.getInitialPosition()).thenReturn(InitialPositionInStream.LATEST);
            when(kinesisStreamBackoffStrategy.getStreamIdentifier(stream)).thenReturn(getStreamIdentifier(stream));
            kinesisStreamConfigs.add(kinesisStreamConfig);
            streamConfigMap.put(stream, kinesisStreamConfig);
        });

        when(kinesisSourceConfig.getStreams()).thenReturn(kinesisStreamConfigs);
    }

    private KinesisMultiStreamTracker createObjectUnderTest() {
        return new KinesisMultiStreamTracker(kinesisSourceConfig, APPLICATION_NAME, kinesisStreamBackoffStrategy);
    }

    @Test
    public void testStreamConfigList() {
        streamConfigList = createObjectUnderTest().streamConfigList();
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

            when(kinesisStreamBackoffStrategy.getStreamIdentifier(stream)).thenThrow(new RuntimeException());
            kinesisStreamConfigs.add(kinesisStreamConfig);
            streamConfigMap.put(stream, kinesisStreamConfig);
        });

        when(kinesisSourceConfig.getStreams()).thenReturn(kinesisStreamConfigs);
        KinesisMultiStreamTracker kinesisMultiStreamTracker = createObjectUnderTest();

        assertThrows(RuntimeException.class, kinesisMultiStreamTracker::streamConfigList);
    }

    @Test
    public void testStreamConfigWithRetries() {
        MockitoAnnotations.openMocks(this);
        List<KinesisStreamConfig> kinesisStreamConfigs = new ArrayList<>();
        streamConfigMap = new HashMap<>();
        STREAMS_LIST.forEach(stream -> {
            KinesisStreamConfig kinesisStreamConfig = mock(KinesisStreamConfig.class);
            when(kinesisStreamConfig.getName()).thenReturn(stream);
            when(kinesisStreamConfig.getInitialPosition()).thenReturn(InitialPositionInStream.LATEST);

            StreamIdentifier streamIdentifier = getStreamIdentifier(stream);
            when(kinesisStreamBackoffStrategy.getStreamIdentifier(stream)).thenReturn(streamIdentifier);
        });

        when(kinesisSourceConfig.getStreams()).thenReturn(kinesisStreamConfigs);

        KinesisMultiStreamTracker kinesisMultiStreamTracker = createObjectUnderTest();
        streamConfigList = kinesisMultiStreamTracker.streamConfigList();
        assertEquals(streamConfigMap.size(), streamConfigList.size());

        for (StreamConfig streamConfig : streamConfigList) {
            final StreamIdentifier streamIdentifier = streamConfig.streamIdentifier();
            final String stream = streamIdentifier.streamName();
            final InitialPositionInStreamExtended initialPositionInStreamExtended = streamConfig.initialPositionInStreamExtended();
            assertEquals(streamIdentifier, getStreamIdentifier(stream));
            assertEquals(initialPositionInStreamExtended, InitialPositionInStreamExtended.newInitialPosition(streamConfigMap.get(stream).getInitialPosition()));
        }
    }

    @Test
    public void formerStreamsLeasesDeletionStrategy() {

        FormerStreamsLeasesDeletionStrategy formerStreamsLeasesDeletionStrategy =
                createObjectUnderTest().formerStreamsLeasesDeletionStrategy();

        Duration duration = formerStreamsLeasesDeletionStrategy.waitPeriodToDeleteFormerStreams();

        Assertions.assertInstanceOf(FormerStreamsLeasesDeletionStrategy.AutoDetectionAndDeferredDeletionStrategy.class, formerStreamsLeasesDeletionStrategy);
        assertEquals(10, duration.getSeconds());
    }

    private StreamIdentifier getStreamIdentifier(final String streamName) {
        return StreamIdentifier.multiStreamInstance(String.join(":", awsAccountId, streamName, String.valueOf(streamCreationTime.getEpochSecond())));
    }
}
