/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.client;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.sink.buffer.Buffer;
import org.opensearch.dataprepper.plugins.sink.buffer.InMemoryBufferFactory;
import org.opensearch.dataprepper.plugins.sink.client.CloudWatchLogsDispatcher.CloudWatchLogsDispatcherBuilder;
import org.opensearch.dataprepper.plugins.sink.config.CloudWatchLogsSinkConfig;
import org.opensearch.dataprepper.plugins.sink.config.ThresholdConfig;
import org.opensearch.dataprepper.plugins.sink.packaging.ThreadTaskEvents;
import org.opensearch.dataprepper.plugins.sink.utils.CloudWatchLogsLimits;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.when;

public class CloudWatchLogsServiceTest {
    private static final int MAX_QUEUE_SIZE = 100;
    private CloudWatchLogsClient mockClient;
    private CloudWatchLogsMetrics mockMetrics;
    private BlockingQueue<ThreadTaskEvents> testQueue;
    private CloudWatchLogsService cloudWatchLogsService;
    private CloudWatchLogsDispatcherBuilder mockDispatchBuilder;
    private CloudWatchLogsSinkConfig cloudWatchLogsSinkConfig;
    private ThresholdConfig thresholdConfig;
    private CloudWatchLogsLimits cloudWatchLogsLimits;
    private InMemoryBufferFactory inMemoryBufferFactory;
    private Buffer buffer;
    private CloudWatchLogsDispatcher testDispatcher;
    private final String logGroup = "testGroup";
    private final String logStream = "testStream";

    @BeforeEach
    void setUp() {
        cloudWatchLogsSinkConfig = mock(CloudWatchLogsSinkConfig.class);

        thresholdConfig = new ThresholdConfig(); //Class can stay as is.
        cloudWatchLogsLimits = new CloudWatchLogsLimits(thresholdConfig.getBatchSize(), thresholdConfig.getMaxEventSizeBytes(),
                thresholdConfig.getMaxRequestSize(), thresholdConfig.getLogSendInterval());

        mockClient = mock(CloudWatchLogsClient.class);
        mockMetrics = mock(CloudWatchLogsMetrics.class);
        inMemoryBufferFactory = new InMemoryBufferFactory();
        buffer = inMemoryBufferFactory.getBuffer();
        testDispatcher = mock(CloudWatchLogsDispatcher.class);
        testQueue = new ArrayBlockingQueue<>(MAX_QUEUE_SIZE);

        mockDispatchBuilder = mock(CloudWatchLogsDispatcherBuilder.class, RETURNS_DEEP_STUBS);
        when(mockDispatchBuilder.taskQueue(any(BlockingQueue.class))).thenReturn(mockDispatchBuilder);
        when(mockDispatchBuilder.cloudWatchLogsClient(any(CloudWatchLogsClient.class))).thenReturn(mockDispatchBuilder);
        when(mockDispatchBuilder.cloudWatchLogsMetrics(any(CloudWatchLogsMetrics.class))).thenReturn(mockDispatchBuilder);
        when(mockDispatchBuilder.logGroup(anyString())).thenReturn(mockDispatchBuilder);
        when(mockDispatchBuilder.logStream(anyString())).thenReturn(mockDispatchBuilder);
        when(mockDispatchBuilder.retryCount(anyInt())).thenReturn(mockDispatchBuilder);
        when(mockDispatchBuilder.backOffTimeBase(anyInt())).thenReturn(mockDispatchBuilder);
        when(mockDispatchBuilder.taskQueue(any(BlockingQueue.class))
                .cloudWatchLogsClient(any(CloudWatchLogsClient.class))
                .cloudWatchLogsMetrics(any(CloudWatchLogsMetrics.class)).logGroup(logGroup)
                .logStream(anyString())
                .retryCount(anyInt())
                .backOffTimeBase(anyLong())
                .build()).thenReturn(testDispatcher);

        cloudWatchLogsService = new CloudWatchLogsService(buffer, mockClient, mockMetrics,
                cloudWatchLogsLimits, testQueue,
                logGroup, logStream,
                thresholdConfig.getRetryCount(), thresholdConfig.getBackOffTime());
    }

    Collection<Record<Event>> getSampleRecordsLess() {
        final ArrayList<Record<Event>> returnCollection = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            JacksonEvent mockJacksonEvent = (JacksonEvent) JacksonEvent.fromMessage("testMessage");
            final EventHandle mockEventHandle = mock(EventHandle.class);
            mockJacksonEvent.setEventHandle(mockEventHandle);
            returnCollection.add(new Record<>(mockJacksonEvent));
        }

        return returnCollection;
    }

    Collection<Record<Event>> getSampleRecords() {
        final ArrayList<Record<Event>> returnCollection = new ArrayList<>();
        for (int i = 0; i < thresholdConfig.getBatchSize(); i++) {
            JacksonEvent mockJacksonEvent = (JacksonEvent) JacksonEvent.fromMessage("testMessage");
            final EventHandle mockEventHandle = mock(EventHandle.class);
            mockJacksonEvent.setEventHandle(mockEventHandle);
            returnCollection.add(new Record<>(mockJacksonEvent));
        }

        return returnCollection;
    }

    Collection<Record<Event>> getSampleRecordsLarge() {
        final ArrayList<Record<Event>> returnCollection = new ArrayList<>();
        for (int i = 0; i < (thresholdConfig.getBatchSize() * 4); i++) {
            JacksonEvent mockJacksonEvent = (JacksonEvent) JacksonEvent.fromMessage("testMessage");
            final EventHandle mockEventHandle = mock(EventHandle.class);
            mockJacksonEvent.setEventHandle(mockEventHandle);
            returnCollection.add(new Record<>(mockJacksonEvent));
        }

        return returnCollection;
    }

    @Test
    void check_dispatcher_run_was_not_called() {
        cloudWatchLogsService.processLogEvents(getSampleRecordsLess());
        verify(mockClient, never()).putLogEvents(any(PutLogEventsRequest.class));
    }

    @Test
    void check_dispatcher_run_was_called_test() throws InterruptedException {
        cloudWatchLogsService.processLogEvents(getSampleRecords());
        Thread.sleep(100);
        verify(mockClient, atLeastOnce()).putLogEvents(any(PutLogEventsRequest.class));
    }

    @Test
    void check_dispatcher_run_called_heavy_load() throws InterruptedException {
        cloudWatchLogsService.processLogEvents(getSampleRecordsLarge());
        Thread.sleep(100);
        verify(mockClient, atLeast(4)).putLogEvents(any(PutLogEventsRequest.class));
    }
}
