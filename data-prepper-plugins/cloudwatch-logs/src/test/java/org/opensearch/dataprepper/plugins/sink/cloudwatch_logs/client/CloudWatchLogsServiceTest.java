/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.buffer.Buffer;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.buffer.InMemoryBuffer;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.buffer.InMemoryBufferFactory;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.CloudWatchLogsSinkConfig;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.ThresholdConfig;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.utils.CloudWatchLogsLimits;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import org.opensearch.dataprepper.plugins.dlq.DlqPushHandler;
import org.opensearch.dataprepper.model.log.JacksonLog;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doAnswer;

class CloudWatchLogsServiceTest {
    private static final int LARGE_THREAD_COUNT = 1000;
    private CloudWatchLogsClient mockClient;
    private CloudWatchLogsMetrics mockMetrics;
    private CloudWatchLogsService cloudWatchLogsService;
    private CloudWatchLogsSinkConfig mockCloudWatchLogsSinkConfig;
    private ThresholdConfig thresholdConfig;
    private CloudWatchLogsLimits cloudWatchLogsLimits;
    private InMemoryBufferFactory inMemoryBufferFactory;
    private Buffer buffer;
    private CloudWatchLogsDispatcher mockDispatcher;
    private DlqPushHandler dlqPushHandler;

    @BeforeEach
    void setUp() {
        mockCloudWatchLogsSinkConfig = mock(CloudWatchLogsSinkConfig.class);

        thresholdConfig = new ThresholdConfig();
        cloudWatchLogsLimits = new CloudWatchLogsLimits(thresholdConfig.getBatchSize(), thresholdConfig.getMaxEventSizeBytes(),
                thresholdConfig.getMaxRequestSizeBytes(), thresholdConfig.getLogSendInterval());

        mockClient = mock(CloudWatchLogsClient.class);
        mockMetrics = mock(CloudWatchLogsMetrics.class);
        inMemoryBufferFactory = new InMemoryBufferFactory();
        mockDispatcher = mock(CloudWatchLogsDispatcher.class);
        dlqPushHandler = mock(DlqPushHandler.class);
        cloudWatchLogsService = new CloudWatchLogsService(buffer,
                cloudWatchLogsLimits, mockDispatcher, null);
    }

    Collection<Record<Event>> getSampleRecordsCollectionSmall() {
        final ArrayList<Record<Event>> returnCollection = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            JacksonEvent mockJacksonEvent = (JacksonEvent) JacksonEvent.fromMessage("testMessage");
            returnCollection.add(new Record<>(mockJacksonEvent));
        }

        return returnCollection;
    }

    Collection<Record<Event>> getSampleRecordsCollection() {
        final ArrayList<Record<Event>> returnCollection = new ArrayList<>();
        for (int i = 0; i < thresholdConfig.getBatchSize(); i++) {
            JacksonEvent mockJacksonEvent = (JacksonEvent) JacksonEvent.fromMessage("testMessage");
            returnCollection.add(new Record<>(mockJacksonEvent));
        }

        return returnCollection;
    }

    Collection<Record<Event>> getSampleRecordsOfLargerSize() {
        final ArrayList<Record<Event>> returnCollection = new ArrayList<>();
        for (int i = 0; i < thresholdConfig.getBatchSize() * 2; i++) {
            JacksonEvent mockJacksonEvent = (JacksonEvent) JacksonEvent.fromMessage("a".repeat((int) (thresholdConfig.getMaxRequestSizeBytes()/24)));
            returnCollection.add(new Record<>(mockJacksonEvent));
        }

        return returnCollection;
    }

    Collection<Record<Event>> getSampleRecordsOfLimitSize() {
        final ArrayList<Record<Event>> returnCollection = new ArrayList<>();
        for (int i = 0; i < thresholdConfig.getBatchSize(); i++) {
            JacksonEvent mockJacksonEvent = (JacksonEvent) JacksonEvent.fromMessage("testMessage".repeat((int) thresholdConfig.getMaxEventSizeBytes()));
            returnCollection.add(new Record<>(mockJacksonEvent));
        }

        return returnCollection;
    }

    void setUpSpyBuffer() {
        buffer = spy(InMemoryBuffer.class);
    }

    void setUpRealBuffer() {
        buffer = inMemoryBufferFactory.getBuffer();
    }

    CloudWatchLogsService getSampleService(DlqPushHandler dlqPushHandler) {
        return new CloudWatchLogsService(buffer, cloudWatchLogsLimits, mockDispatcher, dlqPushHandler);
    }

    CloudWatchLogsService getSampleService() {
        return new CloudWatchLogsService(buffer, cloudWatchLogsLimits, mockDispatcher, null);
    }

    @Test
    void SHOULD_not_call_dispatcher_WHEN_process_log_events_called_with_small_collection() {
        setUpRealBuffer();
        cloudWatchLogsService = getSampleService();
        cloudWatchLogsService.processLogEvents(getSampleRecordsCollectionSmall());
        verify(mockDispatcher, never()).dispatchLogs(any(List.class), any(List.class));
    }

    @Test
    void SHOULD_call_dispatcher_WHEN_process_log_events_called_with_limit_sized_collection() {
        setUpRealBuffer();
        cloudWatchLogsService = getSampleService();
        cloudWatchLogsService.processLogEvents(getSampleRecordsCollection());
        verify(mockDispatcher, atLeast(1)).dispatchLogs(any(List.class), any(List.class));
    }

    @Test
    void SHOULD_call_dispatcher_WHEN_process_log_events_called_with_limit_sized_collection_with_dlq() throws Exception {
        setUpRealBuffer();
        cloudWatchLogsService = getSampleService(dlqPushHandler);
        cloudWatchLogsService.processLogEvents(getSampleRecordsCollection());
        verify(mockDispatcher, atLeast(1)).dispatchLogs(any(List.class), any(List.class));
        verify(dlqPushHandler, never()).perform(any(List.class));
    }

    @Test
    void SHOULD_call_dispatcher_WHEN_process_log_events_called_with_limit_sized_collection_with_large_record_dlq() throws Exception {
        PluginSetting pluginSetting = mock(PluginSetting.class);
        when(pluginSetting.getName()).thenReturn("test");
        when(pluginSetting.getPipelineName()).thenReturn("test");
        when(dlqPushHandler.getPluginSetting()).thenReturn(pluginSetting);
        setUpRealBuffer();
        cloudWatchLogsService = getSampleService(dlqPushHandler);
        cloudWatchLogsService.processLogEvents(List.of(getLargeRecord(2*thresholdConfig.getMaxEventSizeBytes())));
        verify(mockDispatcher, never()).dispatchLogs(any(List.class), any(List.class));
        verify(dlqPushHandler, atLeast(1)).perform(any(List.class));
    }

    @Test
    void SHOULD_call_dispatcher_WHEN_process_log_events_called_with_dispatcher_throws_sending_events_to_dlq() throws Exception {
        PluginSetting pluginSetting = mock(PluginSetting.class);
        doAnswer(a -> {
            throw new RuntimeException("failed to dispatch");
        }).when(mockDispatcher).dispatchLogs(any(List.class), any(List.class));
        when(pluginSetting.getName()).thenReturn("test");
        when(pluginSetting.getPipelineName()).thenReturn("test");
        when(dlqPushHandler.getPluginSetting()).thenReturn(pluginSetting);
        setUpRealBuffer();
        cloudWatchLogsService = getSampleService(dlqPushHandler);
        cloudWatchLogsService.processLogEvents(List.of(getLargeRecord(2*thresholdConfig.getMaxEventSizeBytes())));
        verify(mockDispatcher, never()).dispatchLogs(any(List.class), any(List.class));
        verify(dlqPushHandler, atLeast(1)).perform(any(List.class));
    }

    @Test
    void SHOULD_not_call_buffer_WHEN_process_log_events_called_with_limit_sized_records() {
        setUpSpyBuffer();
        cloudWatchLogsService = getSampleService();
        cloudWatchLogsService.processLogEvents(getSampleRecordsOfLimitSize());
        verify(buffer, never()).writeEvent(any(EventHandle.class), any(byte[].class));
    }
    
    @Test
    void SHOULD_call_buffer_WHEN_process_log_events_called_with_larger_sized_records() {
        setUpSpyBuffer();
        cloudWatchLogsService = getSampleService();
        cloudWatchLogsService.processLogEvents(getSampleRecordsOfLargerSize());
        verify(buffer, atLeast(1)).writeEvent(any(EventHandle.class), any(byte[].class));
    }

    //Multithreaded tests:
    void setUpThreadsProcessingLogsWithNormalSample(final int numberOfThreads) throws InterruptedException {
        Thread[] threads = new Thread[numberOfThreads];
        Collection<Record<Event>> sampleEvents = getSampleRecordsCollection();

        for (int i = 0; i < numberOfThreads; i++) {
            threads[i] = new Thread(() -> {
                cloudWatchLogsService.processLogEvents(sampleEvents);
            });
            threads[i].start();
        }

        for (int i = 0; i < numberOfThreads; i++) {
            threads[i].join();
        }
    }

    @Test
    void GIVEN_large_thread_count_WHEN_processing_log_events_THEN_buffer_should_be_called_large_thread_count_times() throws InterruptedException {
        setUpSpyBuffer();
        cloudWatchLogsService = getSampleService();
        setUpThreadsProcessingLogsWithNormalSample(LARGE_THREAD_COUNT);

        verify(buffer, atLeast(LARGE_THREAD_COUNT)).getBufferedData();
    }

    @Test
    void GIVEN_large_thread_count_WHEN_processing_log_events_THEN_dispatcher_should_be_called_large_thread_count_times() throws InterruptedException {
        setUpSpyBuffer();
        cloudWatchLogsService = getSampleService();
        setUpThreadsProcessingLogsWithNormalSample(LARGE_THREAD_COUNT);

        verify(mockDispatcher, atLeast(LARGE_THREAD_COUNT)).dispatchLogs(any(List.class), any(List.class));
    }

     private Record<Event> getLargeRecord(long size) {
        final Event event = JacksonLog.builder().withData(Map.of("key", RandomStringUtils.randomAlphabetic((int)size))).build();
        return new Record<>(event);
    }   
}
