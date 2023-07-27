/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.client;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.sink.buffer.Buffer;
import org.opensearch.dataprepper.plugins.sink.buffer.InMemoryBuffer;
import org.opensearch.dataprepper.plugins.sink.buffer.InMemoryBufferFactory;
import org.opensearch.dataprepper.plugins.sink.config.CloudWatchLogsSinkConfig;
import org.opensearch.dataprepper.plugins.sink.config.ThresholdConfig;
import org.opensearch.dataprepper.plugins.sink.utils.CloudWatchLogsLimits;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.never;

public class CloudWatchLogsServiceTest {
    private static int SMALL_THREAD_COUNT = 50;
    private static int MEDIUM_THREAD_COUNT = 100;
    private static int HIGH_THREAD_COUNT = 500;
    private static int LARGE_THREAD_COUNT = 1000;
    private CloudWatchLogsClient mockClient;
    private CloudWatchLogsMetrics mockMetrics;
    private CloudWatchLogsService cloudWatchLogsService;
    private CloudWatchLogsSinkConfig cloudWatchLogsSinkConfig;
    private ThresholdConfig thresholdConfig;
    private CloudWatchLogsLimits cloudWatchLogsLimits;
    private InMemoryBufferFactory inMemoryBufferFactory;
    private Buffer buffer;
    private CloudWatchLogsDispatcher testDispatcher;

    @BeforeEach
    void setUp() {
        cloudWatchLogsSinkConfig = mock(CloudWatchLogsSinkConfig.class);

        thresholdConfig = new ThresholdConfig(); //Class can stay as is.
        cloudWatchLogsLimits = new CloudWatchLogsLimits(thresholdConfig.getBatchSize(), thresholdConfig.getMaxEventSizeBytes(),
                thresholdConfig.getMaxRequestSize(), thresholdConfig.getLogSendInterval());

        mockClient = mock(CloudWatchLogsClient.class);
        mockMetrics = mock(CloudWatchLogsMetrics.class);
        inMemoryBufferFactory = new InMemoryBufferFactory();
        testDispatcher = mock(CloudWatchLogsDispatcher.class);
        cloudWatchLogsService = new CloudWatchLogsService(buffer,
                cloudWatchLogsLimits, testDispatcher);
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

    void setUpSpyBuffer() {
        buffer = spy(InMemoryBuffer.class);
    }

    void setUpRealBuffer() {
        buffer = inMemoryBufferFactory.getBuffer();
    }

    CloudWatchLogsService getSampleService() {
        return new CloudWatchLogsService(buffer, cloudWatchLogsLimits, testDispatcher);
    }

    @Test
    void check_dispatcher_run_was_not_called() {
        setUpRealBuffer();
        cloudWatchLogsService = getSampleService();
        cloudWatchLogsService.processLogEvents(getSampleRecordsLess());
        verify(testDispatcher, never()).dispatchLogs(any(List.class), any(Collection.class));
    }

    @Test
    void check_dispatcher_run_was_called_test() throws InterruptedException {
        setUpRealBuffer();
        cloudWatchLogsService = getSampleService();
        cloudWatchLogsService.processLogEvents(getSampleRecords());
        verify(testDispatcher, atLeast(1)).dispatchLogs(any(List.class), any(Collection.class));
    }

    //Multithreaded tests:
    void testThreadsProcessingLogsWithNormalSample(final int numberOfThreads) throws InterruptedException {
        Thread[] threads = new Thread[numberOfThreads];
        Collection<Record<Event>> sampleEvents = getSampleRecords();

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
    void test_buffer_access_with_small_thread_count_test() throws InterruptedException {
        setUpSpyBuffer();
        cloudWatchLogsService = getSampleService();
        testThreadsProcessingLogsWithNormalSample(SMALL_THREAD_COUNT);

        verify(buffer, atLeast(SMALL_THREAD_COUNT)).getBufferedData();
    }

    @Test
    void test_buffer_access_with_medium_thread_count_test() throws InterruptedException {
        setUpSpyBuffer();
        cloudWatchLogsService = getSampleService();
        testThreadsProcessingLogsWithNormalSample(MEDIUM_THREAD_COUNT);

        verify(buffer, atLeast(MEDIUM_THREAD_COUNT)).getBufferedData();
    }

    @Test
    void test_buffer_access_with_high_thread_count_test() throws InterruptedException {
        setUpSpyBuffer();
        cloudWatchLogsService = getSampleService();
        testThreadsProcessingLogsWithNormalSample(HIGH_THREAD_COUNT);

        verify(buffer, atLeast(HIGH_THREAD_COUNT)).getBufferedData();
    }

    @Test
    void test_buffer_access_with_large_thread_count_test() throws InterruptedException {
        setUpSpyBuffer();
        cloudWatchLogsService = getSampleService();
        testThreadsProcessingLogsWithNormalSample(LARGE_THREAD_COUNT);

        verify(buffer, atLeast(LARGE_THREAD_COUNT)).getBufferedData();
    }

    @Test
    void test_dispatcher_access_with_small_thread_count_test() throws InterruptedException {
        setUpSpyBuffer();
        cloudWatchLogsService = getSampleService();
        testThreadsProcessingLogsWithNormalSample(SMALL_THREAD_COUNT);

        verify(testDispatcher, atLeast(SMALL_THREAD_COUNT)).dispatchLogs(any(List.class), any(Collection.class));
    }

    @Test
    void test_dispatcher_access_with_medium_thread_count_test() throws InterruptedException {
        setUpSpyBuffer();
        cloudWatchLogsService = getSampleService();
        testThreadsProcessingLogsWithNormalSample(MEDIUM_THREAD_COUNT);

        verify(testDispatcher, atLeast(MEDIUM_THREAD_COUNT)).dispatchLogs(any(List.class), any(Collection.class));
    }

    @Test
    void test_dispatcher_access_with_high_thread_count_test() throws InterruptedException {
        setUpSpyBuffer();
        cloudWatchLogsService = getSampleService();
        testThreadsProcessingLogsWithNormalSample(HIGH_THREAD_COUNT);

        verify(testDispatcher, atLeast(HIGH_THREAD_COUNT)).dispatchLogs(any(List.class), any(Collection.class));
    }

    @Test
    void test_dispatcher_access_with_large_thread_count_test() throws InterruptedException {
        setUpSpyBuffer();
        cloudWatchLogsService = getSampleService();
        testThreadsProcessingLogsWithNormalSample(LARGE_THREAD_COUNT);

        verify(testDispatcher, atLeast(LARGE_THREAD_COUNT)).dispatchLogs(any(List.class), any(Collection.class));
    }
}
