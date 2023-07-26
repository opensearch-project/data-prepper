/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.client;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.plugins.sink.config.ThresholdConfig;
import org.opensearch.dataprepper.plugins.sink.packaging.ThreadTaskEvents;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.CloudWatchLogsException;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;

import java.util.ArrayList;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

 public class CloudWatchLogsDispatcherTest {
    private CloudWatchLogsDispatcher cloudWatchLogsDispatcher;
    private  CloudWatchLogsClient cloudWatchLogsClient;
    private  CloudWatchLogsMetrics cloudWatchLogsMetrics;
    private PluginMetrics pluginMetrics;
    private Counter requestSuccessCounter;
    private Counter requestFailCounter;
    private Counter successEventCounter;
    private Counter failedEventCounter;
    private static final String LOG_GROUP = "testGroup";
    private static final String LOG_STREAM = "testStream";
    private static final String TEST_STRING = "testMessage";

    @BeforeEach
    void setUp() throws InterruptedException {
        cloudWatchLogsClient = mock(CloudWatchLogsClient.class);

        pluginMetrics = mock(PluginMetrics.class);
        requestSuccessCounter = mock(Counter.class);
        requestFailCounter = mock(Counter.class);
        successEventCounter = mock(Counter.class);
        failedEventCounter = mock(Counter.class);

        cloudWatchLogsMetrics = mock(CloudWatchLogsMetrics.class);

        when(pluginMetrics.counter(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_EVENTS_SUCCEEDED)).thenReturn(successEventCounter);
        when(pluginMetrics.counter(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_REQUESTS_SUCCEEDED)).thenReturn(requestSuccessCounter);
        when(pluginMetrics.counter(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_EVENTS_FAILED)).thenReturn(failedEventCounter);
        when(pluginMetrics.counter(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_REQUESTS_FAILED)).thenReturn(requestFailCounter);
    }

    ThreadTaskEvents getSampleBufferedData() {
        final ArrayList<byte[]> returnCollection = new ArrayList<>();
        final ArrayList<EventHandle> eventHandles = new ArrayList<>();

        for (int i = 0; i < ThresholdConfig.DEFAULT_BATCH_SIZE; i++) {
            returnCollection.add(new String(TEST_STRING).getBytes());
            final EventHandle mockEventHandle = mock(EventHandle.class);
            eventHandles.add(mockEventHandle);
        }

        return new ThreadTaskEvents(returnCollection, eventHandles);
    }

    CloudWatchLogsDispatcher getCloudWatchLogsDispatcher() {
        return new CloudWatchLogsDispatcher(cloudWatchLogsClient,
                cloudWatchLogsMetrics, LOG_GROUP, LOG_STREAM, ThresholdConfig.DEFAULT_RETRY_COUNT,
                ThresholdConfig.DEFAULT_BACKOFF_TIME);
    }

    void establishFailingClientWithCloudWatchLogsExcept() {
        when(cloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class))).thenThrow(CloudWatchLogsException.class);
    }

    void establishFailingClientWithSdkClientExcept() {
        when(cloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class))).thenThrow(SdkClientException.class);
    }

    void setUpInterruptedQueueException() throws InterruptedException {
//        when(mockTaskQueue.take()).thenThrow(InterruptedException.class);
    }

    @Test
    void check_successful_transmission_test() {
        cloudWatchLogsDispatcher = getCloudWatchLogsDispatcher();
        cloudWatchLogsDispatcher.prepareInputLogEvents(getSampleBufferedData());
        cloudWatchLogsDispatcher.dispatchLogs();

        verify(cloudWatchLogsMetrics, atLeastOnce()).increaseRequestSuccessCounter(1);
        verify(cloudWatchLogsMetrics, atLeastOnce()).increaseLogEventSuccessCounter(ThresholdConfig.DEFAULT_BATCH_SIZE);
    }

    @Test
    void check_unsuccesful_transmission_with_cloudwatchlogsexcept_test() {
        establishFailingClientWithCloudWatchLogsExcept();
        cloudWatchLogsDispatcher = getCloudWatchLogsDispatcher();
        cloudWatchLogsDispatcher.run();

        verify(cloudWatchLogsMetrics, times(ThresholdConfig.DEFAULT_RETRY_COUNT)).increaseRequestFailCounter(1);
        verify(cloudWatchLogsMetrics, atLeastOnce()).increaseLogEventFailCounter(ThresholdConfig.DEFAULT_BATCH_SIZE);
    }

    @Test
    void check_unsuccesful_transmission_with_sdkexcept_test() {
        establishFailingClientWithSdkClientExcept();
        cloudWatchLogsDispatcher = getCloudWatchLogsDispatcher();
        cloudWatchLogsDispatcher.run();

        verify(cloudWatchLogsMetrics, times(ThresholdConfig.DEFAULT_RETRY_COUNT)).increaseRequestFailCounter(1);
        verify(cloudWatchLogsMetrics, atLeastOnce()).increaseLogEventFailCounter(ThresholdConfig.DEFAULT_BATCH_SIZE);
    }
}
