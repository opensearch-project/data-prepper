/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.ThresholdConfig;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.RejectedLogEventsInfo;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.cloudwatchlogs.model.CloudWatchLogsException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.never;

class CloudWatchLogsDispatcherTest {
    private CloudWatchLogsDispatcher cloudWatchLogsDispatcher;
    private CloudWatchLogsClient mockCloudWatchLogsClient;
    private CloudWatchLogsMetrics mockCloudWatchLogsMetrics;
    private Executor mockExecutor;
    private static final int RETRY_COUNT = 5;
    private static final String LOG_GROUP = "testGroup";
    private static final String LOG_STREAM = "testStream";
    private static final String TEST_STRING = "testMessage";

    @BeforeEach
    void setUp() {
        mockCloudWatchLogsClient = mock(CloudWatchLogsClient.class);
        mockCloudWatchLogsMetrics = mock(CloudWatchLogsMetrics.class);
        mockExecutor = mock(Executor.class);
    }

    Collection<byte[]> getSampleBufferedData() {
        final ArrayList<byte[]> returnCollection = new ArrayList<>();

        for (int i = 0; i < ThresholdConfig.DEFAULT_BATCH_SIZE; i++) {
            returnCollection.add(new String(TEST_STRING).getBytes());
        }

        return returnCollection;
    }

    List<EventHandle> getSampleEventHandles() {
        final ArrayList<EventHandle> eventHandles = new ArrayList<>();

        for (int i = 0; i < ThresholdConfig.DEFAULT_BATCH_SIZE; i++) {
            final EventHandle mockEventHandle = mock(EventHandle.class);
            eventHandles.add(mockEventHandle);
        }

        return eventHandles;
    }

    CloudWatchLogsDispatcher getCloudWatchLogsDispatcher() {
        return CloudWatchLogsDispatcher.builder()
                .cloudWatchLogsClient(mockCloudWatchLogsClient)
                .cloudWatchLogsMetrics(mockCloudWatchLogsMetrics)
                .executor(mockExecutor)
                .logGroup(LOG_GROUP)
                .logStream(LOG_STREAM)
                .retryCount(RETRY_COUNT)
                .build();
    }

    private void executeDispatcherRunnable() {
        ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(mockExecutor).execute(runnableCaptor.capture());
        runnableCaptor.getValue().run();
    }

    @Test
    void GIVEN_valid_input_log_events_SHOULD_call_executor() {
        cloudWatchLogsDispatcher = getCloudWatchLogsDispatcher();

        final List<EventHandle> eventHandles = getSampleEventHandles();
        final PutLogEventsResponse response = mock(PutLogEventsResponse.class);
        when(response.rejectedLogEventsInfo()).thenReturn(null);
        when(mockCloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class))).thenReturn(response);

        List<InputLogEvent> inputLogEventList = cloudWatchLogsDispatcher.prepareInputLogEvents(getSampleBufferedData());
        cloudWatchLogsDispatcher.dispatchLogs(inputLogEventList, eventHandles);

        executeDispatcherRunnable();

        eventHandles.forEach(eventHandle -> verify(eventHandle).release(true));
    }

    @Test
    void GIVEN_too_old_events_SHOULD_not_release_old_events() {
        cloudWatchLogsDispatcher = getCloudWatchLogsDispatcher();

        final List<EventHandle> eventHandles = getSampleEventHandles();
        final PutLogEventsResponse response = mock(PutLogEventsResponse.class);
        final RejectedLogEventsInfo rejectedInfo = mock(RejectedLogEventsInfo.class);

        // Set first 2 events as too old
        when(rejectedInfo.tooOldLogEventEndIndex()).thenReturn(2);
        when(rejectedInfo.tooNewLogEventStartIndex()).thenReturn(null);
        when(response.rejectedLogEventsInfo()).thenReturn(rejectedInfo);
        when(mockCloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class))).thenReturn(response);

        List<InputLogEvent> inputLogEventList = cloudWatchLogsDispatcher.prepareInputLogEvents(getSampleBufferedData());
        cloudWatchLogsDispatcher.dispatchLogs(inputLogEventList, eventHandles);

        executeDispatcherRunnable();

        // First two events should not be released
        verify(eventHandles.get(0), never()).release(true);
        verify(eventHandles.get(1), never()).release(true);

        // Remaining events should be released
        for (int i = 2; i < eventHandles.size(); i++) {
            verify(eventHandles.get(i)).release(true);
        }
    }

    @Test
    void GIVEN_too_new_events_SHOULD_not_release_new_events() {
        cloudWatchLogsDispatcher = getCloudWatchLogsDispatcher();

        final List<EventHandle> eventHandles = getSampleEventHandles();
        final PutLogEventsResponse response = mock(PutLogEventsResponse.class);
        final RejectedLogEventsInfo rejectedInfo = mock(RejectedLogEventsInfo.class);

        // Set last 3 events as too new
        when(rejectedInfo.tooOldLogEventEndIndex()).thenReturn(null);
        when(rejectedInfo.tooNewLogEventStartIndex()).thenReturn(eventHandles.size() - 3);
        when(response.rejectedLogEventsInfo()).thenReturn(rejectedInfo);
        when(mockCloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class))).thenReturn(response);

        List<InputLogEvent> inputLogEventList = cloudWatchLogsDispatcher.prepareInputLogEvents(getSampleBufferedData());
        cloudWatchLogsDispatcher.dispatchLogs(inputLogEventList, eventHandles);

        executeDispatcherRunnable();

        // Events before tooNewStartIndex should be released
        for (int i = 0; i < eventHandles.size() - 3; i++) {
            verify(eventHandles.get(i)).release(true);
        }

        // Last three events should not be released
        for (int i = eventHandles.size() - 3; i < eventHandles.size(); i++) {
            verify(eventHandles.get(i), never()).release(true);
        }
    }

    @Test
    void GIVEN_both_old_and_new_rejected_events_SHOULD_only_release_valid_events() {
        cloudWatchLogsDispatcher = getCloudWatchLogsDispatcher();

        final List<EventHandle> eventHandles = getSampleEventHandles();
        final PutLogEventsResponse response = mock(PutLogEventsResponse.class);
        final RejectedLogEventsInfo rejectedInfo = mock(RejectedLogEventsInfo.class);

        // Set first 2 events as too old and last 2 as too new
        when(rejectedInfo.tooOldLogEventEndIndex()).thenReturn(2);
        when(rejectedInfo.tooNewLogEventStartIndex()).thenReturn(eventHandles.size() - 2);
        when(response.rejectedLogEventsInfo()).thenReturn(rejectedInfo);
        when(mockCloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class))).thenReturn(response);

        List<InputLogEvent> inputLogEventList = cloudWatchLogsDispatcher.prepareInputLogEvents(getSampleBufferedData());
        cloudWatchLogsDispatcher.dispatchLogs(inputLogEventList, eventHandles);

        executeDispatcherRunnable();

        // First two events should not be released (too old)
        verify(eventHandles.get(0), never()).release(true);
        verify(eventHandles.get(1), never()).release(true);

        // Middle events should be released
        for (int i = 2; i < eventHandles.size() - 2; i++) {
            verify(eventHandles.get(i)).release(true);
        }

        // Last two events should not be released (too new)
        verify(eventHandles.get(eventHandles.size() - 2), never()).release(true);
        verify(eventHandles.get(eventHandles.size() - 1), never()).release(true);
    }

    @Test
    void GIVEN_client_exception_SHOULD_retry() {
        cloudWatchLogsDispatcher = getCloudWatchLogsDispatcher();

        final List<EventHandle> eventHandles = getSampleEventHandles();
        when(mockCloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class)))
            .thenThrow(SdkClientException.create("Test exception"))
            .thenReturn(mock(PutLogEventsResponse.class));

        List<InputLogEvent> inputLogEventList = cloudWatchLogsDispatcher.prepareInputLogEvents(getSampleBufferedData());
        cloudWatchLogsDispatcher.dispatchLogs(inputLogEventList, eventHandles);

        executeDispatcherRunnable();

        verify(mockCloudWatchLogsMetrics, times(1)).increaseRequestFailCounter(1);
        verify(mockCloudWatchLogsMetrics, times(1)).increaseRequestSuccessCounter(1);
    }

    @Test
    void GIVEN_cloudwatch_exception_SHOULD_retry() {
        cloudWatchLogsDispatcher = getCloudWatchLogsDispatcher();

        final List<EventHandle> eventHandles = getSampleEventHandles();
        when(mockCloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class)))
            .thenThrow(CloudWatchLogsException.class)
            .thenReturn(mock(PutLogEventsResponse.class));

        List<InputLogEvent> inputLogEventList = cloudWatchLogsDispatcher.prepareInputLogEvents(getSampleBufferedData());
        cloudWatchLogsDispatcher.dispatchLogs(inputLogEventList, eventHandles);

        executeDispatcherRunnable();

        verify(mockCloudWatchLogsMetrics, times(1)).increaseRequestFailCounter(1);
        verify(mockCloudWatchLogsMetrics, times(1)).increaseRequestSuccessCounter(1);
    }

    @Test
    void GIVEN_max_retries_exceeded_SHOULD_not_release_events() {
        cloudWatchLogsDispatcher = getCloudWatchLogsDispatcher();

        final List<EventHandle> eventHandles = getSampleEventHandles();
        when(mockCloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class)))
            .thenThrow(CloudWatchLogsException.class);

        List<InputLogEvent> inputLogEventList = cloudWatchLogsDispatcher.prepareInputLogEvents(getSampleBufferedData());
        cloudWatchLogsDispatcher.dispatchLogs(inputLogEventList, eventHandles);

        executeDispatcherRunnable();

        verify(mockCloudWatchLogsMetrics, times(RETRY_COUNT)).increaseRequestFailCounter(1);
        verify(mockCloudWatchLogsMetrics, never()).increaseRequestSuccessCounter(1);

        // No events should be released after max retries
        eventHandles.forEach(eventHandle -> verify(eventHandle, never()).release(true));
    }
}
