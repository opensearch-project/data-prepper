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
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogGroupRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogGroupResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogStreamRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogStreamResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.Entity;
import software.amazon.awssdk.services.cloudwatchlogs.model.RejectedEntityInfo;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.RejectedLogEventsInfo;
import software.amazon.awssdk.services.cloudwatchlogs.model.ResourceAlreadyExistsException;
import software.amazon.awssdk.services.cloudwatchlogs.model.ResourceNotFoundException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.cloudwatchlogs.model.CloudWatchLogsException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doThrow;
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

    CloudWatchLogsDispatcher getCloudWatchLogsDispatcher(int retryCount) {
        return CloudWatchLogsDispatcher.builder()
                .cloudWatchLogsClient(mockCloudWatchLogsClient)
                .cloudWatchLogsMetrics(mockCloudWatchLogsMetrics)
                .executor(mockExecutor)
                .logGroup(LOG_GROUP)
                .logStream(LOG_STREAM)
                .retryCount(retryCount)
                .dropIfDlqNotConfigured(true)
                .build();
    }

    CloudWatchLogsDispatcher getCloudWatchLogsDispatcherWithCreateFlag(final int retryCount, final boolean createLogGroup, final boolean createLogStream) {
        return CloudWatchLogsDispatcher.builder()
                .cloudWatchLogsClient(mockCloudWatchLogsClient)
                .cloudWatchLogsMetrics(mockCloudWatchLogsMetrics)
                .executor(mockExecutor)
                .logGroup(LOG_GROUP)
                .logStream(LOG_STREAM)
                .retryCount(retryCount)
                .dropIfDlqNotConfigured(true)
                .createLogGroup(createLogGroup)
                .createLogStream(createLogStream)
                .build();
    }

    private void executeDispatcherRunnable() {
        ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(mockExecutor).execute(runnableCaptor.capture());
        runnableCaptor.getValue().run();
    }

    @Test
    void GIVEN_valid_input_log_events_SHOULD_call_executor() {
        cloudWatchLogsDispatcher = getCloudWatchLogsDispatcher(RETRY_COUNT);

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
        cloudWatchLogsDispatcher = getCloudWatchLogsDispatcher(RETRY_COUNT);

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
        verify(eventHandles.get(0)).release(true);
        verify(eventHandles.get(1)).release(true);

        // Remaining events should be released
        for (int i = 2; i < eventHandles.size(); i++) {
            verify(eventHandles.get(i)).release(true);
        }
    }

    @Test
    void GIVEN_too_new_events_SHOULD_not_release_new_events() {
        cloudWatchLogsDispatcher = getCloudWatchLogsDispatcher(RETRY_COUNT);

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
            verify(eventHandles.get(i)).release(true);
        }
    }

    @Test
    void GIVEN_both_old_and_new_rejected_events_SHOULD_only_release_valid_events() {
        cloudWatchLogsDispatcher = getCloudWatchLogsDispatcher(RETRY_COUNT);

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
        verify(eventHandles.get(0)).release(true);
        verify(eventHandles.get(1)).release(true);

        // Middle events should be released
        for (int i = 2; i < eventHandles.size() - 2; i++) {
            verify(eventHandles.get(i)).release(true);
        }

        // Last two events should not be released (too new)
        verify(eventHandles.get(eventHandles.size() - 2)).release(true);
        verify(eventHandles.get(eventHandles.size() - 1)).release(true);
    }

    @Test
    void GIVEN_client_exception_SHOULD_retry() {
        cloudWatchLogsDispatcher = getCloudWatchLogsDispatcher(RETRY_COUNT);

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
    void GIVEN_cloudwatch_exception_SHOULD_retry_forever() {
        final int TEST_RETRY_COUNT = CloudWatchLogsDispatcher.Uploader.MULTIPLE_FAILURES_METRIC_COUNT+1;
        cloudWatchLogsDispatcher = getCloudWatchLogsDispatcher(TEST_RETRY_COUNT);

        final List<EventHandle> eventHandles = getSampleEventHandles();
        when(mockCloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class)))
            .thenThrow(CloudWatchLogsException.class);
        List<InputLogEvent> inputLogEventList = cloudWatchLogsDispatcher.prepareInputLogEvents(getSampleBufferedData());
        cloudWatchLogsDispatcher.dispatchLogs(inputLogEventList, eventHandles);

        executeDispatcherRunnable();

        verify(mockCloudWatchLogsMetrics, times(TEST_RETRY_COUNT)).increaseRequestFailCounter(1);
        verify(mockCloudWatchLogsMetrics, times(0)).increaseRequestSuccessCounter(1);
        verify(mockCloudWatchLogsMetrics, times(1)).increaseRequestMultiFailCounter(1);
    }

    @Test
    void GIVEN_cloudwatch_exception_SHOULD_retry() {
        cloudWatchLogsDispatcher = getCloudWatchLogsDispatcher(RETRY_COUNT);

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
        cloudWatchLogsDispatcher = getCloudWatchLogsDispatcher(RETRY_COUNT);

        final List<EventHandle> eventHandles = getSampleEventHandles();
        when(mockCloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class)))
            .thenThrow(CloudWatchLogsException.class);

        List<InputLogEvent> inputLogEventList = cloudWatchLogsDispatcher.prepareInputLogEvents(getSampleBufferedData());
        cloudWatchLogsDispatcher.dispatchLogs(inputLogEventList, eventHandles);

        executeDispatcherRunnable();

        verify(mockCloudWatchLogsMetrics, times(RETRY_COUNT)).increaseRequestFailCounter(1);
        verify(mockCloudWatchLogsMetrics, never()).increaseRequestSuccessCounter(1);

        // Pin the contract that the catch-all in run() does NOT fire on the normal
        // CloudWatchLogsException retry-exhaustion path. Without this, a future refactor that
        // accidentally routes a normal exception through the catch-all (e.g. by removing the
        // inner catch (Exception e)) would silently double-fire metrics — every retry-exhaustion
        // would increment both eventsFailed AND unhandledError, and this test would still pass.
        verify(mockCloudWatchLogsMetrics, never()).increaseUnhandledErrorCounter(anyInt());

        // No events should be released after max retries
        eventHandles.forEach(eventHandle -> verify(eventHandle).release(true));
    }

    @Test
    void GIVEN_resource_not_found_and_create_flag_true_WHEN_upload_SHOULD_create_group_and_stream_then_retry() {
        cloudWatchLogsDispatcher = getCloudWatchLogsDispatcherWithCreateFlag(RETRY_COUNT, true, true);

        final List<EventHandle> eventHandles = getSampleEventHandles();
        when(mockCloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class)))
                .thenThrow(ResourceNotFoundException.builder().message("missing").build())
                .thenReturn(mock(PutLogEventsResponse.class));
        when(mockCloudWatchLogsClient.createLogGroup(any(CreateLogGroupRequest.class)))
                .thenReturn(mock(CreateLogGroupResponse.class));
        when(mockCloudWatchLogsClient.createLogStream(any(CreateLogStreamRequest.class)))
                .thenReturn(mock(CreateLogStreamResponse.class));

        final List<InputLogEvent> inputLogEventList = cloudWatchLogsDispatcher.prepareInputLogEvents(getSampleBufferedData());
        cloudWatchLogsDispatcher.dispatchLogs(inputLogEventList, eventHandles);

        executeDispatcherRunnable();

        // Group and stream were created exactly once.
        verify(mockCloudWatchLogsClient, times(1)).createLogGroup(any(CreateLogGroupRequest.class));
        verify(mockCloudWatchLogsClient, times(1)).createLogStream(any(CreateLogStreamRequest.class));
        // Recovery action does not count as a failure — fail counter is not incremented.
        verify(mockCloudWatchLogsMetrics, never()).increaseRequestFailCounter(1);
        // PutLogEvents retry succeeded, so success counter is incremented.
        verify(mockCloudWatchLogsMetrics, times(1)).increaseRequestSuccessCounter(1);
    }

    @Test
    void GIVEN_resource_not_found_and_create_flag_false_WHEN_upload_SHOULD_follow_normal_retry_logic() {
        cloudWatchLogsDispatcher = getCloudWatchLogsDispatcherWithCreateFlag(RETRY_COUNT, false, false);

        final List<EventHandle> eventHandles = getSampleEventHandles();
        when(mockCloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class)))
                .thenThrow(ResourceNotFoundException.builder().message("missing").build());

        final List<InputLogEvent> inputLogEventList = cloudWatchLogsDispatcher.prepareInputLogEvents(getSampleBufferedData());
        cloudWatchLogsDispatcher.dispatchLogs(inputLogEventList, eventHandles);

        executeDispatcherRunnable();

        // No creation attempted when the flag is false.
        verify(mockCloudWatchLogsClient, never()).createLogGroup(any(CreateLogGroupRequest.class));
        verify(mockCloudWatchLogsClient, never()).createLogStream(any(CreateLogStreamRequest.class));
        // ResourceNotFoundException flows to the normal retry/DLQ path: fail counter incremented for every retry.
        verify(mockCloudWatchLogsMetrics, times(RETRY_COUNT)).increaseRequestFailCounter(1);
        verify(mockCloudWatchLogsMetrics, never()).increaseRequestSuccessCounter(1);
    }

    @Test
    void GIVEN_resource_not_found_and_only_create_log_stream_true_WHEN_upload_SHOULD_only_create_stream() {
        cloudWatchLogsDispatcher = getCloudWatchLogsDispatcherWithCreateFlag(RETRY_COUNT, false, true);

        final List<EventHandle> eventHandles = getSampleEventHandles();
        when(mockCloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class)))
                .thenThrow(ResourceNotFoundException.builder().message("missing").build())
                .thenReturn(mock(PutLogEventsResponse.class));
        when(mockCloudWatchLogsClient.createLogStream(any(CreateLogStreamRequest.class)))
                .thenReturn(mock(CreateLogStreamResponse.class));

        final List<InputLogEvent> inputLogEventList = cloudWatchLogsDispatcher.prepareInputLogEvents(getSampleBufferedData());
        cloudWatchLogsDispatcher.dispatchLogs(inputLogEventList, eventHandles);

        executeDispatcherRunnable();

        verify(mockCloudWatchLogsClient, never()).createLogGroup(any(CreateLogGroupRequest.class));
        verify(mockCloudWatchLogsClient, times(1)).createLogStream(any(CreateLogStreamRequest.class));
        verify(mockCloudWatchLogsMetrics, times(1)).increaseRequestSuccessCounter(1);
        verify(mockCloudWatchLogsMetrics, never()).increaseRequestFailCounter(1);
    }

    @Test
    void GIVEN_resource_not_found_and_only_create_log_group_true_WHEN_upload_SHOULD_only_create_group() {
        cloudWatchLogsDispatcher = getCloudWatchLogsDispatcherWithCreateFlag(RETRY_COUNT, true, false);

        final List<EventHandle> eventHandles = getSampleEventHandles();
        when(mockCloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class)))
                .thenThrow(ResourceNotFoundException.builder().message("missing").build())
                .thenReturn(mock(PutLogEventsResponse.class));
        when(mockCloudWatchLogsClient.createLogGroup(any(CreateLogGroupRequest.class)))
                .thenReturn(mock(CreateLogGroupResponse.class));

        final List<InputLogEvent> inputLogEventList = cloudWatchLogsDispatcher.prepareInputLogEvents(getSampleBufferedData());
        cloudWatchLogsDispatcher.dispatchLogs(inputLogEventList, eventHandles);

        executeDispatcherRunnable();

        verify(mockCloudWatchLogsClient, times(1)).createLogGroup(any(CreateLogGroupRequest.class));
        verify(mockCloudWatchLogsClient, never()).createLogStream(any(CreateLogStreamRequest.class));
        verify(mockCloudWatchLogsMetrics, times(1)).increaseRequestSuccessCounter(1);
        verify(mockCloudWatchLogsMetrics, never()).increaseRequestFailCounter(1);
    }

    @Test
    void GIVEN_resource_not_found_and_create_succeeds_WHEN_retry_put_log_events_SHOULD_succeed() {
        cloudWatchLogsDispatcher = getCloudWatchLogsDispatcherWithCreateFlag(RETRY_COUNT, true, true);

        final List<EventHandle> eventHandles = getSampleEventHandles();
        when(mockCloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class)))
                .thenThrow(ResourceNotFoundException.builder().message("missing").build())
                .thenReturn(mock(PutLogEventsResponse.class));
        when(mockCloudWatchLogsClient.createLogGroup(any(CreateLogGroupRequest.class)))
                .thenReturn(mock(CreateLogGroupResponse.class));
        when(mockCloudWatchLogsClient.createLogStream(any(CreateLogStreamRequest.class)))
                .thenReturn(mock(CreateLogStreamResponse.class));

        final List<InputLogEvent> inputLogEventList = cloudWatchLogsDispatcher.prepareInputLogEvents(getSampleBufferedData());
        cloudWatchLogsDispatcher.dispatchLogs(inputLogEventList, eventHandles);

        executeDispatcherRunnable();

        // PutLogEvents retried after successful creation and succeeded — events released.
        verify(mockCloudWatchLogsClient, times(2)).putLogEvents(any(PutLogEventsRequest.class));
        eventHandles.forEach(eventHandle -> verify(eventHandle).release(true));
    }

    @Test
    void GIVEN_resource_not_found_and_group_already_exists_WHEN_create_SHOULD_ignore_and_create_stream() {
        cloudWatchLogsDispatcher = getCloudWatchLogsDispatcherWithCreateFlag(RETRY_COUNT, true, true);

        final List<EventHandle> eventHandles = getSampleEventHandles();
        when(mockCloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class)))
                .thenThrow(ResourceNotFoundException.builder().message("missing").build())
                .thenReturn(mock(PutLogEventsResponse.class));
        when(mockCloudWatchLogsClient.createLogGroup(any(CreateLogGroupRequest.class)))
                .thenThrow(ResourceAlreadyExistsException.builder().message("group exists").build());
        when(mockCloudWatchLogsClient.createLogStream(any(CreateLogStreamRequest.class)))
                .thenReturn(mock(CreateLogStreamResponse.class));

        final List<InputLogEvent> inputLogEventList = cloudWatchLogsDispatcher.prepareInputLogEvents(getSampleBufferedData());
        cloudWatchLogsDispatcher.dispatchLogs(inputLogEventList, eventHandles);

        executeDispatcherRunnable();

        // ResourceAlreadyExistsException is swallowed silently; createLogStream is still attempted; PutLogEvents retry succeeds.
        verify(mockCloudWatchLogsClient, times(1)).createLogGroup(any(CreateLogGroupRequest.class));
        verify(mockCloudWatchLogsClient, times(1)).createLogStream(any(CreateLogStreamRequest.class));
        verify(mockCloudWatchLogsMetrics, times(1)).increaseRequestSuccessCounter(1);
        verify(mockCloudWatchLogsMetrics, never()).increaseRequestFailCounter(1);
    }

    @Test
    void GIVEN_resource_not_found_and_stream_already_exists_WHEN_create_SHOULD_ignore_and_retry_put_log_events() {
        cloudWatchLogsDispatcher = getCloudWatchLogsDispatcherWithCreateFlag(RETRY_COUNT, true, true);

        final List<EventHandle> eventHandles = getSampleEventHandles();
        when(mockCloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class)))
                .thenThrow(ResourceNotFoundException.builder().message("missing").build())
                .thenReturn(mock(PutLogEventsResponse.class));
        when(mockCloudWatchLogsClient.createLogGroup(any(CreateLogGroupRequest.class)))
                .thenReturn(mock(CreateLogGroupResponse.class));
        when(mockCloudWatchLogsClient.createLogStream(any(CreateLogStreamRequest.class)))
                .thenThrow(ResourceAlreadyExistsException.builder().message("stream exists").build());

        final List<InputLogEvent> inputLogEventList = cloudWatchLogsDispatcher.prepareInputLogEvents(getSampleBufferedData());
        cloudWatchLogsDispatcher.dispatchLogs(inputLogEventList, eventHandles);

        executeDispatcherRunnable();

        // Stream ResourceAlreadyExistsException is swallowed; PutLogEvents retry runs and succeeds.
        verify(mockCloudWatchLogsClient, times(1)).createLogStream(any(CreateLogStreamRequest.class));
        verify(mockCloudWatchLogsMetrics, times(1)).increaseRequestSuccessCounter(1);
        verify(mockCloudWatchLogsMetrics, never()).increaseRequestFailCounter(1);
    }

    @Test
    void GIVEN_resource_not_found_and_create_group_throws_access_denied_SHOULD_still_attempt_create_stream() {
        cloudWatchLogsDispatcher = getCloudWatchLogsDispatcherWithCreateFlag(RETRY_COUNT, true, true);

        final List<EventHandle> eventHandles = getSampleEventHandles();
        when(mockCloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class)))
                .thenThrow(ResourceNotFoundException.builder().message("missing").build())
                .thenReturn(mock(PutLogEventsResponse.class));
        // Simulate AccessDeniedException on createLogGroup (a CloudWatchLogsException subtype, surfacing as base in tests).
        when(mockCloudWatchLogsClient.createLogGroup(any(CreateLogGroupRequest.class)))
                .thenThrow(CloudWatchLogsException.builder().message("access denied").build());
        when(mockCloudWatchLogsClient.createLogStream(any(CreateLogStreamRequest.class)))
                .thenReturn(mock(CreateLogStreamResponse.class));

        final List<InputLogEvent> inputLogEventList = cloudWatchLogsDispatcher.prepareInputLogEvents(getSampleBufferedData());
        cloudWatchLogsDispatcher.dispatchLogs(inputLogEventList, eventHandles);

        executeDispatcherRunnable();

        // createLogStream is still attempted even when createLogGroup fails — the helper does not short-circuit.
        verify(mockCloudWatchLogsClient, times(1)).createLogGroup(any(CreateLogGroupRequest.class));
        verify(mockCloudWatchLogsClient, times(1)).createLogStream(any(CreateLogStreamRequest.class));
        verify(mockCloudWatchLogsMetrics, times(1)).increaseRequestSuccessCounter(1);
    }

    @Test
    void GIVEN_resource_not_found_and_create_stream_throws_cloudwatch_logs_exception_SHOULD_not_kill_uploader() {
        cloudWatchLogsDispatcher = getCloudWatchLogsDispatcherWithCreateFlag(RETRY_COUNT, true, true);

        final List<EventHandle> eventHandles = getSampleEventHandles();
        // First PutLogEvents call throws ResourceNotFoundException; second PutLogEvents call (after failed creation) succeeds.
        when(mockCloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class)))
                .thenThrow(ResourceNotFoundException.builder().message("missing").build())
                .thenReturn(mock(PutLogEventsResponse.class));
        when(mockCloudWatchLogsClient.createLogGroup(any(CreateLogGroupRequest.class)))
                .thenReturn(mock(CreateLogGroupResponse.class));
        when(mockCloudWatchLogsClient.createLogStream(any(CreateLogStreamRequest.class)))
                .thenThrow(CloudWatchLogsException.builder().message("creation failed").build());

        final List<InputLogEvent> inputLogEventList = cloudWatchLogsDispatcher.prepareInputLogEvents(getSampleBufferedData());
        cloudWatchLogsDispatcher.dispatchLogs(inputLogEventList, eventHandles);

        executeDispatcherRunnable();

        // Helper swallowed the exception; PutLogEvents retry ran and succeeded; uploader not interrupted.
        verify(mockCloudWatchLogsClient, times(2)).putLogEvents(any(PutLogEventsRequest.class));
        verify(mockCloudWatchLogsMetrics, times(1)).increaseRequestSuccessCounter(1);
        verify(mockCloudWatchLogsMetrics, never()).increaseRequestFailCounter(1);
    }

    @Test
    void GIVEN_resource_not_found_and_create_flag_true_SHOULD_only_attempt_creation_once_per_upload() {
        cloudWatchLogsDispatcher = getCloudWatchLogsDispatcherWithCreateFlag(RETRY_COUNT, true, true);

        final List<EventHandle> eventHandles = getSampleEventHandles();
        // Every PutLogEvents call throws ResourceNotFoundException — creation should still only be attempted once.
        when(mockCloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class)))
                .thenThrow(ResourceNotFoundException.builder().message("missing").build());
        when(mockCloudWatchLogsClient.createLogGroup(any(CreateLogGroupRequest.class)))
                .thenReturn(mock(CreateLogGroupResponse.class));
        when(mockCloudWatchLogsClient.createLogStream(any(CreateLogStreamRequest.class)))
                .thenReturn(mock(CreateLogStreamResponse.class));

        final List<InputLogEvent> inputLogEventList = cloudWatchLogsDispatcher.prepareInputLogEvents(getSampleBufferedData());
        cloudWatchLogsDispatcher.dispatchLogs(inputLogEventList, eventHandles);

        executeDispatcherRunnable();

        // Creation invoked exactly once per Uploader invocation, no matter how many ResourceNotFoundExceptions follow.
        verify(mockCloudWatchLogsClient, times(1)).createLogGroup(any(CreateLogGroupRequest.class));
        verify(mockCloudWatchLogsClient, times(1)).createLogStream(any(CreateLogStreamRequest.class));
    }

    @Test
    void GIVEN_create_flag_true_AND_creation_succeeds_BUT_put_log_events_still_throws_resource_not_found_SHOULD_only_create_once_and_FAIL_RETRIES() {
        cloudWatchLogsDispatcher = getCloudWatchLogsDispatcherWithCreateFlag(RETRY_COUNT, true, true);

        final List<EventHandle> eventHandles = getSampleEventHandles();
        when(mockCloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class)))
                .thenThrow(ResourceNotFoundException.builder().message("missing").build());
        when(mockCloudWatchLogsClient.createLogGroup(any(CreateLogGroupRequest.class)))
                .thenReturn(mock(CreateLogGroupResponse.class));
        when(mockCloudWatchLogsClient.createLogStream(any(CreateLogStreamRequest.class)))
                .thenReturn(mock(CreateLogStreamResponse.class));

        final List<InputLogEvent> inputLogEventList = cloudWatchLogsDispatcher.prepareInputLogEvents(getSampleBufferedData());
        cloudWatchLogsDispatcher.dispatchLogs(inputLogEventList, eventHandles);

        executeDispatcherRunnable();

        // Creation attempted exactly once.
        verify(mockCloudWatchLogsClient, times(1)).createLogGroup(any(CreateLogGroupRequest.class));
        verify(mockCloudWatchLogsClient, times(1)).createLogStream(any(CreateLogStreamRequest.class));
        // Subsequent ResourceNotFoundExceptions flow to the normal retry/DLQ path: fail counter incremented for every retry.
        verify(mockCloudWatchLogsMetrics, times(RETRY_COUNT)).increaseRequestFailCounter(1);
        verify(mockCloudWatchLogsMetrics, never()).increaseRequestSuccessCounter(1);
    }

    @Test
    void GIVEN_resource_not_found_and_both_create_group_and_stream_throw_non_already_exists_SHOULD_not_kill_uploader() {
        cloudWatchLogsDispatcher = getCloudWatchLogsDispatcherWithCreateFlag(RETRY_COUNT, true, true);

        final List<EventHandle> eventHandles = getSampleEventHandles();
        when(mockCloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class)))
                .thenThrow(ResourceNotFoundException.builder().message("missing").build())
                .thenReturn(mock(PutLogEventsResponse.class));
        when(mockCloudWatchLogsClient.createLogGroup(any(CreateLogGroupRequest.class)))
                .thenThrow(CloudWatchLogsException.builder().message("access denied").build());
        when(mockCloudWatchLogsClient.createLogStream(any(CreateLogStreamRequest.class)))
                .thenThrow(CloudWatchLogsException.builder().message("access denied").build());

        final List<InputLogEvent> inputLogEventList = cloudWatchLogsDispatcher.prepareInputLogEvents(getSampleBufferedData());
        cloudWatchLogsDispatcher.dispatchLogs(inputLogEventList, eventHandles);

        executeDispatcherRunnable();

        // Both creation calls failed but the helper swallowed both exceptions; PutLogEvents retry succeeded.
        verify(mockCloudWatchLogsClient, times(1)).createLogGroup(any(CreateLogGroupRequest.class));
        verify(mockCloudWatchLogsClient, times(1)).createLogStream(any(CreateLogStreamRequest.class));
        verify(mockCloudWatchLogsMetrics, times(1)).increaseRequestSuccessCounter(1);
        verify(mockCloudWatchLogsMetrics, never()).increaseRequestFailCounter(1);
    }

    @Test
    void GIVEN_entity_configured_WHEN_dispatch_logs_called_SHOULD_set_entity_on_put_log_events_request() {
        final Map<String, String> keyAttributes = new HashMap<>();
        keyAttributes.put("Type", "RemoteService");
        keyAttributes.put("Name", "okta_auth0");
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("AWS.ServiceNameSource", "UserConfiguration");
        final Entity entity = Entity.builder().keyAttributes(keyAttributes).attributes(attributes).build();

        cloudWatchLogsDispatcher = CloudWatchLogsDispatcher.builder()
                .cloudWatchLogsClient(mockCloudWatchLogsClient)
                .cloudWatchLogsMetrics(mockCloudWatchLogsMetrics)
                .executor(mockExecutor)
                .logGroup(LOG_GROUP)
                .logStream(LOG_STREAM)
                .retryCount(RETRY_COUNT)
                .dropIfDlqNotConfigured(true)
                .entity(entity)
                .build();

        final List<EventHandle> eventHandles = getSampleEventHandles();
        final PutLogEventsResponse response = mock(PutLogEventsResponse.class);
        when(response.rejectedLogEventsInfo()).thenReturn(null);
        when(response.rejectedEntityInfo()).thenReturn(null);
        when(mockCloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class))).thenReturn(response);

        final List<InputLogEvent> inputLogEventList = cloudWatchLogsDispatcher.prepareInputLogEvents(getSampleBufferedData());
        cloudWatchLogsDispatcher.dispatchLogs(inputLogEventList, eventHandles);

        executeDispatcherRunnable();

        final ArgumentCaptor<PutLogEventsRequest> requestCaptor = ArgumentCaptor.forClass(PutLogEventsRequest.class);
        verify(mockCloudWatchLogsClient).putLogEvents(requestCaptor.capture());
        final PutLogEventsRequest captured = requestCaptor.getValue();

        assertThat(captured.entity(), notNullValue());
        assertThat(captured.entity().keyAttributes(), equalTo(keyAttributes));
        assertThat(captured.entity().attributes(), equalTo(attributes));
    }

    @Test
    void GIVEN_entity_not_configured_WHEN_dispatch_logs_called_SHOULD_not_set_entity_on_put_log_events_request() {
        cloudWatchLogsDispatcher = getCloudWatchLogsDispatcher(RETRY_COUNT);

        final List<EventHandle> eventHandles = getSampleEventHandles();
        final PutLogEventsResponse response = mock(PutLogEventsResponse.class);
        when(response.rejectedLogEventsInfo()).thenReturn(null);
        when(response.rejectedEntityInfo()).thenReturn(null);
        when(mockCloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class))).thenReturn(response);

        final List<InputLogEvent> inputLogEventList = cloudWatchLogsDispatcher.prepareInputLogEvents(getSampleBufferedData());
        cloudWatchLogsDispatcher.dispatchLogs(inputLogEventList, eventHandles);

        executeDispatcherRunnable();

        final ArgumentCaptor<PutLogEventsRequest> requestCaptor = ArgumentCaptor.forClass(PutLogEventsRequest.class);
        verify(mockCloudWatchLogsClient).putLogEvents(requestCaptor.capture());

        assertThat(requestCaptor.getValue().entity(), nullValue());
    }

    @Test
    void GIVEN_entity_rejected_WHEN_put_log_events_succeeds_SHOULD_increment_rejection_metric_and_release_events() {
        cloudWatchLogsDispatcher = getCloudWatchLogsDispatcher(RETRY_COUNT);

        final List<EventHandle> eventHandles = getSampleEventHandles();
        final PutLogEventsResponse response = mock(PutLogEventsResponse.class);
        final RejectedEntityInfo rejectedEntityInfo = mock(RejectedEntityInfo.class);
        when(rejectedEntityInfo.errorTypeAsString()).thenReturn("InvalidEntity");
        when(response.rejectedLogEventsInfo()).thenReturn(null);
        when(response.rejectedEntityInfo()).thenReturn(rejectedEntityInfo);
        when(mockCloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class))).thenReturn(response);

        final List<InputLogEvent> inputLogEventList = cloudWatchLogsDispatcher.prepareInputLogEvents(getSampleBufferedData());
        cloudWatchLogsDispatcher.dispatchLogs(inputLogEventList, eventHandles);

        executeDispatcherRunnable();

        verify(mockCloudWatchLogsMetrics, times(1)).increaseEntityRejectedCounter(1);
        verify(mockCloudWatchLogsMetrics, times(1)).increaseRequestSuccessCounter(1);
        eventHandles.forEach(eventHandle -> verify(eventHandle).release(true));
    }

    @Test
    void GIVEN_error_thrown_from_put_log_events_WHEN_run_SHOULD_route_to_unhandled_error_path() {
        cloudWatchLogsDispatcher = getCloudWatchLogsDispatcher(RETRY_COUNT);

        final List<EventHandle> eventHandles = getSampleEventHandles();
        when(mockCloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class)))
                .thenThrow(new NoSuchMethodError("simulated linkage failure"));

        final List<InputLogEvent> inputLogEventList = cloudWatchLogsDispatcher.prepareInputLogEvents(getSampleBufferedData());
        cloudWatchLogsDispatcher.dispatchLogs(inputLogEventList, eventHandles);

        executeDispatcherRunnable();

        // Dedicated unhandled-error metric incremented once — distinct from normal retry exhaustion.
        verify(mockCloudWatchLogsMetrics, times(1)).increaseUnhandledErrorCounter(1);
        // Neither count flag was set before the Error escaped, so events must be accounted as failed.
        verify(mockCloudWatchLogsMetrics, times(1)).increaseLogEventFailCounter(eventHandles.size());
        // No phantom successes.
        verify(mockCloudWatchLogsMetrics, never()).increaseLogEventSuccessCounter(anyInt());
        // Every handle released exactly once with dropIfDlqNotConfigured=true so the source can make forward progress.
        eventHandles.forEach(handle -> verify(handle, times(1)).release(true));
    }

    @Test
    void GIVEN_runtime_exception_from_response_handling_WHEN_run_SHOULD_route_to_unhandled_error_path() {
        cloudWatchLogsDispatcher = getCloudWatchLogsDispatcher(RETRY_COUNT);

        final List<EventHandle> eventHandles = getSampleEventHandles();
        final PutLogEventsResponse response = mock(PutLogEventsResponse.class);
        when(response.rejectedLogEventsInfo()).thenThrow(new RuntimeException("simulated response failure"));
        when(mockCloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class))).thenReturn(response);

        final List<InputLogEvent> inputLogEventList = cloudWatchLogsDispatcher.prepareInputLogEvents(getSampleBufferedData());
        cloudWatchLogsDispatcher.dispatchLogs(inputLogEventList, eventHandles);

        executeDispatcherRunnable();

        verify(mockCloudWatchLogsMetrics, times(1)).increaseUnhandledErrorCounter(1);
        // PutLogEvents itself succeeded (request-level success), but the events couldn't be
        // accounted because of the response-handling failure → fail counter increments.
        verify(mockCloudWatchLogsMetrics, times(1)).increaseLogEventFailCounter(eventHandles.size());
        verify(mockCloudWatchLogsMetrics, never()).increaseLogEventSuccessCounter(anyInt());
        eventHandles.forEach(handle -> verify(handle, times(1)).release(true));
    }

    @Test
    void GIVEN_throwable_after_success_counted_WHEN_run_SHOULD_not_double_count_failures() {
        cloudWatchLogsDispatcher = getCloudWatchLogsDispatcher(RETRY_COUNT);

        final List<EventHandle> eventHandles = getSampleEventHandles();
        final PutLogEventsResponse response = mock(PutLogEventsResponse.class);
        when(response.rejectedLogEventsInfo())
                .thenReturn(null)
                .thenThrow(new RuntimeException("simulated post-success failure"));
        when(mockCloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class))).thenReturn(response);

        final List<InputLogEvent> inputLogEventList = cloudWatchLogsDispatcher.prepareInputLogEvents(getSampleBufferedData());
        cloudWatchLogsDispatcher.dispatchLogs(inputLogEventList, eventHandles);

        executeDispatcherRunnable();

        // Catch-all fired.
        verify(mockCloudWatchLogsMetrics, times(1)).increaseUnhandledErrorCounter(1);
        // Critical: success was already counted; the catch-all must NOT also count failures.
        verify(mockCloudWatchLogsMetrics, never()).increaseLogEventFailCounter(anyInt());
        // Success counter was incremented before the Throwable escaped.
        verify(mockCloudWatchLogsMetrics, times(1)).increaseLogEventSuccessCounter(eventHandles.size());
    }

    @Test
    void GIVEN_event_handle_release_throws_WHEN_run_SHOULD_swallow_and_continue_releasing_other_handles() {
        cloudWatchLogsDispatcher = getCloudWatchLogsDispatcher(RETRY_COUNT);

        final List<EventHandle> eventHandles = getSampleEventHandles();
        // Handle at index 5 throws on release() — releaseOnce must swallow and the loop must continue.
        doThrow(new RuntimeException("simulated bad handle"))
                .when(eventHandles.get(5)).release(true);

        final PutLogEventsResponse response = mock(PutLogEventsResponse.class);
        when(response.rejectedLogEventsInfo()).thenReturn(null);
        when(mockCloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class))).thenReturn(response);

        final List<InputLogEvent> inputLogEventList = cloudWatchLogsDispatcher.prepareInputLogEvents(getSampleBufferedData());
        cloudWatchLogsDispatcher.dispatchLogs(inputLogEventList, eventHandles);

        executeDispatcherRunnable();

        // Every handle had release(true) attempted exactly once — the bad handle did not abort the loop.
        eventHandles.forEach(handle -> verify(handle, times(1)).release(true));
        // No handle was released with the catch-all's negative result — the success path completed.
        eventHandles.forEach(handle -> verify(handle, never()).release(false));
        // Success path completed normally — catch-all NOT triggered.
        verify(mockCloudWatchLogsMetrics, times(1)).increaseLogEventSuccessCounter(eventHandles.size());
        verify(mockCloudWatchLogsMetrics, never()).increaseUnhandledErrorCounter(anyInt());
        verify(mockCloudWatchLogsMetrics, never()).increaseLogEventFailCounter(anyInt());
    }
}
