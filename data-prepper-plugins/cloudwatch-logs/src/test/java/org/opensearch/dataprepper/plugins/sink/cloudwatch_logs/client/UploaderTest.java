package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.ThresholdConfig;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.CloudWatchLogsException;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class UploaderTest {
    private CloudWatchLogsClient mockCloudWatchLogsClient;
    private CloudWatchLogsMetrics mockCloudWatchLogsMetrics;

    @BeforeEach
    void setUp() {
        mockCloudWatchLogsClient = mock(CloudWatchLogsClient.class);
        mockCloudWatchLogsMetrics = mock(CloudWatchLogsMetrics.class);
    }

    List<EventHandle> getTestEventHandles() {
        final ArrayList<EventHandle> eventHandles = new ArrayList<>();
        for (int i = 0; i < ThresholdConfig.DEFAULT_BATCH_SIZE; i++) {
            final EventHandle mockEventHandle = mock(EventHandle.class);
            eventHandles.add(mockEventHandle);
        }

        return eventHandles;
    }

    PutLogEventsRequest getMockPutLogEventsRequest() {
        return mock(PutLogEventsRequest.class);
    }

    CloudWatchLogsDispatcher.Uploader getUploader() {
        return CloudWatchLogsDispatcher.Uploader.builder()
                .cloudWatchLogsClient(mockCloudWatchLogsClient)
                .cloudWatchLogsMetrics(mockCloudWatchLogsMetrics)
                .putLogEventsRequest(getMockPutLogEventsRequest())
                .eventHandles(getTestEventHandles())
                .totalEventCount(ThresholdConfig.DEFAULT_BATCH_SIZE)
                .retryCount(ThresholdConfig.DEFAULT_RETRY_COUNT)
                .backOffTimeBase(ThresholdConfig.DEFAULT_BACKOFF_TIME)
                .build();
    }

    void establishFailingClientWithCloudWatchLogsExcept() {
        when(mockCloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class))).thenThrow(CloudWatchLogsException.class);
    }

    void establishFailingClientWithSdkClientExcept() {
        when(mockCloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class))).thenThrow(SdkClientException.class);
    }

    @Test
    void GIVEN_valid_uploader_SHOULD_update_cloud_watch_logs_metrics() {
        CloudWatchLogsDispatcher.Uploader testUploader = getUploader();
        testUploader.run();

        verify(mockCloudWatchLogsMetrics, atLeastOnce()).increaseRequestSuccessCounter(1);
        verify(mockCloudWatchLogsMetrics, atLeastOnce()).increaseLogEventSuccessCounter(ThresholdConfig.DEFAULT_BATCH_SIZE);
    }

    @Test
    void GIVEN_valid_uploader_WHEN_run_throws_cloud_watch_logs_exception_SHOULD_update_fail_counters() {
        establishFailingClientWithCloudWatchLogsExcept();
        CloudWatchLogsDispatcher.Uploader testUploader = getUploader();
        testUploader.run();

        verify(mockCloudWatchLogsMetrics, times(ThresholdConfig.DEFAULT_RETRY_COUNT)).increaseRequestFailCounter(1);
        verify(mockCloudWatchLogsMetrics, atLeastOnce()).increaseLogEventFailCounter(ThresholdConfig.DEFAULT_BATCH_SIZE);
    }

    @Test
    void GIVEN_valid_uploader_WHEN_run_throws_sdk_client_except_SHOULD_update_fail_counters() {
        establishFailingClientWithSdkClientExcept();
        CloudWatchLogsDispatcher.Uploader testUploader = getUploader();
        testUploader.run();

        verify(mockCloudWatchLogsMetrics, times(ThresholdConfig.DEFAULT_RETRY_COUNT)).increaseRequestFailCounter(1);
        verify(mockCloudWatchLogsMetrics, atLeastOnce()).increaseLogEventFailCounter(ThresholdConfig.DEFAULT_BATCH_SIZE);
    }
}
