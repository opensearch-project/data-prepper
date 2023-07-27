package org.opensearch.dataprepper.plugins.sink.client;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.plugins.sink.config.ThresholdConfig;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.CloudWatchLogsException;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;

import java.util.ArrayList;
import java.util.Collection;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class UploaderTest {
    private CloudWatchLogsClient cloudWatchLogsClient;
    private  CloudWatchLogsMetrics cloudWatchLogsMetrics;

    @BeforeEach
    void setUp() {
        cloudWatchLogsClient = mock(CloudWatchLogsClient.class);
        cloudWatchLogsMetrics = mock(CloudWatchLogsMetrics.class);
    }

    Collection<EventHandle> getTestEventHandles() {
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
                .cloudWatchLogsClient(cloudWatchLogsClient)
                .cloudWatchLogsMetrics(cloudWatchLogsMetrics)
                .putLogEventsRequest(getMockPutLogEventsRequest())
                .eventHandles(getTestEventHandles())
                .retryCount(ThresholdConfig.DEFAULT_RETRY_COUNT)
                .backOffTimeBase(ThresholdConfig.DEFAULT_BACKOFF_TIME)
                .build();
    }

    void establishFailingClientWithCloudWatchLogsExcept() {
        when(cloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class))).thenThrow(CloudWatchLogsException.class);
    }

    void establishFailingClientWithSdkClientExcept() {
        when(cloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class))).thenThrow(SdkClientException.class);
    }

    @Test
    void check_successful_transmission_test() throws InterruptedException {
        CloudWatchLogsDispatcher.Uploader testUploader = getUploader();
        testUploader.run();

        verify(cloudWatchLogsMetrics, atLeastOnce()).increaseRequestSuccessCounter(1);
        verify(cloudWatchLogsMetrics, atLeastOnce()).increaseLogEventSuccessCounter(ThresholdConfig.DEFAULT_BATCH_SIZE);
    }

    @Test
    void check_unsuccesful_transmission_with_cloudwatchlogsexcept_test() throws InterruptedException {
        establishFailingClientWithCloudWatchLogsExcept();
        CloudWatchLogsDispatcher.Uploader testUploader = getUploader();
        testUploader.run();

        verify(cloudWatchLogsMetrics, times(ThresholdConfig.DEFAULT_RETRY_COUNT)).increaseRequestFailCounter(1);
        verify(cloudWatchLogsMetrics, atLeastOnce()).increaseLogEventFailCounter(ThresholdConfig.DEFAULT_BATCH_SIZE);
    }

    @Test
    void check_unsuccesful_transmission_with_sdkexcept_test() {
        establishFailingClientWithSdkClientExcept();
        CloudWatchLogsDispatcher.Uploader testUploader = getUploader();
        testUploader.run();

        verify(cloudWatchLogsMetrics, times(ThresholdConfig.DEFAULT_RETRY_COUNT)).increaseRequestFailCounter(1);
        verify(cloudWatchLogsMetrics, atLeastOnce()).increaseLogEventFailCounter(ThresholdConfig.DEFAULT_BATCH_SIZE);
    }
}
