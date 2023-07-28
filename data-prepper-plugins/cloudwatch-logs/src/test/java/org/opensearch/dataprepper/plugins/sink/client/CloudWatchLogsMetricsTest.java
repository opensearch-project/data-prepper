package org.opensearch.dataprepper.plugins.sink.client;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CloudWatchLogsMetricsTest {
    private CloudWatchLogsMetrics testCloudWatchLogsMetrics;
    private PluginMetrics mockPluginMetrics;
    private Counter mockSuccessEventCounter;
    private Counter mockSuccessRequestCounter;
    private Counter mockFailedEventCounter;
    private Counter mockFailedRequestCounter;

    @BeforeEach
    void setUp() {
        mockPluginMetrics = mock(PluginMetrics.class);
        mockSuccessEventCounter = mock(Counter.class);
        mockSuccessRequestCounter = mock(Counter.class);
        mockFailedEventCounter = mock(Counter.class);
        mockFailedRequestCounter = mock(Counter.class);

        when(mockPluginMetrics.counter(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_EVENTS_SUCCEEDED)).thenReturn(mockSuccessEventCounter);
        when(mockPluginMetrics.counter(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_REQUESTS_SUCCEEDED)).thenReturn(mockSuccessRequestCounter);
        when(mockPluginMetrics.counter(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_EVENTS_FAILED)).thenReturn(mockFailedEventCounter);
        when(mockPluginMetrics.counter(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_REQUESTS_FAILED)).thenReturn(mockFailedRequestCounter);

        testCloudWatchLogsMetrics = new CloudWatchLogsMetrics(mockPluginMetrics);
    }

    @Test
    void GIVEN_valid_plugin_metrics_WHEN_cloud_watch_metrics_initialized_SHOULD_not_be_null() {
        assertNotNull(testCloudWatchLogsMetrics);
    }

    @Test
    void WHEN_increase_event_success_counter_called_THEN_event_success_counter_increase_method_should_be_called() {
        testCloudWatchLogsMetrics.increaseLogEventSuccessCounter(1);
        verify(mockSuccessEventCounter, times(1)).increment(1);
    }

    @Test
    void WHEN_increase_request_success_counter_called_THEN_request_success_counter_increase_method_should_be_called() {
        testCloudWatchLogsMetrics.increaseRequestSuccessCounter(1);
        verify(mockSuccessRequestCounter, times(1)).increment(1);
    }

    @Test
    void WHEN_increase_event_failed_counter_called_THEN_event_failed_counter_increase_method_should_be_called() {
        testCloudWatchLogsMetrics.increaseLogEventFailCounter(1);
        verify(mockFailedEventCounter, times(1)).increment(1);
    }

    @Test
    void WHEN_increase_request_failed_counter_called_THEN_request_failed_counter_increase_method_should_be_called() {
        testCloudWatchLogsMetrics.increaseRequestFailCounter(1);
        verify(mockFailedRequestCounter, times(1)).increment(1);
    }
}
