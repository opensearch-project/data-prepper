package org.opensearch.dataprepper.plugins.aws.sqs.common.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SqsMetricsTest {
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private Timer timer;
    @Mock
    private Counter counter;

    @Test
    void s3ObjectPluginMetricsTest(){
        pluginMetrics = mock(PluginMetrics.class);
        timer = mock(Timer.class);
        counter = mock(Counter.class);
        when(pluginMetrics.timer(SqsMetrics.SQS_MESSAGE_DELAY_METRIC_NAME)).thenReturn(timer);
        when(pluginMetrics.counter(SqsMetrics.SQS_MESSAGES_RECEIVED_METRIC_NAME)).thenReturn(counter);
        when(pluginMetrics.counter(SqsMetrics.SQS_MESSAGES_DELETED_METRIC_NAME)).thenReturn(counter);
        when(pluginMetrics.counter(SqsMetrics.SQS_RECEIVE_MESSAGES_FAILED_METRIC_NAME)).thenReturn(counter);
        when(pluginMetrics.counter(SqsMetrics.SQS_MESSAGES_DELETE_FAILED_METRIC_NAME)).thenReturn(counter);
        when(pluginMetrics.counter(SqsMetrics.ACKNOWLEDGEMENT_SET_CALLBACK_METRIC_NAME)).thenReturn(counter);

        SqsMetrics metrics = new SqsMetrics(pluginMetrics);
        assertThat(metrics.getAcknowledgementSetCallbackCounter(),sameInstance(counter));
        assertThat(metrics.getSqsMessagesDeletedCounter(),sameInstance(counter));
        assertThat(metrics.getSqsMessagesReceivedCounter(),sameInstance(counter));
        assertThat(metrics.getSqsReceiveMessagesFailedCounter(),sameInstance(counter));
        assertThat(metrics.getSqsMessagesDeleteFailedCounter(),sameInstance(counter));
        assertThat(metrics.getSqsMessageDelayTimer(),sameInstance(timer));


    }
}
