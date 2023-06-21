package org.opensearch.dataprepper.plugins.source.sqssource;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.plugins.aws.sqs.common.metrics.SqsMetrics;
import org.opensearch.dataprepper.plugins.source.sqssource.config.SqsSourceConfig;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SqsSourceTest {

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    private SqsSource sqsSource;

    @BeforeEach
    public void setup() {
        SqsSourceConfig sqsSourceConfig = mock(SqsSourceConfig.class);
        pluginMetrics = mock(PluginMetrics.class);
        Timer timer = mock(Timer.class);
        Counter counter = mock(Counter.class);

        awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);
        when(pluginMetrics.timer(SqsMetrics.SQS_MESSAGE_DELAY_METRIC_NAME)).thenReturn(timer);
        when(pluginMetrics.counter(SqsMetrics.SQS_MESSAGES_RECEIVED_METRIC_NAME)).thenReturn(counter);
        when(pluginMetrics.counter(SqsMetrics.SQS_MESSAGES_DELETED_METRIC_NAME)).thenReturn(counter);
        when(pluginMetrics.counter(SqsMetrics.SQS_RECEIVE_MESSAGES_FAILED_METRIC_NAME)).thenReturn(counter);
        when(pluginMetrics.counter(SqsMetrics.SQS_MESSAGES_DELETE_FAILED_METRIC_NAME)).thenReturn(counter);
        when(pluginMetrics.counter(SqsMetrics.ACKNOWLEDGEMENT_SET_CALLBACK_METRIC_NAME)).thenReturn(counter);
        when(sqsSourceConfig.getUrls()).thenReturn(List.of("https://sqs.us-east-1.amazonaws.com/123099425585/dp"));
        this. sqsSource =
                new SqsSource(pluginMetrics,sqsSourceConfig,acknowledgementSetManager,awsCredentialsSupplier);
    }

    @Test
    void start_should_throw_IllegalStateException_when_buffer_is_null(){
        assertThrows(IllegalStateException.class, () -> sqsSource.start(null));
    }

}
