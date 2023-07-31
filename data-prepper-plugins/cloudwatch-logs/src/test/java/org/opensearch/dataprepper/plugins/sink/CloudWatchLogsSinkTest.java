package org.opensearch.dataprepper.plugins.sink;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.plugins.sink.client.CloudWatchLogsMetrics;
import org.opensearch.dataprepper.plugins.sink.config.AwsConfig;
import org.opensearch.dataprepper.plugins.sink.config.CloudWatchLogsSinkConfig;
import org.opensearch.dataprepper.plugins.sink.config.ThresholdConfig;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CloudWatchLogsSinkTest {
    private CloudWatchLogsSink testCloudWatchSink;
    private PluginSetting mockPluginSetting;
    private PluginMetrics mockPluginMetrics;
    private CloudWatchLogsSinkConfig mockCloudWatchLogsSinkConfig;
    private AwsCredentialsSupplier mockCredentialSupplier;
    private AwsConfig mockAwsConfig;
    private ThresholdConfig thresholdConfig;
    private CloudWatchLogsMetrics mockCloudWatchLogsMetrics;
    private static String TEST_LOG_GROUP = "testLogGroup";
    private static String TEST_LOG_STREAM= "testLogStream";
    private static String TEST_PLUGIN_NAME = "testPluginName";
    private static String TEST_PIPELINE_NAME = "testPipelineName";
    @BeforeEach
    void setUp() {
        mockPluginSetting = mock(PluginSetting.class);
        mockPluginMetrics = mock(PluginMetrics.class);
        mockCloudWatchLogsSinkConfig = mock(CloudWatchLogsSinkConfig.class);
        mockCredentialSupplier = mock(AwsCredentialsSupplier.class);
        mockAwsConfig = mock(AwsConfig.class);
        thresholdConfig = new ThresholdConfig();
        mockCloudWatchLogsMetrics = mock(CloudWatchLogsMetrics.class);

        when(mockCloudWatchLogsSinkConfig.getAwsConfig()).thenReturn(mockAwsConfig);
        when(mockCloudWatchLogsSinkConfig.getThresholdConfig()).thenReturn(thresholdConfig);
        when(mockCloudWatchLogsSinkConfig.getLogGroup()).thenReturn(TEST_LOG_GROUP);
        when(mockCloudWatchLogsSinkConfig.getLogStream()).thenReturn(TEST_LOG_STREAM);

        when(mockPluginSetting.getName()).thenReturn(TEST_PLUGIN_NAME);
        when(mockPluginSetting.getPipelineName()).thenReturn(TEST_PIPELINE_NAME);

        testCloudWatchSink = new CloudWatchLogsSink(mockPluginSetting, mockPluginMetrics, mockCloudWatchLogsSinkConfig,
                mockCredentialSupplier);
    }

    @Test
    void WHEN_sink_is_initialized_THEN_sink_is_ready_returns_true() {
        testCloudWatchSink.doInitialize();
        assertTrue(testCloudWatchSink.isReady());
    }
}
