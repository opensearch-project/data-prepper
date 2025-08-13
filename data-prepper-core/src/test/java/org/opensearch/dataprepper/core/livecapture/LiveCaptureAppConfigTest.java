/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.livecapture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.core.parser.model.DataPrepperConfiguration;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.springframework.context.ApplicationContext;

import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LiveCaptureAppConfigTest {

    @Mock
    private DataPrepperConfiguration dataPrepperConfiguration;

    @Mock
    private LiveCaptureConfiguration liveCaptureConfiguration;

    @Mock
    private EventFactory defaultEventFactory;


    @Mock
    private ApplicationContext applicationContext;

    @Mock
    private PluginFactory pluginFactory;

    @Mock
    private Sink<Record<Event>> mockSink;

    @Mock
    private LiveCaptureManager liveCaptureManager;

    private LiveCaptureAppConfig liveCaptureAppConfig;

    @BeforeEach
    void setUp() {
        liveCaptureAppConfig = new LiveCaptureAppConfig(dataPrepperConfiguration, liveCaptureManager);
        liveCaptureAppConfig.setApplicationContext(applicationContext);
    }

    @Test
    void initializeLiveCaptureManager_with_valid_configuration() {
        when(dataPrepperConfiguration.getLiveCaptureConfiguration()).thenReturn(liveCaptureConfiguration);
        when(liveCaptureConfiguration.isDefaultEnabled()).thenReturn(true);
        when(liveCaptureConfiguration.getDefaultRate()).thenReturn(5.0);

        liveCaptureAppConfig.initializeLiveCaptureManager();

        verify(liveCaptureManager).initialize(true, 5.0);
    }

    @Test
    void initializeLiveCaptureManager_with_null_configuration() {
        when(dataPrepperConfiguration.getLiveCaptureConfiguration()).thenReturn(null);

        liveCaptureAppConfig.initializeLiveCaptureManager();

        // Should use defaults: enabled=false, rate=1.0
        verify(liveCaptureManager).initialize(false, 1.0);
    }

    @Test
    void shutdownLiveCapture_completes_successfully() {
        // Test that shutdown method completes without error
        liveCaptureAppConfig.shutdownLiveCapture();
        // No verification needed since shutdown is now a no-op with debug logging
    }



    @Test
    void initializeLiveCaptureManager_with_cloudwatch_sink_configuration() {
        // Create CloudWatch sink configuration
        Map<String, Object> awsConfig = Map.of("region", "us-east-1");
        Map<String, Object> cloudwatchConfig = Map.of(
                "log_group", "/dataprepper/livecapture",
                "log_stream", "livecapture-events",
                "aws", awsConfig
        );
        Map<String, Object> sinkConfig = Map.of(
                "cloudwatch_logs", cloudwatchConfig,
                "entry_threshold", 3,
                "batch_size", 10
        );

        when(dataPrepperConfiguration.getLiveCaptureConfiguration()).thenReturn(liveCaptureConfiguration);
        when(liveCaptureConfiguration.isDefaultEnabled()).thenReturn(true);
        when(liveCaptureConfiguration.getDefaultRate()).thenReturn(2.0);
        when(liveCaptureConfiguration.getFirstSinkConfiguration()).thenReturn(sinkConfig);

        // Mock PluginFactory to return a mock sink
        when(applicationContext.getBean(PluginFactory.class)).thenReturn(pluginFactory);
        when(pluginFactory.loadPlugin(any(Class.class), any(PluginSetting.class), any(SinkContext.class))).thenReturn(mockSink);

        liveCaptureAppConfig.initializeLiveCaptureManager();

        // Verify LiveCaptureManager initialization
        verify(liveCaptureManager).initialize(true, 2.0);
        verify(mockSink).initialize();
        verify(liveCaptureManager).setOutputSink(any(Sink.class));
    }

    @Test
    void testPluginBasedSinkFunctionality() {
        // This test exercises the plugin-based sink functionality
        Map<String, Object> awsConfig = Map.of("region", "us-west-2");
        Map<String, Object> cloudwatchConfig = Map.of(
                "log_group", "/test/livecapture",
                "log_stream", "test-stream",
                "aws", awsConfig
        );
        Map<String, Object> sinkConfig = Map.of("cloudwatch_logs", cloudwatchConfig);

        when(dataPrepperConfiguration.getLiveCaptureConfiguration()).thenReturn(liveCaptureConfiguration);
        when(liveCaptureConfiguration.isDefaultEnabled()).thenReturn(false);
        when(liveCaptureConfiguration.getDefaultRate()).thenReturn(1.0);
        when(liveCaptureConfiguration.getFirstSinkConfiguration()).thenReturn(sinkConfig);

        // Mock PluginFactory to return a mock sink
        when(applicationContext.getBean(PluginFactory.class)).thenReturn(pluginFactory);
        when(pluginFactory.loadPlugin(any(Class.class), any(PluginSetting.class), any(SinkContext.class))).thenReturn(mockSink);

        liveCaptureAppConfig.initializeLiveCaptureManager();
        verify(mockSink).initialize();
        verify(liveCaptureManager).setOutputSink(any(Sink.class));
    }
}