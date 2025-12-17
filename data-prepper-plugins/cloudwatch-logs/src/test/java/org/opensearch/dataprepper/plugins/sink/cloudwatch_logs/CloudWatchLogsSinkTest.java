/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs;

import io.micrometer.core.instrument.DistributionSummary;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.dlq.DlqPushHandler;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.CloudWatchLogsClientFactory;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.CloudWatchLogsDispatcher;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.CloudWatchLogsMetrics;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.AwsConfig;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.CloudWatchLogsSinkConfig;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.ThresholdConfig;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.regions.Region;


import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CloudWatchLogsSinkTest {
    private static int TEST_MAX_RETRIES = 3;
    private PluginSetting mockPluginSetting;
    private PluginMetrics mockPluginMetrics;
    private PluginFactory mockPluginFactory;
    private CloudWatchLogsSinkConfig mockCloudWatchLogsSinkConfig;
    private AwsCredentialsSupplier mockCredentialSupplier;
    private AwsConfig mockAwsConfig;
    private ThresholdConfig thresholdConfig;
    private Map<String, String> mockHeaderOverrides;
    private CloudWatchLogsMetrics mockCloudWatchLogsMetrics;
    private CloudWatchLogsClient mockClient;
    private static final String TEST_LOG_GROUP = "testLogGroup";
    private static final String TEST_LOG_STREAM= "testLogStream";
    private static final String TEST_PLUGIN_NAME = "testPluginName";
    private static final String TEST_PIPELINE_NAME = "testPipelineName";
    private static final String TEST_BUFFER_TYPE = "in_memory";
    private int numRetries;
    @BeforeEach
    void setUp() {
        mockPluginSetting = mock(PluginSetting.class);
        mockPluginMetrics = mock(PluginMetrics.class);
        mockPluginFactory = mock(PluginFactory.class);
        mockCloudWatchLogsSinkConfig = mock(CloudWatchLogsSinkConfig.class);
        mockCredentialSupplier = mock(AwsCredentialsSupplier.class);
        mockAwsConfig = mock(AwsConfig.class);
        thresholdConfig = new ThresholdConfig();
        mockHeaderOverrides = new HashMap<>();
        mockHeaderOverrides.put("X-Test-Header", "test-value");
        mockCloudWatchLogsMetrics = mock(CloudWatchLogsMetrics.class);
        mockClient = mock(CloudWatchLogsClient.class);
        DistributionSummary summary = mock(DistributionSummary.class);
        when(mockPluginMetrics.summary(anyString())).thenReturn(summary);

        when(mockCloudWatchLogsSinkConfig.getDlq()).thenReturn(null);
        when(mockCloudWatchLogsSinkConfig.getAwsConfig()).thenReturn(mockAwsConfig);
        when(mockCloudWatchLogsSinkConfig.getThresholdConfig()).thenReturn(thresholdConfig);
        when(mockCloudWatchLogsSinkConfig.getHeaderOverrides()).thenReturn(new HashMap<>());
        when(mockCloudWatchLogsSinkConfig.getLogGroup()).thenReturn(TEST_LOG_GROUP);
        when(mockCloudWatchLogsSinkConfig.getLogStream()).thenReturn(TEST_LOG_STREAM);
        when(mockCloudWatchLogsSinkConfig.getMaxRetries()).thenReturn(TEST_MAX_RETRIES);
        when(mockCloudWatchLogsSinkConfig.getWorkers()).thenReturn(10);

        when(mockPluginSetting.getName()).thenReturn(TEST_PLUGIN_NAME);
        when(mockPluginSetting.getPipelineName()).thenReturn(TEST_PIPELINE_NAME);
    }

    CloudWatchLogsSink getTestCloudWatchSink() {
        return new CloudWatchLogsSink(mockPluginSetting, mockPluginMetrics, mockPluginFactory, mockCloudWatchLogsSinkConfig,
                mockCredentialSupplier);
    }

    Collection<Record<Event>> getMockedRecords() {
        Collection<Record<Event>> testCollection = new ArrayList<>();
        Record<Event> mockedEvent = new Record<>(JacksonEvent.fromMessage(""));
        Record<Event> spyEvent = spy(mockedEvent);

        testCollection.add(spyEvent);

        return testCollection;
    }

    @Test
    void WHEN_sink_is_initialized_THEN_sink_is_ready_returns_true() {
        try(MockedStatic<CloudWatchLogsClientFactory> mockedStatic = mockStatic(CloudWatchLogsClientFactory.class)) {
            mockedStatic.when(() -> CloudWatchLogsClientFactory.createCwlClient(any(AwsConfig.class),
                            any(AwsCredentialsSupplier.class), any(), any()))
                    .thenReturn(mockClient);

            CloudWatchLogsSink testCloudWatchSink = getTestCloudWatchSink();
            testCloudWatchSink.doInitialize();
            assertTrue(testCloudWatchSink.isReady());
        }
    }

    @Test
    void WHEN_awsConfig_and_awsCredentialsSupplier_null_THEN_should_throw() {
        mockCredentialSupplier = null;
        when(mockCloudWatchLogsSinkConfig.getAwsConfig()).thenReturn(null);
        try(MockedStatic<CloudWatchLogsClientFactory> mockedStatic = mockStatic(CloudWatchLogsClientFactory.class)) {
            mockedStatic.when(() -> CloudWatchLogsClientFactory.createCwlClient(any(AwsConfig.class),
                            any(AwsCredentialsSupplier.class), any(), any()))
                    .thenReturn(mockClient);

            assertThrows(RuntimeException.class, ()-> getTestCloudWatchSink());
        }
    }

    @Test
    void WHEN_given_sample_empty_records_THEN_records_are_processed() {
        try(MockedStatic<CloudWatchLogsClientFactory> mockedStatic = mockStatic(CloudWatchLogsClientFactory.class)) {
            mockedStatic.when(() -> CloudWatchLogsClientFactory.createCwlClient(any(AwsConfig.class),
                            any(AwsCredentialsSupplier.class), any(), any()))
                    .thenReturn(mockClient);

            CloudWatchLogsSink testCloudWatchSink = getTestCloudWatchSink();
            testCloudWatchSink.doInitialize();
            Collection<Record<Event>> spyEvents = getMockedRecords();

            testCloudWatchSink.doOutput(spyEvents);

            for (Record<Event> spyEvent : spyEvents) {
                verify(spyEvent, atLeast(1)).getData();
            }
        }
    }

    @Test
    void WHEN_given_sample_empty_records_THEN_records_are_not_processed() {
        try(MockedStatic<CloudWatchLogsClientFactory> mockedStatic = mockStatic(CloudWatchLogsClientFactory.class)) {
            mockedStatic.when(() -> CloudWatchLogsClientFactory.createCwlClient(any(AwsConfig.class),
                            any(AwsCredentialsSupplier.class), any(), any()))
                    .thenReturn(mockClient);

            CloudWatchLogsSink testCloudWatchSink = getTestCloudWatchSink();
            testCloudWatchSink.doInitialize();
            Collection<Record<Event>> spyEvents = spy(ArrayList.class);

            assertTrue(spyEvents.isEmpty());

            testCloudWatchSink.doOutput(spyEvents);
            verify(spyEvents, times(2)).isEmpty();
        }
    }

    @Test
    void WHEN_header_overrides_is_empty_THEN_empty_map_is_passed_to_client_factory() {
        Map<String, String> emptyHeaders = new HashMap<>();
        when(mockCloudWatchLogsSinkConfig.getHeaderOverrides()).thenReturn(emptyHeaders);

        try(MockedStatic<CloudWatchLogsClientFactory> mockedStatic = mockStatic(CloudWatchLogsClientFactory.class)) {
            mockedStatic.when(() -> CloudWatchLogsClientFactory.createCwlClient(any(AwsConfig.class),
                            any(AwsCredentialsSupplier.class), any(), any()))
                    .thenReturn(mockClient);

            CloudWatchLogsSink testCloudWatchSink = getTestCloudWatchSink();

            mockedStatic.verify(() -> CloudWatchLogsClientFactory.createCwlClient(
                    eq(mockAwsConfig),
                    eq(mockCredentialSupplier),
                    eq(emptyHeaders),
                    any()));
        }
    }

    @Test
    void WHEN_header_overrides_is_provided_THEN_headers_are_passed_to_client_factory() {
        when(mockCloudWatchLogsSinkConfig.getHeaderOverrides()).thenReturn(mockHeaderOverrides);

        try(MockedStatic<CloudWatchLogsClientFactory> mockedStatic = mockStatic(CloudWatchLogsClientFactory.class)) {
            mockedStatic.when(() -> CloudWatchLogsClientFactory.createCwlClient(any(AwsConfig.class),
                            any(AwsCredentialsSupplier.class), any(), any()))
                    .thenReturn(mockClient);

            CloudWatchLogsSink testCloudWatchSink = getTestCloudWatchSink();

            mockedStatic.verify(() -> CloudWatchLogsClientFactory.createCwlClient(
                    eq(mockAwsConfig),
                    eq(mockCredentialSupplier),
                    eq(mockHeaderOverrides),
                    any()));
        }
    }

    @Test
    void WHEN_sink_initialization_with_header_overrides_THEN_sink_is_ready() {
        when(mockCloudWatchLogsSinkConfig.getHeaderOverrides()).thenReturn(mockHeaderOverrides);

        try(MockedStatic<CloudWatchLogsClientFactory> mockedStatic = mockStatic(CloudWatchLogsClientFactory.class)) {
            mockedStatic.when(() -> CloudWatchLogsClientFactory.createCwlClient(any(AwsConfig.class),
                            any(AwsCredentialsSupplier.class), any(), any()))
                    .thenReturn(mockClient);

            CloudWatchLogsSink testCloudWatchSink = getTestCloudWatchSink();
            testCloudWatchSink.doInitialize();

            assertTrue(testCloudWatchSink.isReady());
        }
    }

    @Test
    void WHEN_sink_has_no_dlq_config_THEN_retries_set_to_maxint() {
        when(mockCloudWatchLogsSinkConfig.getHeaderOverrides()).thenReturn(mockHeaderOverrides);

        try(MockedStatic<CloudWatchLogsClientFactory> mockedStatic = mockStatic(CloudWatchLogsClientFactory.class)) {
            final MockedConstruction<CloudWatchLogsDispatcher> dispatcherMock =
                mockConstruction(CloudWatchLogsDispatcher.class, (mock, context) -> {
                numRetries = (int)context.arguments().get(7);
            });

            mockedStatic.when(() -> CloudWatchLogsClientFactory.createCwlClient(any(AwsConfig.class),
                            any(AwsCredentialsSupplier.class), any(), any()))
                    .thenReturn(mockClient);

            CloudWatchLogsSink testCloudWatchSink = getTestCloudWatchSink();
            testCloudWatchSink.doInitialize();
            dispatcherMock.close();

        }
        assertThat(numRetries, equalTo(Integer.MAX_VALUE));
    }

    @Test
    void WHEN_sink_has_dlq_config_THEN_retries_set_to_user_configured_value() {
        PluginModel dlqConfig = mock(PluginModel.class);
        when(mockCloudWatchLogsSinkConfig.getDlq()).thenReturn(dlqConfig);
        when(mockCloudWatchLogsSinkConfig.getHeaderOverrides()).thenReturn(mockHeaderOverrides);
        when(mockAwsConfig.getAwsRegion()).thenReturn(Region.of("us-west-2"));
        when(mockAwsConfig.getAwsStsRoleArn()).thenReturn("role");

        try(MockedStatic<CloudWatchLogsClientFactory> mockedStatic = mockStatic(CloudWatchLogsClientFactory.class)) {
            final MockedConstruction<CloudWatchLogsDispatcher> dispatcherMock =
                mockConstruction(CloudWatchLogsDispatcher.class, (mock, context) -> {
                numRetries = (int)context.arguments().get(7);
            });
            final MockedConstruction<DlqPushHandler> dlqMock =
                mockConstruction(DlqPushHandler.class, (mock, context) -> {
                });

            mockedStatic.when(() -> CloudWatchLogsClientFactory.createCwlClient(any(AwsConfig.class),
                            any(AwsCredentialsSupplier.class), any(), any()))
                    .thenReturn(mockClient);

            CloudWatchLogsSink testCloudWatchSink = getTestCloudWatchSink();
            testCloudWatchSink.doInitialize();
            dispatcherMock.close();
        }
        assertThat(numRetries, equalTo(TEST_MAX_RETRIES));
    }

}
