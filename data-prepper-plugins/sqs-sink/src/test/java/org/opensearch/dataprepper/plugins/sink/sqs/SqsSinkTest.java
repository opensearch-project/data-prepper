/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.sqs;

import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.mockito.MockedStatic;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import org.opensearch.dataprepper.plugins.source.sqs.common.SqsClientFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import org.opensearch.dataprepper.aws.api.AwsConfig;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.plugins.codec.json.JsonOutputCodec;
import org.opensearch.dataprepper.plugins.codec.json.JsonOutputCodecConfig;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

public class SqsSinkTest {
    private static final String TEST_CODEC_PLUGIN_NAME = "json";
    private static final String TEST_PLUGIN_NAME = "testPluginName";
    private static final String TEST_PIPELINE_NAME = "testPipelineName";
    @Mock
    private SqsSinkConfig sqsSinkConfig;
    @Mock
    private SinkContext sinkContext;
    @Mock
    private ExpressionEvaluator expressionEvaluator;
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private PluginSetting pluginSetting;
    @Mock
    private PluginFactory pluginFactory;
    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;
    @Mock
    private AwsCredentialsProvider awsCredentialsProvider;
    @Mock
    private AwsConfig awsConfig;

    private SqsClient sqsClient;
    private PluginModel codecConfig;
    private String queueUrl;

    @BeforeEach
    void setup() {
        pluginSetting = mock(PluginSetting.class);
        pluginMetrics = mock(PluginMetrics.class);
        pluginFactory = mock(PluginFactory.class);
        sqsSinkConfig = mock(SqsSinkConfig.class);
        sinkContext = mock(SinkContext.class);
        when(sinkContext.getExcludeKeys()).thenReturn(null);
        when(sinkContext.getIncludeKeys()).thenReturn(null);
        when(sinkContext.getTagsTargetKey()).thenReturn(null);
        sqsClient = mock(SqsClient.class);
        expressionEvaluator = mock(ExpressionEvaluator.class);
        awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);
        awsCredentialsProvider = mock(AwsCredentialsProvider.class);
        when(sqsSinkConfig.getDlq()).thenReturn(null);
        codecConfig = mock(PluginModel.class);
        when(codecConfig.getPluginName()).thenReturn(TEST_CODEC_PLUGIN_NAME);
        when(codecConfig.getPluginSettings()).thenReturn(Map.of());
        when(sqsSinkConfig.getCodec()).thenReturn(codecConfig);
        queueUrl = UUID.randomUUID().toString();
        when(sqsSinkConfig.getQueueUrl()).thenReturn(queueUrl);
        when(pluginFactory.loadPlugin(eq(OutputCodec.class), any())).thenReturn(new JsonOutputCodec(new JsonOutputCodecConfig()));
        awsConfig = mock(AwsConfig.class);
        when(awsConfig.getAwsRegion()).thenReturn(Region.of("us-west-2"));
        when(sqsSinkConfig.getAwsConfig()).thenReturn(awsConfig);
        when(pluginSetting.getName()).thenReturn(TEST_PLUGIN_NAME);
        when(pluginSetting.getPipelineName()).thenReturn(TEST_PIPELINE_NAME);

    }
    SqsSink createObjectUnderTest() {
        return new SqsSink(pluginSetting, pluginMetrics, pluginFactory, sqsSinkConfig, sinkContext, expressionEvaluator, awsCredentialsSupplier);
    }

    @Test
    void TestBasic() {
        try(MockedStatic<SqsClientFactory> mockedStatic = mockStatic(SqsClientFactory.class)) {
            mockedStatic.when(() -> SqsClientFactory.createSqsClient(any(Region.class),
                            any(AwsCredentialsProvider.class)))
                    .thenReturn(sqsClient);

            SqsSink sqsSink = createObjectUnderTest();
            sqsSink.doInitialize();
            assertTrue(sqsSink.isReady());
        }
    }

    @Test
    void TestWithInvalidCodec() {
        when(codecConfig.getPluginName()).thenReturn("badCodec");
        awsCredentialsSupplier = null;
        when(sqsSinkConfig.getAwsConfig()).thenReturn(null);
        try(MockedStatic<SqsClientFactory> mockedStatic = mockStatic(SqsClientFactory.class)) {
            mockedStatic.when(() -> SqsClientFactory.createSqsClient(any(Region.class),
                            any(AwsCredentialsProvider.class)))
                    .thenReturn(sqsClient);

            assertThrows(RuntimeException.class, ()-> createObjectUnderTest());
        }
    }

    @Test
    void TestWithNullAwsConfig() {
        awsCredentialsSupplier = null;
        when(sqsSinkConfig.getAwsConfig()).thenReturn(null);
        try(MockedStatic<SqsClientFactory> mockedStatic = mockStatic(SqsClientFactory.class)) {
            mockedStatic.when(() -> SqsClientFactory.createSqsClient(any(Region.class),
                            any(AwsCredentialsProvider.class)))
                    .thenReturn(sqsClient);

            assertThrows(RuntimeException.class, ()-> createObjectUnderTest());
        }
    }

    @Test
    void TestForDefaultCodec() {
        when(sqsSinkConfig.getCodec()).thenReturn(codecConfig);
        try(MockedStatic<SqsClientFactory> mockedStatic = mockStatic(SqsClientFactory.class)) {
            mockedStatic.when(() -> SqsClientFactory.createSqsClient(any(Region.class),
                            any(AwsCredentialsProvider.class)))
                    .thenReturn(sqsClient);

            SqsSink sqsSink = createObjectUnderTest();
            sqsSink.doInitialize();
            assertTrue(sqsSink.isReady());
        }
    }

    @Test
    void TestSinkOutputWithEvents() {
        try(MockedStatic<SqsClientFactory> mockedStatic = mockStatic(SqsClientFactory.class)) {
            mockedStatic.when(() -> SqsClientFactory.createSqsClient(any(Region.class),
                            any(AwsCredentialsProvider.class)))
                    .thenReturn(sqsClient);

            SqsSink sqsSink = createObjectUnderTest();
            sqsSink.doInitialize();
            Collection<Record<Event>> spyEvents = getMockedRecords();

            sqsSink.doOutput(spyEvents);

            for (Record<Event> spyEvent : spyEvents) {
                verify(spyEvent, atLeast(1)).getData();
            }
        }
    }

    @Test
    void TestOutputWithEmptyEvents() {
        try(MockedStatic<SqsClientFactory> mockedStatic = mockStatic(SqsClientFactory.class)) {
            mockedStatic.when(() -> SqsClientFactory.createSqsClient(any(Region.class),
                            any(AwsCredentialsProvider.class)))
                    .thenReturn(sqsClient);

            SqsSink sqsSink = createObjectUnderTest();
            sqsSink.doInitialize();
            Collection<Record<Event>> spyEvents = spy(ArrayList.class);

            assertTrue(spyEvents.isEmpty());

            sqsSink.doOutput(spyEvents);
            verify(spyEvents, times(2)).isEmpty();
        }
    }

    Collection<Record<Event>> getMockedRecords() {
        Collection<Record<Event>> testCollection = new ArrayList<>();
        Record<Event> mockedEvent = new Record<>(JacksonEvent.fromMessage(""));
        Record<Event> spyEvent = spy(mockedEvent);
        testCollection.add(spyEvent);
        return testCollection;
    }


}

