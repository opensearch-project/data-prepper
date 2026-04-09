/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.http;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.aws.api.AwsConfig;
import org.opensearch.dataprepper.plugins.sink.http.configuration.HttpSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.http.configuration.ThresholdOptions;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HttpSinkTest {

     HttpSink httpSink;

    private PluginSetting pluginSetting;
    private PluginFactory pluginFactory;

    private HttpSinkConfiguration httpSinkConfiguration;

    private PipelineDescription pipelineDescription;

    private AwsCredentialsSupplier awsCredentialsSupplier;

    private SinkContext sinkContext;

    private ThresholdOptions thresholdOptions;

    private AwsConfig awsConfig;

    private OutputCodec codec;

    private PluginMetrics pluginMetrics;


    @BeforeEach
    void setUp() {
        pluginSetting = mock(PluginSetting.class);
        pluginFactory = mock(PluginFactory.class);
        httpSinkConfiguration = mock(HttpSinkConfiguration.class);
        pipelineDescription = mock(PipelineDescription.class);
        awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);
        thresholdOptions = mock(ThresholdOptions.class);
        sinkContext = mock(SinkContext.class);
        codec = mock(OutputCodec.class);
        awsConfig = mock(AwsConfig.class);
        pluginMetrics = mock(PluginMetrics.class);
        when(pluginSetting.getPipelineName()).thenReturn("log-pipeline");
        when(pipelineDescription.getPipelineName()).thenReturn("log-pipeline");
        PluginModel codecConfiguration = new PluginModel("ndjson", new HashMap<>());
        when(httpSinkConfiguration.getCodec()).thenReturn(codecConfiguration);
        when(pluginFactory.loadPlugin(any(), any())).thenReturn(codec);
        when(httpSinkConfiguration.getUrl()).thenReturn("http://localhost:8080");
        when(httpSinkConfiguration.getConnectionTimeout()).thenReturn(Duration.ofSeconds(10));
        Map<String, Object> dlqSetting = new HashMap<>();
        dlqSetting.put("bucket", "dlq.test");
        dlqSetting.put("key_path_prefix", "\\dlq");
        PluginModel dlq = new PluginModel("s3",dlqSetting);
        when(httpSinkConfiguration.getAwsConfig()).thenReturn(awsConfig);
        when(httpSinkConfiguration.getThresholdOptions()).thenReturn(thresholdOptions);
        when(thresholdOptions.getMaxEvents()).thenReturn(10);
        when(thresholdOptions.getMaxRequestSize()).thenReturn(ByteCount.parse("50mb"));
        when(thresholdOptions.getFlushTimeOut()).thenReturn(Duration.ofSeconds(10));
        when(sinkContext.getIncludeKeys()).thenReturn(new ArrayList<>());
        when(sinkContext.getExcludeKeys()).thenReturn(new ArrayList<>());
    }

    private HttpSink createObjectUnderTest() {
        return new HttpSink(pluginSetting, httpSinkConfiguration, pluginFactory, pipelineDescription, sinkContext,
                awsCredentialsSupplier, pluginMetrics);
    }
    @Test
    void test_http_sink_plugin_isReady_positive() {
        httpSink = createObjectUnderTest();
        Assertions.assertNotNull(httpSink);
        httpSink.doInitialize();
        assertTrue(httpSink.isReady(), "http sink is initialized and ready to work");
    }

    @Test
    void test_http_Sink_plugin_isReady_negative() {
        httpSink = createObjectUnderTest();
        Assertions.assertNotNull(httpSink);
        assertFalse(httpSink.isReady(), "httpSink sink is not initialized and not ready to work");
    }

    @Test
    void test_doOutput_with_empty_records() {
        httpSink = createObjectUnderTest();
        Assertions.assertNotNull(httpSink);
        httpSink.doInitialize();
        Collection<Record<Event>> records = new ArrayList<>();
        httpSink.doOutput(records);
    }
}
