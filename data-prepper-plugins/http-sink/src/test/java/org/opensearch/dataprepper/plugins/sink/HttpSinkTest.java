/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.plugins.sink.configuration.AuthTypeOptions;
import org.opensearch.dataprepper.plugins.accumulator.BufferTypeOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.HttpSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.configuration.ThresholdOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.HTTPMethodOptions;
import org.opensearch.dataprepper.plugins.sink.handler.HttpAuthOptions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HttpSinkTest {

     HTTPSink httpSink;

    private PluginSetting pluginSetting;
    private PluginFactory pluginFactory;

    private HttpSinkConfiguration httpSinkConfiguration;

    private PipelineDescription pipelineDescription;

    private AwsCredentialsSupplier awsCredentialsSupplier;

    private SinkContext sinkContext;

    private ThresholdOptions thresholdOptions;

    private AwsAuthenticationOptions awsAuthenticationOptions;

    private OutputCodec codec;

    private HttpAuthOptions httpAuthOptions;

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
        httpAuthOptions = mock(HttpAuthOptions.class);
        awsAuthenticationOptions = mock(AwsAuthenticationOptions.class);
        when(pluginSetting.getPipelineName()).thenReturn("log-pipeline");
        PluginModel codecConfiguration = new PluginModel("http", new HashMap<>());
        when(httpSinkConfiguration.getCodec()).thenReturn(codecConfiguration);
        when(httpSinkConfiguration.getBufferType()).thenReturn(BufferTypeOptions.LOCALFILE);
        when(httpAuthOptions.getUrl()).thenReturn("http://localhost:8080");
        when(httpSinkConfiguration.getHttpMethod()).thenReturn(HTTPMethodOptions.POST);
        when(httpSinkConfiguration.getAuthType()).thenReturn(AuthTypeOptions.UNAUTHENTICATED);
        Map<String, Object> dlqSetting = new HashMap<>();
        dlqSetting.put("bucket", "dlq.test");
        dlqSetting.put("key_path_prefix", "\\dlq");
        PluginModel dlq = new PluginModel("s3",dlqSetting);
        when(httpSinkConfiguration.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        when(httpSinkConfiguration.getDlqStsRoleARN()).thenReturn("arn:aws:iam::1234567890:role/app-test");
        when(httpSinkConfiguration.getDlqStsRegion()).thenReturn("ap-south-1");
        when(httpSinkConfiguration.getDlq()).thenReturn(dlq);
        when(httpSinkConfiguration.getThresholdOptions()).thenReturn(thresholdOptions);
        when(thresholdOptions.getEventCount()).thenReturn(10);
        when(httpSinkConfiguration.getDlqFile()).thenReturn("\\dlq");
    }

    private HTTPSink createObjectUnderTest() {
        return new HTTPSink(pluginSetting, httpSinkConfiguration, pluginFactory, pipelineDescription,
                awsCredentialsSupplier);
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
