/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.lambda;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.plugins.sink.lambda.config.AwsAuthenticationOptions;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaClient;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class LambdaSinkTest {

    public static final String S3_REGION = "us-east-1";
    public static final String CODEC_PLUGIN_NAME = "json";
    public static final String SINK_PLUGIN_NAME = "lambda";
    public static final String SINK_PIPELINE_NAME = "lambda-sink-pipeline";
    private LambdaSinkConfig lambdaSinkConfig;
    private LambdaSink lambdaSink;
    private PluginSetting pluginSetting;
    private PluginFactory pluginFactory;
    private AwsCredentialsSupplier awsCredentialsSupplier;
    private SinkContext sinkContext;

    @BeforeEach
    void setUp() {
        lambdaSinkConfig = mock(LambdaSinkConfig.class);
        sinkContext = mock(SinkContext.class);
        AwsAuthenticationOptions awsAuthenticationOptions = mock(AwsAuthenticationOptions.class);
        pluginSetting = mock(PluginSetting.class);
        PluginModel pluginModel = mock(PluginModel.class);
        pluginFactory = mock(PluginFactory.class);
        awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);
        Map<String,Object> dlqMap = mock(HashMap.class);
        LambdaClient lambdaClient = mock(LambdaClient.class);


        when(lambdaSinkConfig.getDlq()).thenReturn(pluginModel);
        when(pluginModel.getPluginSettings()).thenReturn(dlqMap);
        when(lambdaSinkConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.of(S3_REGION));
        when(pluginModel.getPluginName()).thenReturn(CODEC_PLUGIN_NAME);
        when(pluginSetting.getName()).thenReturn(SINK_PLUGIN_NAME);
        when(pluginSetting.getPipelineName()).thenReturn(SINK_PIPELINE_NAME);
    }

    private LambdaSink createObjectUnderTest() {
        return new LambdaSink(pluginSetting, lambdaSinkConfig, pluginFactory, sinkContext, awsCredentialsSupplier);
    }

    @Test
    void test_lambda_sink_plugin_isReady_positive() {
        lambdaSink = createObjectUnderTest();
        Assertions.assertNotNull(lambdaSink);
        Assertions.assertNotNull(lambdaSinkConfig);
        lambdaSink.doInitialize();
        assertTrue(lambdaSink.isReady(), "lambda sink is not initialized and not ready to work");
    }

    @Test
    void test_lambda_sink_plugin_isReady_negative() {
        lambdaSink = createObjectUnderTest();
        Assertions.assertNotNull(lambdaSink);
        assertFalse(lambdaSink.isReady(), "lambda sink is initialized and ready to work");
    }
}
