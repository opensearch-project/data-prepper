/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.sns;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.plugins.sink.sns.configuration.AwsAuthenticationOptions;
import software.amazon.awssdk.regions.Region;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SnsSinkTest {

    public static final int MAX_EVENTS = 100;
    public static final int MAX_RETRIES = 5;
    public static final String BUCKET_NAME = "dataprepper";
    public static final String S3_REGION = "us-east-1";
    public static final String MAXIMUM_SIZE = "1kb";
    public static final String OBJECT_KEY_NAME_PATTERN = "my-elb-%{yyyy-MM-dd'T'hh-mm-ss}";
    public static final String CODEC_PLUGIN_NAME = "json";
    public static final String SINK_PLUGIN_NAME = "sns";
    public static final String SINK_PIPELINE_NAME = "sns-sink-pipeline";
    private SnsSinkConfig snsSinkConfig;
    private SnsSink snsSink;
    private PluginSetting pluginSetting;
    private PluginFactory pluginFactory;
    private AwsCredentialsSupplier awsCredentialsSupplier;
    private SinkContext sinkContext;
    private PluginModel pluginModel;

    @BeforeEach
    void setUp() {
        snsSinkConfig = mock(SnsSinkConfig.class);
        sinkContext = mock(SinkContext.class);
        AwsAuthenticationOptions awsAuthenticationOptions = mock(AwsAuthenticationOptions.class);
        pluginSetting = mock(PluginSetting.class);
        PluginModel pluginModel = mock(PluginModel.class);
        pluginFactory = mock(PluginFactory.class);
        awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);
        Map<String,Object> dlqMap = mock(HashMap.class);

        when(snsSinkConfig.getMessageGroupId()).thenReturn("/message");
        when(snsSinkConfig.getMessageDeduplicationId()).thenReturn("/message");
        when(snsSinkConfig.getDlq()).thenReturn(pluginModel);
        when(pluginModel.getPluginSettings()).thenReturn(dlqMap);
        when(snsSinkConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.of(S3_REGION));
        when(snsSinkConfig.getCodec()).thenReturn(pluginModel);
        when(pluginModel.getPluginName()).thenReturn(CODEC_PLUGIN_NAME);
        when(pluginSetting.getName()).thenReturn(SINK_PLUGIN_NAME);
        when(pluginSetting.getPipelineName()).thenReturn(SINK_PIPELINE_NAME);
    }

    private SnsSink createObjectUnderTest() {
        return new SnsSink(pluginSetting, snsSinkConfig, pluginFactory, sinkContext,mock(ExpressionEvaluator.class), awsCredentialsSupplier);
    }

    @Test
    void test_sns_sink_plugin_isReady_positive() {
        snsSink = createObjectUnderTest();
        Assertions.assertNotNull(snsSink);
        Assertions.assertNotNull(snsSinkConfig);
        snsSink.doInitialize();
        assertTrue(snsSink.isReady(), "sns sink is not initialized and not ready to work");
    }

    @Test
    void test_sns_sink_plugin_isReady_negative() {
        snsSink = createObjectUnderTest();
        Assertions.assertNotNull(snsSink);
        assertFalse(snsSink.isReady(), "sns sink is initialized and ready to work");
    }
}
