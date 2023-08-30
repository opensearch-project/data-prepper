/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.sink.s3.accumulator.BufferTypeOptions;
import org.opensearch.dataprepper.plugins.sink.s3.compression.CompressionOption;
import org.opensearch.dataprepper.plugins.sink.s3.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.sink.s3.configuration.ObjectKeyOptions;
import org.opensearch.dataprepper.plugins.sink.s3.configuration.ThresholdOptions;
import software.amazon.awssdk.regions.Region;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class S3SinkTest {
    public static final int MAX_EVENTS = 100;
    public static final int MAX_RETRIES = 5;
    public static final String BUCKET_NAME = "dataprepper";
    public static final String S3_REGION = "us-east-1";
    public static final String MAXIMUM_SIZE = "1kb";
    public static final String OBJECT_KEY_NAME_PATTERN = "my-elb-%{yyyy-MM-dd'T'hh-mm-ss}";
    public static final String CODEC_PLUGIN_NAME = "json";
    public static final String SINK_PLUGIN_NAME = "s3";
    public static final String SINK_PIPELINE_NAME = "S3-sink-pipeline";
    private S3SinkConfig s3SinkConfig;
    private S3Sink s3Sink;
    private PluginSetting pluginSetting;
    private PluginFactory pluginFactory;
    private AwsCredentialsSupplier awsCredentialsSupplier;
    private SinkContext sinkContext;
    private OutputCodec codec;

    @BeforeEach
    void setUp() {

        s3SinkConfig = mock(S3SinkConfig.class);
        sinkContext = mock(SinkContext.class);
        ThresholdOptions thresholdOptions = mock(ThresholdOptions.class);
        AwsAuthenticationOptions awsAuthenticationOptions = mock(AwsAuthenticationOptions.class);
        codec = mock(OutputCodec.class);
        ObjectKeyOptions objectKeyOptions = mock(ObjectKeyOptions.class);
        pluginSetting = mock(PluginSetting.class);
        PluginModel pluginModel = mock(PluginModel.class);
        pluginFactory = mock(PluginFactory.class);
        awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);

        when(s3SinkConfig.getBufferType()).thenReturn(BufferTypeOptions.INMEMORY);
        when(s3SinkConfig.getThresholdOptions()).thenReturn(thresholdOptions);
        when(s3SinkConfig.getThresholdOptions().getEventCount()).thenReturn(MAX_EVENTS);
        when(s3SinkConfig.getThresholdOptions().getMaximumSize()).thenReturn(ByteCount.parse(MAXIMUM_SIZE));
        when(s3SinkConfig.getThresholdOptions().getEventCollectTimeOut()).thenReturn(Duration.ofSeconds(MAX_RETRIES));
        when(s3SinkConfig.getCompression()).thenReturn(CompressionOption.NONE);
        when(objectKeyOptions.getNamePattern()).thenReturn(OBJECT_KEY_NAME_PATTERN);
        when(s3SinkConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.of(S3_REGION));
        when(s3SinkConfig.getCodec()).thenReturn(pluginModel);
        when(pluginModel.getPluginName()).thenReturn(CODEC_PLUGIN_NAME);
        when(pluginFactory.loadPlugin(any(), any())).thenReturn(codec);
        when(pluginSetting.getName()).thenReturn(SINK_PLUGIN_NAME);
        when(pluginSetting.getPipelineName()).thenReturn(SINK_PIPELINE_NAME);
        when(s3SinkConfig.getBucketName()).thenReturn(BUCKET_NAME);
    }

    private S3Sink createObjectUnderTest() {
        return new S3Sink(pluginSetting, s3SinkConfig, pluginFactory, sinkContext, awsCredentialsSupplier);
    }

    @Test
    void test_s3_sink_plugin_isReady_positive() {
        s3Sink = createObjectUnderTest();
        Assertions.assertNotNull(s3Sink);
        s3Sink.doInitialize();
        assertTrue(s3Sink.isReady(), "s3 sink is not initialized and not ready to work");
    }

    @Test
    void test_s3_Sink_plugin_isReady_negative() {
        s3Sink = createObjectUnderTest();
        Assertions.assertNotNull(s3Sink);
        assertFalse(s3Sink.isReady(), "s3 sink is initialized and ready to work");
    }

    @Test
    void test_doOutput_with_empty_records() {
        when(s3SinkConfig.getBucketName()).thenReturn(BUCKET_NAME);
        s3Sink = createObjectUnderTest();
        Assertions.assertNotNull(s3Sink);
        s3Sink.doInitialize();
        Collection<Record<Event>> records = new ArrayList<>();
        s3Sink.doOutput(records);
    }

    @Test
    void constructor_should_call_codec_validateAgainstCodecContext_with_context() {
        createObjectUnderTest();

        ArgumentCaptor<OutputCodecContext> outputCodecContextArgumentCaptor = ArgumentCaptor.forClass(OutputCodecContext.class);
        verify(codec).validateAgainstCodecContext(outputCodecContextArgumentCaptor.capture());

        OutputCodecContext actualCodecContext = outputCodecContextArgumentCaptor.getValue();

        assertThat(actualCodecContext, instanceOf(S3OutputCodecContext.class));
    }

    @Test
    void constructor_should_throw_if_codec_validateAgainstCodecContext_throws() {
        RuntimeException codecException = mock(RuntimeException.class);

        doThrow(codecException).when(codec).validateAgainstCodecContext(any());

        RuntimeException actualException = assertThrows(RuntimeException.class, () -> createObjectUnderTest());
        assertThat(actualException, sameInstance(codecException));
    }
}
