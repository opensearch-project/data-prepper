/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.sink.accumulator.BufferTypeOptions;
import org.opensearch.dataprepper.plugins.sink.codec.Codec;
import org.opensearch.dataprepper.plugins.sink.codec.JsonCodec;
import org.opensearch.dataprepper.plugins.sink.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.ObjectKeyOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.ThresholdOptions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;

class S3SinkTest {

    private S3SinkConfig s3SinkConfig;
    private ThresholdOptions thresholdOptions;
    private AwsAuthenticationOptions awsAuthenticationOptions;
    private AwsCredentialsProvider awsCredentialsProvider;
    private ObjectKeyOptions objectKeyOptions;
    private JsonCodec codec;
    private S3Sink s3Sink;
    private PluginSetting pluginSetting;
    private PluginFactory pluginFactory;
    private PluginModel pluginModel;

    @BeforeEach
    void setUp() {

        s3SinkConfig = mock(S3SinkConfig.class);
        thresholdOptions = mock(ThresholdOptions.class);
        awsAuthenticationOptions = mock(AwsAuthenticationOptions.class);
        codec = mock(JsonCodec.class);
        objectKeyOptions = mock(ObjectKeyOptions.class);

        pluginSetting = mock(PluginSetting.class);
        pluginModel = mock(PluginModel.class);
        pluginFactory = mock(PluginFactory.class);

        when(s3SinkConfig.getThresholdOptions()).thenReturn(thresholdOptions);
        when(s3SinkConfig.getThresholdOptions().getEventCount()).thenReturn(100);
        when(s3SinkConfig.getThresholdOptions().getMaximumSize()).thenReturn(ByteCount.parse("1kb"));
        when(s3SinkConfig.getThresholdOptions().getEventCollect()).thenReturn(Duration.ofSeconds(5));

        when(objectKeyOptions.getNamePattern()).thenReturn("my-elb-%{yyyy-MM-dd'T'hh-mm-ss}");

        when(s3SinkConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.of("us-east-1"));
        when(awsAuthenticationOptions.authenticateAwsConfiguration()).thenReturn(awsCredentialsProvider);

        when(s3SinkConfig.getCodec()).thenReturn(pluginModel);
        when(pluginModel.getPluginName()).thenReturn("json");
        when(pluginFactory.loadPlugin(Codec.class, pluginSetting)).thenReturn(codec);

        when(pluginSetting.getName()).thenReturn("s3");
        when(pluginSetting.getPipelineName()).thenReturn("S3-sink-pipeline");
    }

    @Test
    void test_s3_sink_plugin_isReady_true() {
        s3Sink = new S3Sink(pluginSetting, s3SinkConfig, pluginFactory);
        s3Sink.doInitialize();
        assertTrue(s3Sink.isReady(), "s3 sink is not ready to work");
    }

    @Test
    void test_s3_Sink_plugin_isReady_false() {
        s3Sink = new S3Sink(pluginSetting, s3SinkConfig, pluginFactory);
        assertFalse(s3Sink.isReady(), "s3 sink is ready to work");
    }

    @Test
    void test_doOutput_with_exception() {
        s3Sink = new S3Sink(pluginSetting, s3SinkConfig, pluginFactory);
        when(s3SinkConfig.getThresholdOptions()).thenReturn(null);
        assertThrows(NullPointerException.class, s3Sink::doInitialize);
    }

    @Test
    void test_doOutput_with_data_local_file() {
        when(s3SinkConfig.getBufferType()).thenReturn(BufferTypeOptions.LOCALFILE);
        s3Sink = new S3Sink(pluginSetting, s3SinkConfig, pluginFactory);
        assertNotNull(s3Sink);
        s3Sink.doInitialize();
        s3Sink.doOutput(generateRandomStringEventRecord());
    }

    @Test
    void test_doOutput_with_data_in_memory() {
        when(s3SinkConfig.getBufferType()).thenReturn(BufferTypeOptions.INMEMORY);
        s3Sink = new S3Sink(pluginSetting, s3SinkConfig, pluginFactory);
        assertNotNull(s3Sink);
        s3Sink.doInitialize();
        s3Sink.doOutput(generateRandomStringEventRecord());
    }
    @Test
    void test_doOutput_noInteractions_with_s3SinkService() {
        Collection<Record<Event>> records = generateRandomStringEventRecord();
        S3SinkService s3SinkService = mock(S3SinkService.class);
        S3SinkWorker s3Worker = mock(S3SinkWorker.class);
        s3Sink = new S3Sink(pluginSetting, s3SinkConfig, pluginFactory);
        s3Sink.doInitialize();
        s3Sink.doOutput(records);
        verifyNoInteractions(s3SinkService);
        verifyNoInteractions(s3Worker);
    }

    @Test
    void test_doOutput_invocation_order() {
        Collection<Record<Event>> records = generateRandomStringEventRecord();

        S3SinkService s3SinkService = mock(S3SinkService.class);
        S3SinkWorker worker = mock(S3SinkWorker.class);

        s3SinkService.createS3Client();
        s3SinkService.processRecords(records);
        s3SinkService.accumulateBufferEvents(worker);

        InOrder inOrder = inOrder(s3SinkService);
        inOrder.verify(s3SinkService).createS3Client();
        inOrder.verify(s3SinkService).processRecords(records);
        inOrder.verify(s3SinkService).accumulateBufferEvents(worker);

        verifyNoMoreInteractions(s3SinkService);
    }

    private Collection<Record<Event>> generateRandomStringEventRecord() {

        Collection<Record<Event>> records = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            final Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
            records.add(new Record<>(event));
        }
        return records;
    }
}