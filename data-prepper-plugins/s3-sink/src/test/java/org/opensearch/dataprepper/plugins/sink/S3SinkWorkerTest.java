/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import java.io.IOException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.sink.accumulator.BufferTypeOptions;
import org.opensearch.dataprepper.plugins.sink.accumulator.InMemoryBuffer;
import org.opensearch.dataprepper.plugins.sink.accumulator.LocalFileBuffer;
import org.opensearch.dataprepper.plugins.sink.codec.Codec;
import org.opensearch.dataprepper.plugins.sink.codec.JsonCodec;
import org.opensearch.dataprepper.plugins.sink.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.BucketOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.ObjectKeyOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.ThresholdOptions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

class S3SinkWorkerTest {

    private S3SinkConfig s3SinkConfig;
    private ThresholdOptions thresholdOptions;
    private AwsAuthenticationOptions awsAuthenticationOptions;
    private AwsCredentialsProvider awsCredentialsProvider;
    private BucketOptions bucketOptions;
    private ObjectKeyOptions objectKeyOptions;
    private JsonCodec codec;
    private PluginSetting pluginSetting;
    private PluginFactory pluginFactory;
    private PluginModel pluginModel;

    @BeforeEach
    void setUp() throws Exception {

        s3SinkConfig = mock(S3SinkConfig.class);
        thresholdOptions = mock(ThresholdOptions.class);
        bucketOptions = mock(BucketOptions.class);
        objectKeyOptions = mock(ObjectKeyOptions.class);
        awsAuthenticationOptions = mock(AwsAuthenticationOptions.class);
        codec = mock(JsonCodec.class);

        pluginSetting = mock(PluginSetting.class);
        pluginModel = mock(PluginModel.class);
        pluginFactory = mock(PluginFactory.class);

        when(s3SinkConfig.getThresholdOptions()).thenReturn(thresholdOptions);
        when(s3SinkConfig.getThresholdOptions().getEventCount()).thenReturn(100);
        when(s3SinkConfig.getThresholdOptions().getMaximumSize()).thenReturn(ByteCount.parse("1kb"));
        when(s3SinkConfig.getThresholdOptions().getEventCollect()).thenReturn(Duration.ofSeconds(5));

        when(objectKeyOptions.getNamePattern()).thenReturn("my-elb-%{yyyy-MM-dd'T'hh-mm-ss}");

        when(s3SinkConfig.getBufferType()).thenReturn(BufferTypeOptions.LOCALFILE);

        when(s3SinkConfig.getBucketOptions()).thenReturn(bucketOptions);
        when(s3SinkConfig.getBucketOptions().getObjectKeyOptions()).thenReturn(objectKeyOptions);
        when(s3SinkConfig.getBucketOptions().getBucketName()).thenReturn("dataprepper");
        when(s3SinkConfig.getBucketOptions().getObjectKeyOptions().getPathPrefix()).thenReturn("logdata/");

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
    void verify_interactions() throws InterruptedException {
        BlockingQueue<Event> queue = generateEventQueue();

        S3SinkWorker worker = mock(S3SinkWorker.class);
        worker.bufferAccumulator(queue);

        InMemoryBuffer inMemoryBuffer = mock(InMemoryBuffer.class);
        verifyNoInteractions(inMemoryBuffer);

        LocalFileBuffer localFileBuffer = mock(LocalFileBuffer.class);
        verifyNoInteractions(localFileBuffer);
    }

    @Test
    void test_cover_localFile_bufferAccumulator() throws IOException {
        when(s3SinkConfig.getBufferType()).thenReturn(BufferTypeOptions.LOCALFILE);
        when(codec.parse(any())).thenReturn("{\"message\":\"31824252-adba-4c47-a2ac-05d16c5b8140\"}");
        S3SinkService s3SinkService = new S3SinkService(s3SinkConfig);
        S3Client s3Client = s3SinkService.createS3Client();
        S3SinkWorker worker = new S3SinkWorker(s3Client, s3SinkConfig, codec);
        assertNotNull(worker);
        worker = mock(S3SinkWorker.class);
        BlockingQueue<Event> queue = generateEventQueue();
        worker.bufferAccumulator(queue);
        verify(worker, atLeastOnce()).bufferAccumulator(queue);
    }

    @Test
    void test_cover_inMemory_bufferAccumulator() throws IOException {
        when(s3SinkConfig.getBufferType()).thenReturn(BufferTypeOptions.INMEMORY);
        when(codec.parse(any())).thenReturn("{\"message\":\"31824252-adba-4c47-a2ac-05d16c5b8140\"}");
        S3SinkService s3SinkService = new S3SinkService(s3SinkConfig);
        S3Client s3Client = s3SinkService.createS3Client();
        S3SinkWorker worker = new S3SinkWorker(s3Client, s3SinkConfig, codec);
        assertNotNull(worker);
        worker = mock(S3SinkWorker.class);
        BlockingQueue<Event> queue = generateEventQueue();
        worker.bufferAccumulator(queue);
        verify(worker, atLeastOnce()).bufferAccumulator(queue);
    }

    @Test
    void test_cover_exception() throws IOException {
        when(s3SinkConfig.getBufferType()).thenReturn(BufferTypeOptions.LOCALFILE);
        when(codec.parse(any())).thenReturn("{\"message\":\"31824252-adba-4c47-a2ac-05d16c5b8140\"}");
        S3SinkWorker worker = new S3SinkWorker(null, s3SinkConfig, codec);
        assertNotNull(worker);
        assertThrows(Throwable.class, () -> worker.bufferAccumulator(generateEventQueue()));
    }

    private BlockingQueue<Event> generateEventQueue() {
        BlockingQueue<Event> eventQueue = new ArrayBlockingQueue<>(100);
        for (int i = 0; i < 50; i++) {
            final Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
            eventQueue.add(event);
        }
        return eventQueue;
    }
}