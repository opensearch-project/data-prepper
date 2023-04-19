/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.accumulator;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import java.util.NavigableSet;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.plugins.sink.S3SinkConfig;
import org.opensearch.dataprepper.plugins.sink.S3SinkService;
import org.opensearch.dataprepper.plugins.sink.configuration.BucketOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.ObjectKeyOptions;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

@ExtendWith(MockitoExtension.class)
class InMemoryBufferTest {
    private static final String DEFAULT_CODEC_FILE_EXTENSION = "json";
    @Mock
    private S3SinkConfig s3SinkConfig;
    @Mock
    private BucketOptions bucketOptions;
    @Mock
    private ObjectKeyOptions objectKeyOptions;
    @Mock
    private PluginModel pluginModel;

    @BeforeEach
    void setUp() throws Exception {
    }

    @Test
    void verify_interactions_putObject() throws InterruptedException {
        NavigableSet<String> bufferedEventSet = generateSet();
        S3Client s3Client = mock(S3Client.class);

        when(s3SinkConfig.getBucketOptions()).thenReturn(bucketOptions);
        when(s3SinkConfig.getBucketOptions().getObjectKeyOptions()).thenReturn(objectKeyOptions);
        when(s3SinkConfig.getBucketOptions().getBucketName()).thenReturn("dataprepper");
        when(s3SinkConfig.getBucketOptions().getObjectKeyOptions().getPathPrefix()).thenReturn("logdata/");
        when(objectKeyOptions.getNamePattern()).thenReturn("my-elb-%{yyyy-MM-dd'T'hh-mm-ss}");

        InMemoryBuffer inMemoryBuffer = new InMemoryBuffer(s3Client, s3SinkConfig);
        inMemoryBuffer.inMemoryAccumulate(bufferedEventSet);
        verify(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }

    @Test
    void verify_interactions_with_upload() throws InterruptedException {
        NavigableSet<String> bufferedEventSet = generateSet();
        InMemoryBuffer inMemoryBuffer = mock(InMemoryBuffer.class);
        inMemoryBuffer.inMemoryAccumulate(bufferedEventSet);
        verify(inMemoryBuffer, never()).uploadToAmazonS3(any(), any(), any());
    }

    @Test
    void verify_interactions_s3Client() {
        S3SinkService s3SinkService = mock(S3SinkService.class);
        s3SinkService.createS3Client();
        verify(s3SinkService).createS3Client();
        verifyNoMoreInteractions(s3SinkService);
    }

    @Test
    void test_in_order_invocation() throws InterruptedException {
        NavigableSet<String> bufferedEventSet = generateSet();

        InMemoryBuffer inMemoryBuffer = mock(InMemoryBuffer.class);
        inMemoryBuffer.inMemoryAccumulate(bufferedEventSet);
        inMemoryBuffer.uploadToAmazonS3(any(), any(), any());

        S3SinkService s3SinkService = mock(S3SinkService.class);
        s3SinkService.createS3Client();

        InOrder inOrder = inOrder(inMemoryBuffer, s3SinkService);
        inOrder.verify(inMemoryBuffer).inMemoryAccumulate(bufferedEventSet);
        inOrder.verify(inMemoryBuffer).uploadToAmazonS3(any(), any(), any());
        inOrder.verify(s3SinkService).createS3Client();

        verifyNoMoreInteractions(inMemoryBuffer);
        verifyNoMoreInteractions(s3SinkService);
    }

    @Test
    void test_in_memoryAccumulate_with_s3Upload_success() throws InterruptedException {

        S3SinkService s3SinkService = mock(S3SinkService.class);
        S3Client s3Client = mock(S3Client.class);

        when(s3SinkConfig.getBucketOptions()).thenReturn(bucketOptions);
        when(s3SinkConfig.getBucketOptions().getObjectKeyOptions()).thenReturn(objectKeyOptions);
        when(s3SinkConfig.getBucketOptions().getBucketName()).thenReturn("dataprepper");
        when(s3SinkConfig.getBucketOptions().getObjectKeyOptions().getPathPrefix()).thenReturn("logdata/");
        when(objectKeyOptions.getNamePattern()).thenReturn("my-elb-%{yyyy-MM-dd'T'hh-mm-ss}");

        InMemoryBuffer inMemoryBuffer = new InMemoryBuffer(s3Client, s3SinkConfig);
        assertNotNull(inMemoryBuffer);
        assertTrue(inMemoryBuffer.inMemoryAccumulate(generateSet()));
        verify(s3SinkService, never()).createS3Client();
    }

    @Test
    void test_in_memoryAccumulate_with_s3Upload_fail() throws InterruptedException {
        InMemoryBuffer inMemoryBuffer = mock(InMemoryBuffer.class);
        assertNotNull(inMemoryBuffer);
        assertFalse(inMemoryBuffer.inMemoryAccumulate(generateSet()));
    }

    @Test
    void test_in_memoryAccumulate_cover_exception() throws InterruptedException {
        InMemoryBuffer inMemoryBuffer = new InMemoryBuffer(null, s3SinkConfig);
        assertNotNull(inMemoryBuffer);
        assertThrows(Throwable.class, () -> inMemoryBuffer.inMemoryAccumulate(generateSet()));
    }

    @Test
    void test_default_constructor_notNull() {
        InMemoryBuffer inMemoryBuffer = new InMemoryBuffer();
        assertNotNull(inMemoryBuffer);
    }

    private NavigableSet<String> generateSet() {
        DB eventDb = DBMaker.memoryDB().make();
        NavigableSet<String> bufferedEventSet = eventDb.treeSet("set").serializer(Serializer.STRING).createOrOpen();
        for (int i = 0; i < 50; i++) {
            final Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
            bufferedEventSet.add(event.toString());
        }
        return bufferedEventSet;
    }
}