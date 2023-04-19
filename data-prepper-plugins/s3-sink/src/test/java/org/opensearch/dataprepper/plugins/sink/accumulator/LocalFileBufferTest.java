/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.accumulator;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import java.time.Duration;
import java.util.NavigableSet;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.sink.S3SinkConfig;
import org.opensearch.dataprepper.plugins.sink.S3SinkService;
import org.opensearch.dataprepper.plugins.sink.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.BucketOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.ObjectKeyOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.ThresholdOptions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
class LocalFileBufferTest {

    private S3SinkConfig s3SinkConfig;
    private ThresholdOptions thresholdOptions;
    private BucketOptions bucketOptions;
    private AwsAuthenticationOptions awsAuthenticationOptions;
    private AwsCredentialsProvider awsCredentialsProvider;
    private ObjectKeyOptions objectKeyOptions;

    @BeforeEach
    void setUp() throws Exception {

        s3SinkConfig = mock(S3SinkConfig.class);
        thresholdOptions = mock(ThresholdOptions.class);
        bucketOptions = mock(BucketOptions.class);
        awsAuthenticationOptions = mock(AwsAuthenticationOptions.class);
        objectKeyOptions = mock(ObjectKeyOptions.class);

        when(s3SinkConfig.getThresholdOptions()).thenReturn(thresholdOptions);
        when(s3SinkConfig.getThresholdOptions().getEventCount()).thenReturn(100);
        when(s3SinkConfig.getThresholdOptions().getMaximumSize()).thenReturn(ByteCount.parse("1kb"));
        when(s3SinkConfig.getThresholdOptions().getEventCollect()).thenReturn(Duration.ofSeconds(5));

        when(objectKeyOptions.getNamePattern()).thenReturn("my-elb-%{yyyy-MM-dd'T'hh-mm-ss}");

        when(s3SinkConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.of("us-east-1"));
        when(awsAuthenticationOptions.authenticateAwsConfiguration()).thenReturn(awsCredentialsProvider);

        when(s3SinkConfig.getBucketOptions()).thenReturn(bucketOptions);
        when(s3SinkConfig.getBucketOptions().getObjectKeyOptions()).thenReturn(objectKeyOptions);
        when(s3SinkConfig.getBucketOptions().getBucketName()).thenReturn("dataprepper");
        when(s3SinkConfig.getBucketOptions().getObjectKeyOptions().getPathPrefix()).thenReturn("logdata/");
    }

    @Test
    void verify_interactions(){
        S3SinkService s3SinkService = mock(S3SinkService.class);
        s3SinkService.createS3Client();
        verify(s3SinkService).createS3Client();
        verifyNoMoreInteractions(s3SinkService);
    }

    @Test
    void test_local_file_accumulate_with_s3Upload_success() throws InterruptedException {
        S3SinkService s3SinkService = mock(S3SinkService.class);
        S3Client s3Client = mock(S3Client.class);

        LocalFileBuffer localFileBuffer = new LocalFileBuffer(s3Client, s3SinkConfig);
        assertNotNull(localFileBuffer);
        assertTrue(localFileBuffer.localFileAccumulate(generateSet()));
        verify(s3SinkService, never()).createS3Client();
    }

    @Test
    void test_local_file_accumulate_with_s3Upload_fail() throws InterruptedException {
        LocalFileBuffer localFileBuffer = mock(LocalFileBuffer.class);
        assertNotNull(localFileBuffer);
        assertFalse(localFileBuffer.localFileAccumulate (generateSet()));
    }

    @Test
    void test_local_file_accumulate_cover_exception() throws InterruptedException {
        LocalFileBuffer localFileBuffer = new LocalFileBuffer(null, s3SinkConfig);
        assertNotNull(localFileBuffer);
        assertThrows(Throwable.class, () -> localFileBuffer.localFileAccumulate(generateSet()));
    }

    @Test
    void test_default_constructor_notNull() {
        LocalFileBuffer localFileBuffer = new LocalFileBuffer();
        assertNotNull(localFileBuffer);
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