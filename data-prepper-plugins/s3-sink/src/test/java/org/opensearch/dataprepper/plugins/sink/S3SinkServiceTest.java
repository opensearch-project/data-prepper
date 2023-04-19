/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.sink.accumulator.BufferTypeOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.BucketOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.ObjectKeyOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.ThresholdOptions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

class S3SinkServiceTest {

    private S3SinkConfig s3SinkConfig;
    private ThresholdOptions thresholdOptions;
    private AwsAuthenticationOptions awsAuthenticationOptions;
    private AwsCredentialsProvider awsCredentialsProvider;
    private BucketOptions bucketOptions;
    private ObjectKeyOptions objectKeyOptions;

    @BeforeEach
    void setUp() throws Exception {

        s3SinkConfig = mock(S3SinkConfig.class);
        thresholdOptions = mock(ThresholdOptions.class);
        bucketOptions = mock(BucketOptions.class);
        objectKeyOptions = mock(ObjectKeyOptions.class);
        awsAuthenticationOptions = mock(AwsAuthenticationOptions.class);

        when(objectKeyOptions.getNamePattern()).thenReturn("my-elb-%{yyyy-MM-dd'T'hh-mm-ss}");
        when(s3SinkConfig.getThresholdOptions()).thenReturn(thresholdOptions);
        when(s3SinkConfig.getThresholdOptions().getEventCount()).thenReturn(100);
        when(s3SinkConfig.getThresholdOptions().getMaximumSize()).thenReturn(ByteCount.parse("1kb"));
        when(s3SinkConfig.getThresholdOptions().getEventCollect()).thenReturn(Duration.ofSeconds(5));
        when(s3SinkConfig.getBufferType()).thenReturn(BufferTypeOptions.LOCALFILE);

        when(s3SinkConfig.getBucketOptions()).thenReturn(bucketOptions);
        when(s3SinkConfig.getBucketOptions().getObjectKeyOptions()).thenReturn(objectKeyOptions);
        when(s3SinkConfig.getBucketOptions().getBucketName()).thenReturn("dataprepper");
        when(s3SinkConfig.getBucketOptions().getObjectKeyOptions().getPathPrefix()).thenReturn("logdata/");

        when(s3SinkConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.of("us-east-1"));
        when(awsAuthenticationOptions.authenticateAwsConfiguration()).thenReturn(awsCredentialsProvider);
    }

    @Test
    void test_call_createS3Client() {
        S3SinkService s3SinkService = new S3SinkService(s3SinkConfig);
        S3Client s3Client = s3SinkService.createS3Client();
        assertNotNull(s3Client);
    }

    @Test
    void test_call_processRecords() {
        S3SinkService s3SinkService = new S3SinkService(s3SinkConfig);
        assertNotNull(s3SinkService);
        s3SinkService.processRecords(generateRandomStringEventRecord());
        s3SinkService = mock(S3SinkService.class);
        Collection<Record<Event>> records = generateRandomStringEventRecord();
        s3SinkService.processRecords(records);
        verify(s3SinkService, atLeastOnce()).processRecords(records);
    }

    @Test
    void test_call_bufferAccumulator() {
        Collection<Record<Event>> records = generateRandomStringEventRecord();
        S3SinkService service = new S3SinkService(s3SinkConfig);
        S3SinkWorker worker = mock(S3SinkWorker.class);
        service.processRecords(records);
        service.accumulateBufferEvents(worker);
        verify(worker, only()).bufferAccumulator(any(BlockingQueue.class));
    }

    private Collection<Record<Event>> generateRandomStringEventRecord() {
        Collection<Record<Event>> records = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            final Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
            records.add(new Record<>(event));
        }
        return records;
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