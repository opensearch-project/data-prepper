/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.sink.accumulator.BufferTypeOptions;
import org.opensearch.dataprepper.plugins.sink.codec.Codec;
import org.opensearch.dataprepper.plugins.sink.codec.JsonCodec;
import org.opensearch.dataprepper.plugins.sink.configuration.BucketOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.ObjectKeyOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.ThresholdOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

class S3SinkWorkerIT {

    private static final Logger LOG = LoggerFactory.getLogger(S3SinkWorkerIT.class);
    private S3Client s3Client;
    private final BlockingQueue<Event> eventQueue;
    private S3SinkConfig s3SinkConfig;
    private S3SinkService s3SinkService;
    private Codec codec;
    private ObjectKeyOptions objectKeyOptions;
    private BucketOptions bucketOptions;
    private ThresholdOptions thresholdOptions;
    private String bucketName;

    private static final int EVENT_QUEUE_SIZE = 100000;

    public S3SinkWorkerIT() {
        eventQueue = new ArrayBlockingQueue<>(EVENT_QUEUE_SIZE);
    }

    @BeforeEach
    public void setUp() {

        s3Client = S3Client.builder().region(Region.of(System.getProperty("tests.s3source.region"))).build();
        bucketName = System.getProperty("tests.s3source.bucket");

        s3SinkConfig = mock(S3SinkConfig.class);
        codec = new JsonCodec();
        s3SinkService = new S3SinkService(s3SinkConfig);

        objectKeyOptions = mock(ObjectKeyOptions.class);
        when(objectKeyOptions.getNamePattern()).thenReturn("my-elb-%{yyyy-MM-dd'T'hh-mm-ss}");

        bucketOptions = mock(BucketOptions.class);
        when(bucketOptions.getBucketName()).thenReturn(bucketName);
        when(bucketOptions.getObjectKeyOptions()).thenReturn(objectKeyOptions);

        thresholdOptions = mock(ThresholdOptions.class);
        when(thresholdOptions.getEventCount()).thenReturn(10);
        when(thresholdOptions.getMaximumSize()).thenReturn(ByteCount.parse("2kb"));
        when(thresholdOptions.getEventCollect()).thenReturn(Duration.parse("PT3M"));

        when(s3SinkConfig.getThresholdOptions()).thenReturn(thresholdOptions);
        when(s3SinkConfig.getBucketOptions()).thenReturn(bucketOptions);
        when(s3SinkConfig.getBufferType()).thenReturn(BufferTypeOptions.INMEMORY);

    }

    @Test
    void copy_s3_object_correctly_into_s3_bucket() {

        int s3ObjectCountBeforeIngest = gets3ObjectCount();

        S3SinkWorker s3SinkWorker = createObjectUnderTest();
        Collection<Record<Event>> records = setEventQueue();
        s3SinkService.processRecords(records);
        s3SinkService.accumulateBufferEvents(s3SinkWorker);

        int s3ObjectCountAfterIngest = gets3ObjectCount();

        Assert.assertNotSame(s3ObjectCountBeforeIngest, s3ObjectCountAfterIngest);
        assertThat(s3ObjectCountAfterIngest, greaterThan(s3ObjectCountBeforeIngest));
    }
    
    private S3SinkWorker createObjectUnderTest() {
        return new S3SinkWorker(s3Client, s3SinkConfig, codec);
    }

    public BlockingQueue<Event> getEventQueue() {
        return eventQueue;
    }

    private int gets3ObjectCount() {
        int s3ObjectCount = 0;
        try {
            ListObjectsRequest listObjects = ListObjectsRequest.builder().bucket(bucketName).build();
            ListObjectsResponse res = s3Client.listObjects(listObjects);
            List<S3Object> objects = res.contents();
            s3ObjectCount = objects.size();
        } catch (S3Exception e) {
            LOG.error(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return s3ObjectCount;
    }

    public Collection<Record<Event>> setEventQueue() {
        final Collection<Record<Event>> jsonObjects = new LinkedList<Record<Event>>();
        for (int i = 0; i < 5; i++)
            jsonObjects.add(createRecord());
        for (final Record<Event> recordData : jsonObjects) {
            Event event = recordData.getData();
            getEventQueue().add(event);
        }
        return jsonObjects;
    }

    private static Record<Event> createRecord() {
        Map<String, Object> json = generateJson();
        final JacksonEvent event = JacksonLog.builder().withData(json).build();
        return new Record<>(event);
    }

    private static Map<String, Object> generateJson() {
        final Map<String, Object> jsonObject = new LinkedHashMap<>();
        for (int i = 0; i < 7; i++) {
            jsonObject.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        }
        jsonObject.put(UUID.randomUUID().toString(), Arrays.asList(UUID.randomUUID().toString(),
                UUID.randomUUID().toString(), UUID.randomUUID().toString()));

        return jsonObject;
    }
}