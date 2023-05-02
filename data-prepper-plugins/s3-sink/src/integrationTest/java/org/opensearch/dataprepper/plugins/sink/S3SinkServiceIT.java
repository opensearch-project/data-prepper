/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.Mockito.when;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.sink.accumulator.BufferFactory;
import org.opensearch.dataprepper.plugins.sink.accumulator.InMemoryBufferFactory;
import org.opensearch.dataprepper.plugins.sink.codec.JsonCodec;
import org.opensearch.dataprepper.plugins.sink.configuration.AwsAuthenticationOptions;
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
@ExtendWith(MockitoExtension.class)
class S3SinkServiceIT {

    private static final Logger LOG = LoggerFactory.getLogger(S3SinkServiceIT.class);
    private S3Client s3Client;
    private String bucketName;
    private JsonCodec codec;
    private BufferFactory bufferFactory;
    @Mock
    private S3SinkConfig s3SinkConfig;
    @Mock
    private ThresholdOptions thresholdOptions;
    @Mock
    private AwsAuthenticationOptions awsAuthenticationOptions;
    @Mock
    private ObjectKeyOptions objectKeyOptions;
    @Mock
    private BucketOptions bucketOptions;

    @BeforeEach
    public void setUp() {
        String s3region = System.getProperty("tests.s3ink.region");

        s3Client = S3Client.builder().region(Region.of(s3region)).build();
        bucketName = System.getProperty("tests.s3sink.bucket");

        codec = new JsonCodec();
        bufferFactory = new InMemoryBufferFactory();

        when(objectKeyOptions.getNamePattern()).thenReturn("my-elb-%{yyyy-MM-dd'T'hh-mm-ss}");
        when(bucketOptions.getBucketName()).thenReturn(bucketName);
        when(bucketOptions.getObjectKeyOptions()).thenReturn(objectKeyOptions);
        when(thresholdOptions.getEventCount()).thenReturn(10);
        when(thresholdOptions.getMaximumSize()).thenReturn(ByteCount.parse("2mb"));
        when(thresholdOptions.getEventCollectTimeOut()).thenReturn(Duration.parse("PT3M"));
        when(s3SinkConfig.getThresholdOptions()).thenReturn(thresholdOptions);
        when(s3SinkConfig.getBucketOptions()).thenReturn(bucketOptions);
        when(s3SinkConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.of(s3region));

    }

    @Test
    void verify_flushed_records_into_s3_bucket() {

        int s3ObjectCountBeforeIngest = gets3ObjectCount();
        S3SinkService s3SinkService = createObjectUnderTest();
        s3SinkService.output(setEventQueue());
        int s3ObjectCountAfterIngest = gets3ObjectCount();
        assertThat(s3ObjectCountAfterIngest, greaterThan(s3ObjectCountBeforeIngest));
        assertThat(s3ObjectCountAfterIngest, equalTo(s3ObjectCountBeforeIngest + 1));
    }

    private S3SinkService createObjectUnderTest() {
        return new S3SinkService(s3SinkConfig, bufferFactory, codec);
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

    private Collection<Record<Event>> setEventQueue() {
        final Collection<Record<Event>> jsonObjects = new LinkedList<>();
        for (int i = 0; i < 15; i++)
            jsonObjects.add(createRecord());
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