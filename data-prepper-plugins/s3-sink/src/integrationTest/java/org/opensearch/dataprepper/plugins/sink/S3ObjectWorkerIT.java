/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.sink.configuration.ObjectOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.ThresholdOptions;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

class S3ObjectWorkerIT {

    private static final String DEFAULT_CODEC_FILE_EXTENSION = "json";
    private static final int EVENT_QUEUE_SIZE = 100000;
    private static final long DURATION = 20L;
    private static final String BYTE_CAPACITY = "50mb";
    private S3Client s3Client;
    private final BlockingQueue<Event> eventQueue;
    private String bucket;
    private S3SinkWorker s3SinkWorker;
    private S3SinkConfig s3SinkConfig;
    private S3SinkService s3SinkService;
    private ThresholdOptions thresholdOptions;
    private ObjectOptions objectOptions;

    public S3ObjectWorkerIT() {
        eventQueue = new ArrayBlockingQueue<>(EVENT_QUEUE_SIZE);
    }

    @BeforeEach
    public void setUp() {
        s3Client = S3Client.builder().region(Region.of(System.getProperty("tests.s3source.region"))).build();
        bucket = System.getProperty("tests.s3source.bucket");

        s3SinkConfig = mock(S3SinkConfig.class);
        s3SinkService = new S3SinkService(s3SinkConfig);
        thresholdOptions = mock(ThresholdOptions.class);
        objectOptions = mock(ObjectOptions.class);
        when(s3SinkConfig.getObjectOptions()).thenReturn(objectOptions);
        when(objectOptions.getFilePattern()).thenReturn("logs-${yyyy-MM-dd}");
        when(s3SinkConfig.getTemporaryStorage()).thenReturn("local_file");
        when(s3SinkConfig.getBucketName()).thenReturn(bucket);

        when(s3SinkConfig.getThresholdOptions()).thenReturn(thresholdOptions);
        when(thresholdOptions.getEventCount()).thenReturn(10);
        when(thresholdOptions.getMaximumSize()).thenReturn(ByteCount.parse(BYTE_CAPACITY));
        when(thresholdOptions.getEventCollectionDuration()).thenReturn(Duration.ofSeconds(DURATION));
    }

    @Test
    void copy_s3_object_correctly_into_s3_bucket() {
        String codecFileExtension = DEFAULT_CODEC_FILE_EXTENSION;
        RecordsGenerator recordsGenerator = new JsonRecordsGenerator();
        s3SinkWorker = new S3SinkWorker(s3Client, s3SinkConfig, recordsGenerator.getCodec(), codecFileExtension);
        Collection<Record<Event>> records = setEventQueue();
        s3SinkService.processRecods(records);
        s3SinkService.accumulateBufferEvents(s3SinkWorker);
        assertNotNull(recordsGenerator);
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

    public BlockingQueue<Event> getEventQueue() {
        return eventQueue;
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
}