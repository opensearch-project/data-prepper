/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink;

import java.io.IOException;
import java.util.NavigableSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.time.StopWatch;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.sink.accumulator.BufferType;
import org.opensearch.dataprepper.plugins.sink.accumulator.BufferTypeOptions;
import org.opensearch.dataprepper.plugins.sink.accumulator.InMemoryBuffer;
import org.opensearch.dataprepper.plugins.sink.accumulator.LocalFileBuffer;
import org.opensearch.dataprepper.plugins.sink.codec.Codec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.services.s3.S3Client;

/**
 * Class responsible for threshold check and instantiation of {@link BufferType}
 */
public class S3SinkWorker {

    private static final Logger LOG = LoggerFactory.getLogger(S3SinkWorker.class);
    private final S3Client s3Client;
    private final S3SinkConfig s3SinkConfig;
    private final Codec codec;
    private final int numEvents;
    private final ByteCount byteCapacity;
    private final long duration;

    /**
     * @param s3Client
     * @param s3SinkConfig
     * @param codec
     */
    public S3SinkWorker(final S3Client s3Client, final S3SinkConfig s3SinkConfig, final Codec codec) {
        this.s3Client = s3Client;
        this.s3SinkConfig = s3SinkConfig;
        this.codec = codec;
        numEvents = s3SinkConfig.getThresholdOptions().getEventCount();
        byteCapacity = s3SinkConfig.getThresholdOptions().getMaximumSize();
        duration = s3SinkConfig.getThresholdOptions().getEventCollect().getSeconds();
    }

    /**
     * Accumulates data from buffer and store in local-file/in-memory.
     *
     * @param eventQueue
     */
    public void bufferAccumulator(BlockingQueue<Event> eventQueue) {
        boolean isFileUploadedToS3 = Boolean.FALSE;
        DB eventDb = null;
        NavigableSet<String> bufferedEventSet = null;
        int byteCount = 0;
        int eventCount = 0;
        long eventCollectionDuration = 0;
        try {
            StopWatch watch = new StopWatch();
            watch.start();
            eventDb = DBMaker.memoryDB().make();
            bufferedEventSet = eventDb.treeSet("set").serializer(Serializer.STRING).createOrOpen();
            int data = 0;
            while (thresholdsCheck(data, watch, byteCount)) {
                if (!eventQueue.isEmpty()) {
                    Event event = eventQueue.take();
                    String jsonSerEvent = codec.parse(event);
                    byteCount += jsonSerEvent.getBytes().length;
                    bufferedEventSet.add(codec.parse(event).concat(System.lineSeparator()));
                    eventCount++;
                    data++;
                    eventCollectionDuration = watch.getTime(TimeUnit.SECONDS);
                }
            }
            eventDb.commit();
            LOG.info(
                    "Snapshot info : Byte_capacity = {} Bytes, Event_count = {} Records & Event_collection_duration = {} Sec",
                    byteCount, eventCount, eventCollectionDuration);

            if (s3SinkConfig.getBufferType().equals(BufferTypeOptions.LOCALFILE)) {
                isFileUploadedToS3 = new LocalFileBuffer(s3Client, s3SinkConfig).localFileAccumulate(bufferedEventSet);
            } else {
                isFileUploadedToS3 = new InMemoryBuffer(s3Client, s3SinkConfig).inMemoryAccumulate(bufferedEventSet);
            }

            if (isFileUploadedToS3) {
                LOG.info("Snapshot uploaded successfully");
            } else {
                LOG.info("Snapshot upload failed");
            }

        } catch (InterruptedException e) {
            LOG.error("Exception while storing recoreds to buffer, or upload object to Amazon s3 bucket", e);
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            LOG.error("Exception while json serialization", e);
        } finally {
            if (eventDb != null && !eventDb.isClosed()) {
                eventDb.close();
            }
        }
    }

    /**
     * Toggle the flag based on the threshold limits. If flag is false write to in-memory/local-file.
     * 
     * @param eventCount
     * @param watch
     * @param byteCount
     * @return
     */
    private boolean thresholdsCheck(int eventCount, StopWatch watch, int byteCount) {
        if (eventCount > 0) {
            return eventCount < numEvents && watch.getTime(TimeUnit.SECONDS) < duration
                    && byteCount < byteCapacity.getBytes();
        } else {
            return watch.getTime(TimeUnit.SECONDS) < duration && byteCount < byteCapacity.getBytes();
        }
    }
}