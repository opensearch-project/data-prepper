/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.sink.s3.accumulator.Buffer;
import org.opensearch.dataprepper.plugins.sink.s3.accumulator.BufferFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Class responsible for create {@link S3Client} object, check thresholds,
 * get new buffer and write records into buffer.
 */
public class S3SinkService {

    private static final Logger LOG = LoggerFactory.getLogger(S3SinkService.class);
    public static final String OBJECTS_SUCCEEDED = "s3SinkObjectsSucceeded";
    public static final String OBJECTS_FAILED = "s3SinkObjectsFailed";
    public static final String NUMBER_OF_RECORDS_FLUSHED_TO_S3_SUCCESS = "s3SinkObjectsEventsSucceeded";
    public static final String NUMBER_OF_RECORDS_FLUSHED_TO_S3_FAILED = "s3SinkObjectsEventsFailed";
    static final String S3_OBJECTS_SIZE = "s3SinkObjectSizeBytes";
    private final S3SinkConfig s3SinkConfig;
    private final Lock reentrantLock;
    private final BufferFactory bufferFactory;
    private final Collection<EventHandle> bufferedEventHandles;
    private final OutputCodec codec;
    private final S3Client s3Client;
    private Buffer currentBuffer;
    private final int maxEvents;
    private final ByteCount maxBytes;
    private final Duration maxCollectionDuration;
    private final String bucket;
    private final int maxRetries;
    private final Counter objectsSucceededCounter;
    private final Counter objectsFailedCounter;
    private final Counter numberOfRecordsSuccessCounter;
    private final Counter numberOfRecordsFailedCounter;
    private final DistributionSummary s3ObjectSizeSummary;
    private final OutputCodecContext codecContext;
    private final KeyGenerator keyGenerator;
    private final Duration retrySleepTime;

    /**
     * @param s3SinkConfig  s3 sink related configuration.
     * @param bufferFactory factory of buffer.
     * @param codec         parser.
     * @param s3Client
     * @param pluginMetrics metrics.
     */
    public S3SinkService(final S3SinkConfig s3SinkConfig, final BufferFactory bufferFactory,
                         final OutputCodec codec, final OutputCodecContext codecContext, final S3Client s3Client, final KeyGenerator keyGenerator,
                         final Duration retrySleepTime, final PluginMetrics pluginMetrics) {
        this.s3SinkConfig = s3SinkConfig;
        this.bufferFactory = bufferFactory;
        this.codec = codec;
        this.s3Client = s3Client;
        this.codecContext = codecContext;
        this.keyGenerator = keyGenerator;
        this.retrySleepTime = retrySleepTime;
        reentrantLock = new ReentrantLock();

        bufferedEventHandles = new LinkedList<>();

        maxEvents = s3SinkConfig.getThresholdOptions().getEventCount();
        maxBytes = s3SinkConfig.getThresholdOptions().getMaximumSize();
        maxCollectionDuration = s3SinkConfig.getThresholdOptions().getEventCollectTimeOut();

        bucket = s3SinkConfig.getBucketName();
        maxRetries = s3SinkConfig.getMaxUploadRetries();

        objectsSucceededCounter = pluginMetrics.counter(OBJECTS_SUCCEEDED);
        objectsFailedCounter = pluginMetrics.counter(OBJECTS_FAILED);
        numberOfRecordsSuccessCounter = pluginMetrics.counter(NUMBER_OF_RECORDS_FLUSHED_TO_S3_SUCCESS);
        numberOfRecordsFailedCounter = pluginMetrics.counter(NUMBER_OF_RECORDS_FLUSHED_TO_S3_FAILED);
        s3ObjectSizeSummary = pluginMetrics.summary(S3_OBJECTS_SIZE);

        currentBuffer = bufferFactory.getBuffer(s3Client, () -> bucket, keyGenerator::generateKey);
    }

    /**
     * @param records received records and add into buffer.
     */
    void output(Collection<Record<Event>> records) {
        // Don't acquire the lock if there's no work to be done
        if (records.isEmpty() && currentBuffer.getEventCount() == 0) {
            return;
        }

        List<Event> failedEvents = new ArrayList<>();
        Exception sampleException = null;
        reentrantLock.lock();
        try {
            for (Record<Event> record : records) {

                final Event event = record.getData();
                try {
                    if (currentBuffer.getEventCount() == 0) {
                        codec.start(currentBuffer.getOutputStream(), event, codecContext);
                    }

                    codec.writeEvent(event, currentBuffer.getOutputStream());
                    int count = currentBuffer.getEventCount() + 1;
                    currentBuffer.setEventCount(count);

                    if (event.getEventHandle() != null) {
                        bufferedEventHandles.add(event.getEventHandle());
                    }
                } catch (Exception ex) {
                    if(sampleException == null) {
                        sampleException = ex;
                    }

                    failedEvents.add(event);
                }

                flushToS3IfNeeded();
            }
            flushToS3IfNeeded();
        } finally {
            reentrantLock.unlock();
        }

        if(!failedEvents.isEmpty()) {
            failedEvents
                    .stream()
                    .map(Event::getEventHandle)
                    .filter(Objects::nonNull)
                    .forEach(eventHandle -> eventHandle.release(false));
            LOG.error("Unable to add {} events to buffer. Dropping these events. Sample exception provided.", failedEvents.size(), sampleException);
        }
    }

    private void releaseEventHandles(final boolean result) {
        for (EventHandle eventHandle : bufferedEventHandles) {
            eventHandle.release(result);
        }

        bufferedEventHandles.clear();
    }

    private void flushToS3IfNeeded() {
        LOG.trace("Flush to S3 check: currentBuffer.size={}, currentBuffer.events={}, currentBuffer.duration={}",
                currentBuffer.getSize(), currentBuffer.getEventCount(), currentBuffer.getDuration());
        if (ThresholdCheck.checkThresholdExceed(currentBuffer, maxEvents, maxBytes, maxCollectionDuration)) {
            try {
                codec.complete(currentBuffer.getOutputStream());
                String s3Key = currentBuffer.getKey();
                LOG.info("Writing {} to S3 with {} events and size of {} bytes.",
                        s3Key, currentBuffer.getEventCount(), currentBuffer.getSize());
                final boolean isFlushToS3 = retryFlushToS3(currentBuffer, s3Key);
                if (isFlushToS3) {
                    LOG.info("Successfully saved {} to S3.", s3Key);
                    numberOfRecordsSuccessCounter.increment(currentBuffer.getEventCount());
                    objectsSucceededCounter.increment();
                    s3ObjectSizeSummary.record(currentBuffer.getSize());
                    releaseEventHandles(true);
                } else {
                    LOG.error("Failed to save {} to S3.", s3Key);
                    numberOfRecordsFailedCounter.increment(currentBuffer.getEventCount());
                    objectsFailedCounter.increment();
                    releaseEventHandles(false);
                }
                currentBuffer = bufferFactory.getBuffer(s3Client, () -> bucket, keyGenerator::generateKey);
            } catch (final IOException e) {
                LOG.error("Exception while completing codec", e);
            }
        }
    }

    /**
     * perform retry in-case any issue occurred, based on max_upload_retries configuration.
     *
     * @param currentBuffer current buffer.
     * @param s3Key
     * @return boolean based on object upload status.
     */
    protected boolean retryFlushToS3(final Buffer currentBuffer, final String s3Key) {
        boolean isUploadedToS3 = Boolean.FALSE;
        int retryCount = maxRetries;
        do {
            try {
                currentBuffer.flushToS3();
                isUploadedToS3 = Boolean.TRUE;
            } catch (AwsServiceException | SdkClientException e) {
                LOG.error("Exception occurred while uploading records to s3 bucket. Retry countdown  : {} | exception:",
                        retryCount, e);
                LOG.info("Error Message {}", e.getMessage());
                --retryCount;
                if (retryCount == 0) {
                    return isUploadedToS3;
                }

                try {
                    Thread.sleep(retrySleepTime.toMillis());
                } catch (final InterruptedException ex) {
                    LOG.warn("Interrupted while backing off before retrying S3 upload", ex);
                }
            }
        } while (!isUploadedToS3);
        return isUploadedToS3;
    }
}
