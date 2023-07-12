/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.sink.accumulator.Buffer;
import org.opensearch.dataprepper.plugins.sink.accumulator.BufferFactory;
import org.opensearch.dataprepper.plugins.sink.accumulator.ObjectKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.LinkedList;
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
    private final long maxCollectionDuration;
    private final String bucket;
    private final int maxRetries;
    private final Counter objectsSucceededCounter;
    private final Counter objectsFailedCounter;
    private final Counter numberOfRecordsSuccessCounter;
    private final Counter numberOfRecordsFailedCounter;
    private final DistributionSummary s3ObjectSizeSummary;
    private final String tagsTargetKey;

    /**
     * @param s3SinkConfig  s3 sink related configuration.
     * @param bufferFactory factory of buffer.
     * @param codec         parser.
     * @param s3Client
     * @param pluginMetrics metrics.
     */
    public S3SinkService(final S3SinkConfig s3SinkConfig, final BufferFactory bufferFactory,
                         final OutputCodec codec, final S3Client s3Client, final String tagsTargetKey, final PluginMetrics pluginMetrics) {
        this.s3SinkConfig = s3SinkConfig;
        this.bufferFactory = bufferFactory;
        this.codec = codec;
        this.s3Client = s3Client;
        this.tagsTargetKey = tagsTargetKey;
        reentrantLock = new ReentrantLock();

        bufferedEventHandles = new LinkedList<>();

        maxEvents = s3SinkConfig.getThresholdOptions().getEventCount();
        maxBytes = s3SinkConfig.getThresholdOptions().getMaximumSize();
        maxCollectionDuration = s3SinkConfig.getThresholdOptions().getEventCollectTimeOut().getSeconds();

        bucket = s3SinkConfig.getBucketName();
        maxRetries = s3SinkConfig.getMaxUploadRetries();

        objectsSucceededCounter = pluginMetrics.counter(OBJECTS_SUCCEEDED);
        objectsFailedCounter = pluginMetrics.counter(OBJECTS_FAILED);
        numberOfRecordsSuccessCounter = pluginMetrics.counter(NUMBER_OF_RECORDS_FLUSHED_TO_S3_SUCCESS);
        numberOfRecordsFailedCounter = pluginMetrics.counter(NUMBER_OF_RECORDS_FLUSHED_TO_S3_FAILED);
        s3ObjectSizeSummary = pluginMetrics.summary(S3_OBJECTS_SIZE);
    }

    /**
     * @param records received records and add into buffer.
     */
    void output(Collection<Record<Event>> records) {
        reentrantLock.lock();
        if (currentBuffer == null) {
            currentBuffer = bufferFactory.getBuffer();
        }
        try {
            OutputStream outputStream = currentBuffer.getOutputStream();

            for (Record<Event> record : records) {

                if(currentBuffer.getEventCount() == 0) {
                    codec.start(outputStream, record.getData());
                }

                final Event event = record.getData();
                codec.writeEvent(event, outputStream, tagsTargetKey);
                int count = currentBuffer.getEventCount() +1;
                currentBuffer.setEventCount(count);

                if(event.getEventHandle() != null) {
                    bufferedEventHandles.add(event.getEventHandle());
                }
                if (ThresholdCheck.checkThresholdExceed(currentBuffer, maxEvents, maxBytes, maxCollectionDuration)) {
                    codec.complete(outputStream);
                    final String s3Key = generateKey(codec);
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
                    currentBuffer = bufferFactory.getBuffer();
                }
            }
        } catch (IOException | InterruptedException e) {
            LOG.error("Exception while write event into buffer :", e);
        }
        reentrantLock.unlock();
    }

    private void releaseEventHandles(final boolean result) {
        for (EventHandle eventHandle : bufferedEventHandles) {
            eventHandle.release(result);
        }

        bufferedEventHandles.clear();
    }

    /**
     * perform retry in-case any issue occurred, based on max_upload_retries configuration.
     *
     * @param currentBuffer current buffer.
     * @param s3Key
     * @return boolean based on object upload status.
     * @throws InterruptedException interruption during sleep.
     */
    protected boolean retryFlushToS3(final Buffer currentBuffer, final String s3Key) throws InterruptedException {
        boolean isUploadedToS3 = Boolean.FALSE;
        int retryCount = maxRetries;
        do {
            try {
                currentBuffer.flushToS3(s3Client, bucket, s3Key);
                isUploadedToS3 = Boolean.TRUE;
            } catch (AwsServiceException | SdkClientException e) {
                LOG.error("Exception occurred while uploading records to s3 bucket. Retry countdown  : {} | exception:",
                        retryCount, e);
                LOG.info("Error Massage {}", e.getMessage());
                --retryCount;
                if (retryCount == 0) {
                    return isUploadedToS3;
                }
                Thread.sleep(5000);
            }
        } while (!isUploadedToS3);
        return isUploadedToS3;
    }

    /**
     * Generate the s3 object path prefix and object file name.
     * @return object key path.
     */
    protected String generateKey(OutputCodec codec) {
        final String pathPrefix = ObjectKey.buildingPathPrefix(s3SinkConfig);
        final String namePattern = ObjectKey.objectFileName(s3SinkConfig, codec.getExtension());
        return (!pathPrefix.isEmpty()) ? pathPrefix + namePattern : namePattern;
    }
}
