/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.sink.accumulator.Buffer;
import org.opensearch.dataprepper.plugins.sink.accumulator.BufferFactory;
import org.opensearch.dataprepper.plugins.sink.accumulator.ObjectKey;
import org.opensearch.dataprepper.plugins.sink.codec.Codec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Class responsible for create {@link S3Client} object, check thresholds,
 * get new buffer and write records into buffer.
 */
public class S3SinkService {

    private static final Logger LOG = LoggerFactory.getLogger(S3SinkService.class);
    public static final String SNAPSHOT_SUCCESS = "snapshotSuccess";
    public static final String SNAPSHOT_FAILED = "snapshotFailed";
    public static final String NUMBER_OF_RECORDS_FLUSHED_TO_S3_SUCCESS = "numberOfRecordsFlushedToS3Success";
    public static final String NUMBER_OF_RECORDS_FLUSHED_TO_S3_FAILED = "numberOfRecordsFlushedToS3Failed";
    static final String S3_OBJECTS_SIZE = "s3ObjectSizeRecords";
    private final S3SinkConfig s3SinkConfig;
    private final Lock reentrantLock;
    private final BufferFactory bufferFactory;
    private final Codec codec;
    private final PluginMetrics pluginMetrics;
    private final S3Client s3Client;
    private Buffer currentBuffer;
    private final int maxEvents;
    private final ByteCount maxBytes;
    private final long maxCollectionDuration;
    private final Counter snapshotSuccessCounter;
    private final Counter snapshotFailedCounter;
    private final Counter numberOfRecordsSuccessCounter;
    private final Counter numberOfRecordsFailedCounter;
    private final DistributionSummary s3ObjectSizeSummary;

    /**
     * @param s3SinkConfig  s3 sink related configuration.
     * @param codec         parser
     * @param pluginMetrics metrics
     */
    public S3SinkService(final S3SinkConfig s3SinkConfig, final BufferFactory bufferFactory, final Codec codec, PluginMetrics pluginMetrics) {
        this.s3SinkConfig = s3SinkConfig;
        this.bufferFactory = bufferFactory;
        this.codec = codec;
        this.pluginMetrics = pluginMetrics;
        this.s3Client = createS3Client();
        reentrantLock = new ReentrantLock();

        maxEvents = s3SinkConfig.getThresholdOptions().getEventCount();
        maxBytes = s3SinkConfig.getThresholdOptions().getMaximumSize();
        maxCollectionDuration = s3SinkConfig.getThresholdOptions().getEventCollectTimeOut().getSeconds();

        snapshotSuccessCounter = pluginMetrics.counter(SNAPSHOT_SUCCESS);
        snapshotFailedCounter = pluginMetrics.counter(SNAPSHOT_FAILED);
        numberOfRecordsSuccessCounter = pluginMetrics.counter(NUMBER_OF_RECORDS_FLUSHED_TO_S3_SUCCESS);
        numberOfRecordsFailedCounter = pluginMetrics.counter(NUMBER_OF_RECORDS_FLUSHED_TO_S3_FAILED);
        s3ObjectSizeSummary = pluginMetrics.summary(S3_OBJECTS_SIZE);
    }

    /**
     * @param records received records and add into buffer.
     */
    void output(Collection<Record<Event>> records) {
        reentrantLock.lock();
        final String bucket = s3SinkConfig.getBucketOptions().getBucketName();
        final int maxRetries = s3SinkConfig.getMaxUploadRetries();
        if (currentBuffer == null) {
            currentBuffer = bufferFactory.getBuffer();
        }
        try {
            for (Record<Event> record : records) {

                final Event event = record.getData();
                final String encodedEvent;
                encodedEvent = codec.parse(event);
                final byte[] encodedBytes = encodedEvent.getBytes();
                if (willExceedThreshold()) {
                    s3ObjectSizeSummary.record(currentBuffer.getEventCount());
                    LOG.info("Snapshot info : Byte_capacity = {} Bytes," +
                            " Event_count = {} Records & Event_collection_duration = {} Sec",
                            maxBytes.getBytes(), currentBuffer.getEventCount(), currentBuffer.getDuration());
                    boolean isUploadedToS3 = currentBuffer.flushToS3(s3Client, bucket, generateKey(), maxRetries);
                    if (isUploadedToS3) {
                        LOG.info("Snapshot uploaded successfully");
                        numberOfRecordsSuccessCounter.increment(currentBuffer.getEventCount());
                        snapshotSuccessCounter.increment();
                    } else {
                        LOG.info("Snapshot upload failed");
                        numberOfRecordsFailedCounter.increment(currentBuffer.getEventCount());
                        snapshotFailedCounter.increment();
                    }
                    currentBuffer = bufferFactory.getBuffer();
                }
                currentBuffer.writeEvent(encodedBytes);
            }
        } catch (NullPointerException | IOException | InterruptedException e) {
            LOG.error("Exception while write event into buffer :", e);
            Thread.currentThread().interrupt();
        }
        reentrantLock.unlock();
    }

    /**
     * Generate the s3 object path prefix & object file name.
     * @return object key path.
     */
    protected String generateKey() {
        final String pathPrefix = ObjectKey.buildingPathPrefix(s3SinkConfig);
        final String namePattern = ObjectKey.objectFileName(s3SinkConfig);
        return (!pathPrefix.isEmpty()) ? pathPrefix + namePattern : namePattern;
    }

    /**
     * Check threshold limits.
     * @return boolean value whether the threshold are met.
     */
    private boolean willExceedThreshold() {
        if (maxEvents > 0) {
            return currentBuffer.getEventCount() + 1 > maxEvents ||
                    currentBuffer.getDuration() > maxCollectionDuration ||
                    currentBuffer.getSize() > maxBytes.getBytes();
        } else {
            return currentBuffer.getDuration() > maxCollectionDuration ||
                    currentBuffer.getSize() > maxBytes.getBytes();
        }
    }

    /**
     * create s3 client instance.
     * @return {@link S3Client}
     */
    public S3Client createS3Client() {
        LOG.info("Creating S3 client");
        return S3Client.builder().region(s3SinkConfig.getAwsAuthenticationOptions().getAwsRegion())
                .credentialsProvider(s3SinkConfig.getAwsAuthenticationOptions().authenticateAwsConfiguration())
                .overrideConfiguration(ClientOverrideConfiguration.builder().retryPolicy(RetryPolicy.builder()
                        .numRetries(s3SinkConfig.getMaxConnectionRetries()).build()).build()).build();
    }
}