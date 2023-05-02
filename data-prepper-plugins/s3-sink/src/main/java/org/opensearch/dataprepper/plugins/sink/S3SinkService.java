/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
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
    private final S3SinkConfig s3SinkConfig;
    private final Lock reentrantLock;
    private final BufferFactory bufferFactory;
    private final Codec codec;
    private final S3Client s3Client;
    private Buffer currentBuffer;
    private final int numEvents;
    private final ByteCount byteCapacity;
    private final long duration;

    /**
     * @param s3SinkConfig s3 sink related configuration.
     * @param codec        parser
     */
    public S3SinkService(final S3SinkConfig s3SinkConfig, final BufferFactory bufferFactory, final Codec codec) {
        this.s3SinkConfig = s3SinkConfig;
        this.bufferFactory = bufferFactory;
        this.codec = codec;
        this.s3Client = createS3Client();
        reentrantLock = new ReentrantLock();

        numEvents = s3SinkConfig.getThresholdOptions().getEventCount();
        byteCapacity = s3SinkConfig.getThresholdOptions().getMaximumSize();
        duration = s3SinkConfig.getThresholdOptions().getEventCollectTimeOut().getSeconds();
    }

    /**
     * @param records received records and add into buffer.
     */
    void output(Collection<Record<Event>> records) {
        reentrantLock.lock();
        final String bucket = s3SinkConfig.getBucketOptions().getBucketName();
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
                    LOG.info("Snapshot info : Byte_capacity = {} Bytes," +
                            " Event_count = {} Records & Event_collection_duration = {} Sec",
                            byteCapacity.getBytes(), currentBuffer.getEventCount(), currentBuffer.getDuration());
                    boolean isUploadedToS3 = currentBuffer.flushToS3(s3Client, bucket, generateKey());
                    if (isUploadedToS3) {
                        LOG.info("Snapshot uploaded successfully");
                    } else {
                        LOG.info("Snapshot upload failed");
                    }
                    currentBuffer = bufferFactory.getBuffer();
                }
                currentBuffer.writeEvent(encodedBytes);
            }
        } catch (NullPointerException | IOException e) {
            LOG.error("Exception while write event into buffer :", e);
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
        if (numEvents > 0) {
            return currentBuffer.getEventCount() + 1 > numEvents ||
                    currentBuffer.getDuration() > duration ||
                    currentBuffer.getSize() > byteCapacity.getBytes();
        } else {
            return currentBuffer.getDuration() > duration ||
                    currentBuffer.getSize() > byteCapacity.getBytes();
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