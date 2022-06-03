/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.plugins.source.codec.Codec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.IOException;
import java.time.Duration;

/**
 * Class responsible for taking an {@link S3ObjectReference} and creating all the necessary {@link Event}
 * objects in the Data Prepper {@link Buffer}.
 */
class S3ObjectWorker {
    private static final Logger LOG = LoggerFactory.getLogger(S3ObjectWorker.class);

    private final S3Client s3Client;
    private final Buffer<Record<Event>> buffer;
    private final Codec codec;
    private final Duration bufferTimeout;
    private final int numberOfRecordsToAccumulate;

    public S3ObjectWorker(final S3Client s3Client,
                          final Buffer<Record<Event>> buffer,
                          final Codec codec,
                          final Duration bufferTimeout,
                          final int numberOfRecordsToAccumulate) {
        this.s3Client = s3Client;
        this.buffer = buffer;
        this.codec = codec;
        this.bufferTimeout = bufferTimeout;
        this.numberOfRecordsToAccumulate = numberOfRecordsToAccumulate;
    }

    void parseS3Object(final S3ObjectReference s3ObjectPointer) throws IOException {
        final GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(s3ObjectPointer.getBucketName())
                .key(s3ObjectPointer.getKey())
                .build();

        final BufferAccumulator bufferAccumulator = BufferAccumulator.create(buffer, numberOfRecordsToAccumulate, bufferTimeout);

        try (final ResponseInputStream<GetObjectResponse> object = s3Client.getObject(getObjectRequest)) {
            codec.parse(object, record -> {
                try {
                    bufferAccumulator.add(record);
                } catch (Exception e) {
                    LOG.error("Failed writing S3 objects to buffer.", e);
                }
            });
        } catch (IOException e) {
            LOG.error("Error reading from S3 object: s3ObjectReference={}.", s3ObjectPointer, e);
            throw e;
        }

        try {
            bufferAccumulator.flush();
        } catch (Exception e) {
            LOG.error("Failed writing S3 objects to buffer.", e);
        }
    }
}
