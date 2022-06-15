/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.plugins.source.codec.Codec;
import com.amazon.dataprepper.plugins.source.compression.CompressionEngine;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.Callable;

/**
 * Class responsible for taking an {@link S3ObjectReference} and creating all the necessary {@link Event}
 * objects in the Data Prepper {@link Buffer}.
 */
class S3ObjectWorker {
    private static final Logger LOG = LoggerFactory.getLogger(S3ObjectWorker.class);
    static final String S3_OBJECTS_FAILED_METRIC_NAME = "s3ObjectsFailed";
    static final String S3_OBJECTS_SUCCEEDED_METRIC_NAME = "s3ObjectsSucceeded";
    static final String S3_OBJECTS_TIME_ELAPSED_METRIC_NAME = "s3ObjectReadTimeElapsed";

    private final S3Client s3Client;
    private final Buffer<Record<Event>> buffer;
    private final CompressionEngine compressionEngine;
    private final Codec codec;
    private final Duration bufferTimeout;
    private final int numberOfRecordsToAccumulate;
    private final Counter s3ObjectsFailedCounter;
    private final Counter s3ObjectsSucceededCounter;
    private final Timer s3ObjectReadTimer;

    public S3ObjectWorker(final S3Client s3Client,
                          final Buffer<Record<Event>> buffer,
                          final CompressionEngine compressionEngine,
                          final Codec codec,
                          final Duration bufferTimeout,
                          final int numberOfRecordsToAccumulate,
                          final PluginMetrics pluginMetrics) {
        this.s3Client = s3Client;
        this.buffer = buffer;
        this.compressionEngine = compressionEngine;
        this.codec = codec;
        this.bufferTimeout = bufferTimeout;
        this.numberOfRecordsToAccumulate = numberOfRecordsToAccumulate;

        s3ObjectsFailedCounter = pluginMetrics.counter(S3_OBJECTS_FAILED_METRIC_NAME);
        s3ObjectsSucceededCounter = pluginMetrics.counter(S3_OBJECTS_SUCCEEDED_METRIC_NAME);
        s3ObjectReadTimer = pluginMetrics.timer(S3_OBJECTS_TIME_ELAPSED_METRIC_NAME);
    }

    void parseS3Object(final S3ObjectReference s3ObjectReference) throws IOException {
        final GetObjectRequest.Builder getObjectBuilder = GetObjectRequest.builder()
                .bucket(s3ObjectReference.getBucketName())
                .key(s3ObjectReference.getKey());
        s3ObjectReference.getBucketOwner().ifPresent(getObjectBuilder::expectedBucketOwner);
        final GetObjectRequest getObjectRequest = getObjectBuilder
                .build();

        final BufferAccumulator<Record<Event>> bufferAccumulator = BufferAccumulator.create(buffer, numberOfRecordsToAccumulate, bufferTimeout);
        s3ObjectReadTimer.wrap((Callable<Void>) () -> {
            doParseObject(s3ObjectReference, getObjectRequest, bufferAccumulator);

            return null;
        });

        s3ObjectsSucceededCounter.increment();
    }

    private void doParseObject(final S3ObjectReference s3ObjectReference, final GetObjectRequest getObjectRequest, final BufferAccumulator<Record<Event>> bufferAccumulator) throws IOException {
        try (final ResponseInputStream<GetObjectResponse> responseInputStream = s3Client.getObject(getObjectRequest);
             final InputStream inputStream = compressionEngine.createInputStream(getObjectRequest.key(), responseInputStream)) {
            codec.parse(inputStream, record -> {
                try {
                    bufferAccumulator.add(record);
                } catch (final Exception e) {
                    LOG.error("Failed writing S3 objects to buffer.", e);
                }
            });
        } catch (final Exception e) {
            LOG.error("Error reading from S3 object: s3ObjectReference={}.", s3ObjectReference, e);
            s3ObjectsFailedCounter.increment();
            throw e;
        }

        try {
            bufferAccumulator.flush();
        } catch (final Exception e) {
            LOG.error("Failed writing S3 objects to buffer.", e);
        }
    }
}
