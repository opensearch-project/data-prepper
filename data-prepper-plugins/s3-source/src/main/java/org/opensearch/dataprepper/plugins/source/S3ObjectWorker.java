/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.apache.commons.compress.utils.CountingInputStream;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.compression.CompressionEngine;
import org.opensearch.dataprepper.plugins.source.ownership.BucketOwnerProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

/**
 * Class responsible for taking an {@link S3ObjectReference} and creating all the necessary {@link Event}
 * objects in the Data Prepper {@link Buffer}.
 */
class S3ObjectWorker {
    private static final Logger LOG = LoggerFactory.getLogger(S3ObjectWorker.class);
    static final String S3_OBJECTS_FAILED_METRIC_NAME = "s3ObjectsFailed";
    static final String S3_OBJECTS_FAILED_NOT_FOUND_METRIC_NAME = "s3ObjectsNotFound";
    static final String S3_OBJECTS_FAILED_NOT_FOUND_ACCESS_DENIED = "s3ObjectsAccessDenied";
    static final String S3_OBJECTS_SUCCEEDED_METRIC_NAME = "s3ObjectsSucceeded";
    static final String S3_OBJECTS_TIME_ELAPSED_METRIC_NAME = "s3ObjectReadTimeElapsed";
    static final String S3_OBJECTS_SIZE = "s3ObjectSizeBytes";
    static final String S3_OBJECTS_SIZE_PROCESSED = "s3ObjectProcessedBytes";
    static final String S3_OBJECTS_EVENTS = "s3ObjectsEvents";

    private final S3Client s3Client;
    private final Buffer<Record<Event>> buffer;
    private final CompressionEngine compressionEngine;
    private final InputCodec codec;
    private final BucketOwnerProvider bucketOwnerProvider;
    private final Duration bufferTimeout;
    private final int numberOfRecordsToAccumulate;
    private final BiConsumer<Event, S3ObjectReference> eventConsumer;
    private final Counter s3ObjectsFailedCounter;
    private final Counter s3ObjectsFailedNotFoundCounter;
    private final Counter s3ObjectsFailedAccessDeniedCounter;
    private final Counter s3ObjectsSucceededCounter;
    private final Timer s3ObjectReadTimer;
    private final DistributionSummary s3ObjectSizeSummary;
    private final DistributionSummary s3ObjectSizeProcessedSummary;
    private final DistributionSummary s3ObjectEventsSummary;

    public S3ObjectWorker(final S3Client s3Client,
                          final Buffer<Record<Event>> buffer,
                          final CompressionEngine compressionEngine,
                          final InputCodec codec,
                          final BucketOwnerProvider bucketOwnerProvider,
                          final Duration bufferTimeout,
                          final int numberOfRecordsToAccumulate,
                          final BiConsumer<Event, S3ObjectReference> eventConsumer,
                          final PluginMetrics pluginMetrics) {
        this.s3Client = s3Client;
        this.buffer = buffer;
        this.compressionEngine = compressionEngine;
        this.codec = codec;
        this.bucketOwnerProvider = bucketOwnerProvider;
        this.bufferTimeout = bufferTimeout;
        this.numberOfRecordsToAccumulate = numberOfRecordsToAccumulate;
        this.eventConsumer = eventConsumer;

        s3ObjectsFailedCounter = pluginMetrics.counter(S3_OBJECTS_FAILED_METRIC_NAME);
        s3ObjectsFailedNotFoundCounter = pluginMetrics.counter(S3_OBJECTS_FAILED_NOT_FOUND_METRIC_NAME);
        s3ObjectsFailedAccessDeniedCounter = pluginMetrics.counter(S3_OBJECTS_FAILED_NOT_FOUND_ACCESS_DENIED);
        s3ObjectsSucceededCounter = pluginMetrics.counter(S3_OBJECTS_SUCCEEDED_METRIC_NAME);
        s3ObjectReadTimer = pluginMetrics.timer(S3_OBJECTS_TIME_ELAPSED_METRIC_NAME);
        s3ObjectSizeSummary = pluginMetrics.summary(S3_OBJECTS_SIZE);
        s3ObjectSizeProcessedSummary = pluginMetrics.summary(S3_OBJECTS_SIZE_PROCESSED);
        s3ObjectEventsSummary = pluginMetrics.summary(S3_OBJECTS_EVENTS);
    }

    void parseS3Object(final S3ObjectReference s3ObjectReference) throws IOException {
        final GetObjectRequest.Builder getObjectBuilder = GetObjectRequest.builder()
                .bucket(s3ObjectReference.getBucketName())
                .key(s3ObjectReference.getKey());
        bucketOwnerProvider.getBucketOwner(s3ObjectReference.getBucketName()).ifPresent(getObjectBuilder::expectedBucketOwner);
        final GetObjectRequest getObjectRequest = getObjectBuilder
                .build();

        final BufferAccumulator<Record<Event>> bufferAccumulator = BufferAccumulator.create(buffer, numberOfRecordsToAccumulate, bufferTimeout);
        try {
            s3ObjectReadTimer.recordCallable((Callable<Void>) () -> {
                doParseObject(s3ObjectReference, getObjectRequest, bufferAccumulator);

                return null;
            });
        } catch (final IOException | RuntimeException e) {
            throw e;
        } catch (final Exception e) {
            // doParseObject does not throw Exception, only IOException or RuntimeException. But, Callable has Exception as a checked
            // exception on the interface. This catch block thus should not be reached, but in case it is, wrap it.
            throw new RuntimeException(e);
        }

        s3ObjectsSucceededCounter.increment();
    }

    private void doParseObject(final S3ObjectReference s3ObjectReference, final GetObjectRequest getObjectRequest, final BufferAccumulator<Record<Event>> bufferAccumulator) throws IOException {
        final long s3ObjectSize;
        final long totalBytesRead;

        try (final ResponseInputStream<GetObjectResponse> responseInputStream = s3Client.getObject(getObjectRequest);
             final CountingInputStream inputStream = new CountingInputStream(compressionEngine.createInputStream(getObjectRequest.key(), responseInputStream))) {
            s3ObjectSize = responseInputStream.response().contentLength();
            codec.parse(inputStream, record -> {
                try {
                    eventConsumer.accept(record.getData(), s3ObjectReference);
                    bufferAccumulator.add(record);
                } catch (final Exception e) {
                    LOG.error("Failed writing S3 objects to buffer.", e);
                }
            });
            totalBytesRead = inputStream.getBytesRead();
        } catch (final Exception ex) {
            LOG.error("Error reading from S3 object: s3ObjectReference={}.", s3ObjectReference, ex);
            s3ObjectsFailedCounter.increment();
            if(ex instanceof S3Exception) {
                recordS3Exception((S3Exception) ex);
            }
            throw ex;
        }

        try {
            bufferAccumulator.flush();
        } catch (final Exception e) {
            LOG.error("Failed writing S3 objects to buffer.", e);
        }

        s3ObjectSizeSummary.record(s3ObjectSize);
        s3ObjectSizeProcessedSummary.record(totalBytesRead);
        s3ObjectEventsSummary.record(bufferAccumulator.getTotalWritten());
    }

    private void recordS3Exception(final S3Exception ex) {
        if(ex.statusCode() == HttpStatusCode.NOT_FOUND) {
            s3ObjectsFailedNotFoundCounter.increment();
        }
        else if(ex.statusCode() == HttpStatusCode.FORBIDDEN) {
            s3ObjectsFailedAccessDeniedCounter.increment();
        }
    }
}
