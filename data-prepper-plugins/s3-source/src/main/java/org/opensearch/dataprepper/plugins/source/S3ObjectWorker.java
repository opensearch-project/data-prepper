/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import org.apache.commons.compress.utils.CountingInputStream;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.plugins.source.codec.Codec;
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
class S3ObjectWorker implements S3ObjectHandler {
    private static final Logger LOG = LoggerFactory.getLogger(S3ObjectWorker.class);

    private final S3Client s3Client;
    private final Buffer<Record<Event>> buffer;
    private final CompressionEngine compressionEngine;
    private final Codec codec;
    private final BucketOwnerProvider bucketOwnerProvider;
    private final Duration bufferTimeout;
    private final int numberOfRecordsToAccumulate;
    private final BiConsumer<Event, S3ObjectReference> eventConsumer;
    private final S3ObjectPluginMetrics s3ObjectPluginMetrics;

    public S3ObjectWorker(final S3ObjectRequest s3ObjectRequest) {
        this.buffer = s3ObjectRequest.getBuffer();
        this.compressionEngine = s3ObjectRequest.getCompressionEngine();
        this.codec = s3ObjectRequest.getCodec();
        this.bucketOwnerProvider = s3ObjectRequest.getBucketOwnerProvider();
        this.bufferTimeout = s3ObjectRequest.getBufferTimeout();
        this.numberOfRecordsToAccumulate = s3ObjectRequest.getNumberOfRecordsToAccumulate();
        this.eventConsumer = s3ObjectRequest.getEventConsumer();
        this.s3Client = s3ObjectRequest.getS3Client();
        this.s3ObjectPluginMetrics = s3ObjectRequest.getS3ObjectPluginMetrics();
    }

    public void parseS3Object(final S3ObjectReference s3ObjectReference, final AcknowledgementSet acknowledgementSet) throws IOException {
        final GetObjectRequest.Builder getObjectBuilder = GetObjectRequest.builder()
                .bucket(s3ObjectReference.getBucketName())
                .key(s3ObjectReference.getKey());
        bucketOwnerProvider.getBucketOwner(s3ObjectReference.getBucketName()).ifPresent(getObjectBuilder::expectedBucketOwner);
        final GetObjectRequest getObjectRequest = getObjectBuilder
                .build();

        final BufferAccumulator<Record<Event>> bufferAccumulator = BufferAccumulator.create(buffer, numberOfRecordsToAccumulate, bufferTimeout);
        try {
            s3ObjectPluginMetrics.getS3ObjectReadTimer().recordCallable((Callable<Void>) () -> {
                doParseObject(acknowledgementSet, s3ObjectReference, getObjectRequest, bufferAccumulator);
                return null;
            });
        } catch (final IOException | RuntimeException e) {
            throw e;
        } catch (final Exception e) {
            // doParseObject does not throw Exception, only IOException or RuntimeException. But, Callable has Exception as a checked
            // exception on the interface. This catch block thus should not be reached, but, in case it is, wrap it.
            throw new RuntimeException(e);
        }
        s3ObjectPluginMetrics.getS3ObjectsSucceededCounter().increment();
    }

    private void doParseObject(final AcknowledgementSet acknowledgementSet, final S3ObjectReference s3ObjectReference, final GetObjectRequest getObjectRequest, final BufferAccumulator<Record<Event>> bufferAccumulator) throws IOException {
        final long s3ObjectSize;
        final long totalBytesRead;

        LOG.info("Read S3 object: {}", s3ObjectReference);

        try (final ResponseInputStream<GetObjectResponse> responseInputStream = s3Client.getObject(getObjectRequest);
             final CountingInputStream inputStream = new CountingInputStream(compressionEngine.createInputStream(getObjectRequest.key(), responseInputStream))) {
            s3ObjectSize = responseInputStream.response().contentLength();
            codec.parse(inputStream, record -> {
                try {
                    eventConsumer.accept(record.getData(), s3ObjectReference);
                    bufferAccumulator.add(record);
                    if (acknowledgementSet != null) {
                        acknowledgementSet.add(record.getData());
                    }
                } catch (final Exception e) {
                    LOG.error("Failed writing S3 objects to buffer due to: {}", e.getMessage());
                }
            });
            totalBytesRead = inputStream.getBytesRead();
        } catch (final Exception ex) {
            s3ObjectPluginMetrics.getS3ObjectsFailedCounter().increment();
            if (ex instanceof S3Exception) {
                LOG.error("Error reading from S3 object: s3ObjectReference={}. {}", s3ObjectReference, ex.getMessage());
                recordS3Exception((S3Exception) ex);
            }
            if (ex instanceof IOException) {
                LOG.error("Error while parsing S3 object: S3ObjectReference={}. {}", s3ObjectReference, ex.getMessage());
            }
            throw ex;
        }

        try {
            bufferAccumulator.flush();
        } catch (final Exception e) {
            LOG.error("Failed writing S3 objects to buffer.", e);
        }

        s3ObjectPluginMetrics.getS3ObjectSizeSummary().record(s3ObjectSize);
        s3ObjectPluginMetrics.getS3ObjectSizeProcessedSummary().record(totalBytesRead);
        s3ObjectPluginMetrics.getS3ObjectEventsSummary().record(bufferAccumulator.getTotalWritten());
    }

    private void recordS3Exception(final S3Exception ex) {
        if (ex.statusCode() == HttpStatusCode.NOT_FOUND) {
            s3ObjectPluginMetrics.getS3ObjectsFailedNotFoundCounter().increment();
        } else if (ex.statusCode() == HttpStatusCode.FORBIDDEN) {
            s3ObjectPluginMetrics.getS3ObjectsFailedAccessDeniedCounter().increment();
        }
    }
}
