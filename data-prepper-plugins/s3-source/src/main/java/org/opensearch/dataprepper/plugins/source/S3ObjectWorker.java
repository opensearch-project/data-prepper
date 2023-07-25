/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.plugins.source.ownership.BucketOwnerProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

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

    private final CompressionOption compressionOption;
    private final InputCodec codec;
    private final BucketOwnerProvider bucketOwnerProvider;
    private final Duration bufferTimeout;
    private final int numberOfRecordsToAccumulate;
    private final BiConsumer<Event, S3ObjectReference> eventConsumer;
    private final S3ObjectPluginMetrics s3ObjectPluginMetrics;

    public S3ObjectWorker(final S3ObjectRequest s3ObjectRequest) {
        this.buffer = s3ObjectRequest.getBuffer();
        this.compressionOption = s3ObjectRequest.getCompressionOption();
        this.codec = s3ObjectRequest.getCodec();
        this.bucketOwnerProvider = s3ObjectRequest.getBucketOwnerProvider();
        this.bufferTimeout = s3ObjectRequest.getBufferTimeout();
        this.numberOfRecordsToAccumulate = s3ObjectRequest.getNumberOfRecordsToAccumulate();
        this.eventConsumer = s3ObjectRequest.getEventConsumer();
        this.s3Client = s3ObjectRequest.getS3Client();
        this.s3ObjectPluginMetrics = s3ObjectRequest.getS3ObjectPluginMetrics();
    }

    public void parseS3Object(final S3ObjectReference s3ObjectReference, final AcknowledgementSet acknowledgementSet) throws IOException {
        final BufferAccumulator<Record<Event>> bufferAccumulator = BufferAccumulator.create(buffer, numberOfRecordsToAccumulate, bufferTimeout);
        try {
            s3ObjectPluginMetrics.getS3ObjectReadTimer().recordCallable((Callable<Void>) () -> {
                doParseObject(acknowledgementSet, s3ObjectReference, bufferAccumulator);
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

    private void doParseObject(final AcknowledgementSet acknowledgementSet, final S3ObjectReference s3ObjectReference, final BufferAccumulator<Record<Event>> bufferAccumulator) throws IOException {
        final long s3ObjectSize;
        final long totalBytesRead;

        LOG.info("Read S3 object: {}", s3ObjectReference);

        final S3InputFile inputFile = new S3InputFile(s3Client, s3ObjectReference, bucketOwnerProvider, s3ObjectPluginMetrics);

        final CompressionOption fileCompressionOption = compressionOption != CompressionOption.AUTOMATIC ?
                compressionOption : CompressionOption.fromFileName(s3ObjectReference.getKey());

        try {
            s3ObjectSize = inputFile.getLength();

            codec.parse(inputFile, fileCompressionOption.getDecompressionEngine(), record -> {
                try {
                    eventConsumer.accept(record.getData(), s3ObjectReference);
                    // Always add record to acknowledgementSet before adding to
                    // buffer because another thread may take and process
                    // buffer contents before the event record is added
                    // to acknowledgement set
                    if (acknowledgementSet != null) {
                        acknowledgementSet.add(record.getData());
                    }
                    bufferAccumulator.add(record);
                } catch (final Exception e) {
                    LOG.error("Failed writing S3 objects to buffer due to: {}", e.getMessage());
                }
            });
        } catch (final Exception ex) {
            s3ObjectPluginMetrics.getS3ObjectsFailedCounter().increment();
            LOG.error("Error reading from S3 object: s3ObjectReference={}. {}", s3ObjectReference, ex.getMessage());
            throw ex;
        }

        try {
            bufferAccumulator.flush();
        } catch (final Exception e) {
            LOG.error("Failed writing S3 objects to buffer.", e);
        }

        final int recordsWritten = bufferAccumulator.getTotalWritten();

        if (recordsWritten == 0) {
            LOG.warn("Failed to find any records in S3 object: s3ObjectReference={}.", s3ObjectReference);
            s3ObjectPluginMetrics.getS3ObjectNoRecordsFound().increment();
        }
        s3ObjectPluginMetrics.getS3ObjectSizeSummary().record(s3ObjectSize);
        s3ObjectPluginMetrics.getS3ObjectEventsSummary().record(recordsWritten);
    }
}
