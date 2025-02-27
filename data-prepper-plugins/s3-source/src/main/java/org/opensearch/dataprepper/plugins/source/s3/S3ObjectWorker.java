/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3;

import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.plugins.source.s3.ownership.BucketOwnerProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

/**
 * Class responsible for taking an {@link S3ObjectReference} and creating all the necessary {@link Event}
 * objects in the Data Prepper {@link Buffer}.
 */
class S3ObjectWorker implements S3ObjectHandler {
    private static final Logger LOG = LoggerFactory.getLogger(S3ObjectWorker.class);
    private static final long DEFAULT_CHECKPOINT_INTERVAL_MILLS = 5 * 60_000;

    private static final int MAX_RETRIES_DELETE_OBJECT = 3;
    private static final long DELETE_OBJECT_RETRY_DELAY_MS = 1000;

    private final S3Client s3Client;
    private final Buffer<Record<Event>> buffer;

    private final CompressionOption compressionOption;
    private final InputCodec codec;
    private final BucketOwnerProvider bucketOwnerProvider;
    private final Duration bufferTimeout;
    private final int numberOfRecordsToAccumulate;
    private final BiConsumer<Event, S3ObjectReference> eventConsumer;
    private final S3ObjectPluginMetrics s3ObjectPluginMetrics;
    private Instant lastModified;

    public S3ObjectWorker(final S3ObjectRequest s3ObjectRequest) {
        this.buffer = s3ObjectRequest.getBuffer();
        this.compressionOption = s3ObjectRequest.getCompressionOption();
        this.codec = s3ObjectRequest.getCodec();
        this.bucketOwnerProvider = s3ObjectRequest.getBucketOwnerProvider();
        this.bufferTimeout = s3ObjectRequest.getBufferTimeout();
        this.numberOfRecordsToAccumulate = s3ObjectRequest.getNumberOfRecordsToAccumulate();
        this.eventConsumer = s3ObjectRequest.getEventConsumer();
        this.s3Client = s3ObjectRequest.getS3Client();
        this.lastModified = Instant.now();
        this.s3ObjectPluginMetrics = s3ObjectRequest.getS3ObjectPluginMetrics();
    }

    private void processObjectMetadata(final AcknowledgementSet acknowledgementSet,
                               final S3ObjectReference s3ObjectReference,
                               final BufferAccumulator<Record<Event>> bufferAccumulator,
                               final SourceCoordinator<S3SourceProgressState> sourceCoordinator,
                               final String partitionKey) throws IOException {
        final S3InputFile inputFile = new S3InputFile(s3Client, s3ObjectReference, bucketOwnerProvider, s3ObjectPluginMetrics);
        final String BUCKET = "bucket";
        final String KEY = "key";
        final String TIME = "time";
        final String LENGTH = "length";
        Map<String, Object> data = new HashMap<>();
        data.put(BUCKET, s3ObjectReference.getBucketName());
        data.put(KEY, s3ObjectReference.getKey());
        data.put(TIME, inputFile.getLastModified());
        data.put(LENGTH, inputFile.getLength());
        Event event = JacksonEvent.builder()
                .withEventType("event")
                .withData(data)
                .build();
        final long s3ObjectSize = event.toJsonString().length();
        if (acknowledgementSet != null) {
            acknowledgementSet.add(event);
        }
        AtomicLong lastCheckpointTime = new AtomicLong(System.currentTimeMillis());
        final AtomicInteger saveStateCounter = new AtomicInteger();
        final Instant lastModifiedTime = inputFile.getLastModified();
        final Instant now = Instant.now();
        final Instant originationTime = (lastModifiedTime == null || lastModifiedTime.isAfter(now)) ? now : lastModifiedTime;
        event.getMetadata().setExternalOriginationTime(originationTime);
        event.getEventHandle().setExternalOriginationTime(originationTime);
        try {
            bufferAccumulator.add(new Record<>(event));
        } catch (final Exception e) {
            LOG.error("Failed writing S3 objects to buffer.", e);
        }
        if (acknowledgementSet != null && sourceCoordinator != null && partitionKey != null &&
                (System.currentTimeMillis() - lastCheckpointTime.get() > DEFAULT_CHECKPOINT_INTERVAL_MILLS)) {
            LOG.debug("Renew partition ownership for the object {}", partitionKey);
            sourceCoordinator.saveProgressStateForPartition(partitionKey, null);
            lastCheckpointTime.set(System.currentTimeMillis());
            saveStateCounter.getAndIncrement();
        }

        try {
            bufferAccumulator.flush();
        } catch (final Exception e) {
            LOG.error("Failed writing S3 objects to buffer.", e);
        }

        final int recordsWritten = bufferAccumulator.getTotalWritten();

        if (recordsWritten == 0) {
            LOG.warn("Failed to get metadata for S3 object: s3ObjectReference={}.", s3ObjectReference);
            s3ObjectPluginMetrics.getS3ObjectNoRecordsFound().increment();
        }
        s3ObjectPluginMetrics.getS3ObjectSizeSummary().record(s3ObjectSize);
        s3ObjectPluginMetrics.getS3ObjectEventsSummary().record(recordsWritten);
    }

    public void processS3ObjectMetadata(final S3ObjectReference s3ObjectReference,
                                        final AcknowledgementSet acknowledgementSet,
                                        final SourceCoordinator<S3SourceProgressState> sourceCoordinator,
                                        final String partitionKey) throws IOException {
        final BufferAccumulator<Record<Event>> bufferAccumulator = BufferAccumulator.create(buffer, numberOfRecordsToAccumulate, bufferTimeout);
        try {
            s3ObjectPluginMetrics.getS3ObjectReadTimer().recordCallable((Callable<Void>) () -> {
                processObjectMetadata(acknowledgementSet, s3ObjectReference, bufferAccumulator, sourceCoordinator, partitionKey);
                return null;
            });
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        s3ObjectPluginMetrics.getS3ObjectsSucceededCounter().increment();
    }

    public void parseS3Object(final S3ObjectReference s3ObjectReference,
                              final AcknowledgementSet acknowledgementSet,
                              final SourceCoordinator<S3SourceProgressState> sourceCoordinator,
                              final String partitionKey) throws IOException {
        final BufferAccumulator<Record<Event>> bufferAccumulator = BufferAccumulator.create(buffer, numberOfRecordsToAccumulate, bufferTimeout);
        try {
            s3ObjectPluginMetrics.getS3ObjectReadTimer().recordCallable((Callable<Void>) () -> {
                doParseObject(acknowledgementSet, s3ObjectReference, bufferAccumulator, sourceCoordinator, partitionKey);
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

    @Override
    public void deleteS3Object(final S3ObjectReference s3ObjectReference) {
        final DeleteObjectRequest.Builder deleteRequestBuilder = DeleteObjectRequest.builder()
                        .bucket(s3ObjectReference.getBucketName())
                        .key(s3ObjectReference.getKey());

        final Optional<String> bucketOwner = bucketOwnerProvider.getBucketOwner(s3ObjectReference.getBucketName());
        bucketOwner.ifPresent(deleteRequestBuilder::expectedBucketOwner);

        final DeleteObjectRequest deleteObjectRequest = deleteRequestBuilder.build();

        boolean deleteSuccessFul = false;
        int retryCount = 0;
        
        while (!deleteSuccessFul && retryCount < MAX_RETRIES_DELETE_OBJECT) {
            try {
                s3Client.deleteObject(deleteObjectRequest);
                deleteSuccessFul = true;
                LOG.debug("Successfully deleted object {} from bucket {} on attempt {}",
                        s3ObjectReference.getKey(), s3ObjectReference.getBucketName(), retryCount);
            } catch (final Exception e) {
                retryCount++;
                if (retryCount == MAX_RETRIES_DELETE_OBJECT) {
                    LOG.error("Failed to delete object {} from bucket {} after {} attempts: {}",
                            s3ObjectReference.getKey(), s3ObjectReference.getBucketName(), MAX_RETRIES_DELETE_OBJECT, e.getMessage());
                    s3ObjectPluginMetrics.getS3ObjectsDeleteFailed().increment();
                } else {
                    LOG.warn("Failed to delete object {} from bucket {} on attempt {}, will retry: {}",
                            s3ObjectReference.getKey(), s3ObjectReference.getBucketName(), retryCount, e.getMessage());
                    try {
                        Thread.sleep(DELETE_OBJECT_RETRY_DELAY_MS);
                    } catch (final InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
    }

    private void doParseObject(final AcknowledgementSet acknowledgementSet,
                               final S3ObjectReference s3ObjectReference,
                               final BufferAccumulator<Record<Event>> bufferAccumulator,
                               final SourceCoordinator<S3SourceProgressState> sourceCoordinator,
                               final String partitionKey) throws IOException {
        final long s3ObjectSize;
        final long totalBytesRead;

        LOG.info("Read S3 object: {}", s3ObjectReference);
        AtomicLong lastCheckpointTime = new AtomicLong(System.currentTimeMillis());

        final S3InputFile inputFile = new S3InputFile(s3Client, s3ObjectReference, bucketOwnerProvider, s3ObjectPluginMetrics);

        final CompressionOption fileCompressionOption = compressionOption != CompressionOption.AUTOMATIC ?
                compressionOption : CompressionOption.fromFileName(s3ObjectReference.getKey());

        final AtomicInteger saveStateCounter = new AtomicInteger();
        try {
            s3ObjectSize = inputFile.getLength();
            final Instant lastModifiedTime = inputFile.getLastModified();
            final Instant now = Instant.now();
            final Instant originationTime = (lastModifiedTime == null || lastModifiedTime.isAfter(now)) ? now : lastModifiedTime;

            codec.parse(inputFile, fileCompressionOption.getDecompressionEngine(), record -> {
                try {
                    Event event = record.getData();
                    eventConsumer.accept(event, s3ObjectReference);
                    event.getMetadata().setExternalOriginationTime(originationTime);
                    event.getEventHandle().setExternalOriginationTime(originationTime);
                    // Always add record to acknowledgementSet before adding to
                    // buffer because another thread may take and process
                    // buffer contents before the event record is added
                    // to acknowledgement set
                    if (acknowledgementSet != null) {
                        acknowledgementSet.add(event);
                    }
                    bufferAccumulator.add(record);

                    if (acknowledgementSet != null && sourceCoordinator != null && partitionKey != null &&
                            (System.currentTimeMillis() - lastCheckpointTime.get() > DEFAULT_CHECKPOINT_INTERVAL_MILLS)) {
                        LOG.debug("Renew partition ownership for the object {}", partitionKey);
                        sourceCoordinator.saveProgressStateForPartition(partitionKey, null);
                        lastCheckpointTime.set(System.currentTimeMillis());
                        saveStateCounter.getAndIncrement();
                    }
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
