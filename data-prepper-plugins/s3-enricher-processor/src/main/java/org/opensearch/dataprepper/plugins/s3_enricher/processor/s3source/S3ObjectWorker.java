/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.s3_enricher.processor.s3source;

import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.plugins.s3_enricher.processor.S3EnricherProcessor;
import org.opensearch.dataprepper.plugins.s3_enricher.processor.S3EnricherProcessorConfig;
import org.opensearch.dataprepper.plugins.s3_enricher.processor.cache.S3EnricherCacheService;
import org.opensearch.dataprepper.plugins.s3_enricher.processor.s3source.ownership.BucketOwnerProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

public class S3ObjectWorker {
    private static final Logger LOG = LoggerFactory.getLogger(S3EnricherProcessor.class);
    private final CompressionOption compressionOption;
    private final InputCodec codec;
    private final BucketOwnerProvider bucketOwnerProvider;
    private final S3EnricherObjectPluginMetrics s3ObjectPluginMetrics;
    private final S3Client s3Client;
    private final S3EnricherProcessorConfig s3EnricherProcessorConfig;
    private Instant lastModified;
    private S3EnricherCacheService cacheService;

    public S3ObjectWorker(S3EnricherProcessorConfig s3EnricherProcessorConfig, InputCodec codec, BucketOwnerProvider bucketOwnerProvider, S3Client s3Client, S3EnricherObjectPluginMetrics s3ObjectPluginMetrics, S3EnricherCacheService cacheService) {
        this.compressionOption = s3EnricherProcessorConfig.getCompression();
        this.s3EnricherProcessorConfig =  s3EnricherProcessorConfig;
        this.codec = codec;
        this.bucketOwnerProvider = bucketOwnerProvider;
        this.s3Client = s3Client;
        this.lastModified = Instant.now();
        this.s3ObjectPluginMetrics = s3ObjectPluginMetrics;
        this.cacheService = cacheService;
    }

    public void processS3Object(final S3ObjectReference s3ObjectReference) throws IOException {
        try {
            s3ObjectPluginMetrics.getS3ObjectReadTimer().recordCallable((Callable<Void>) () -> {
                doProcessObject(s3ObjectReference);
                return null;
            });
        } catch (final IllegalArgumentException e) {
            throw new IOException(e.getMessage());
        } catch (final IOException | RuntimeException e) {
            throw e;
        } catch (final Exception e) {
            // doParseObject does not throw Exception, only IOException or RuntimeException. But, Callable has Exception as a checked
            // exception on the interface. This catch block thus should not be reached, but, in case it is, wrap it.
            throw new RuntimeException(e);
        }
        s3ObjectPluginMetrics.getS3ObjectsSucceededCounter().increment();
    }

    public long consumeS3Object(final S3InputFile inputFile, final Consumer<Record<Event>> consumer) throws Exception {
        final S3ObjectReference s3ObjectReference = inputFile.getObjectReference();

        final CompressionOption fileCompressionOption = compressionOption != CompressionOption.AUTOMATIC ?
                compressionOption : CompressionOption.fromFileName(s3ObjectReference.getKey());

        codec.parse(inputFile, fileCompressionOption.getDecompressionEngine(), consumer::accept);
        return inputFile.getLength();
    }

    private void doProcessObject(final S3ObjectReference s3ObjectReference) throws Exception {
        final long s3ObjectSize;
        LOG.info("Read S3 object: {}", s3ObjectReference);
        final S3InputFile inputFile = new S3InputFile(s3Client, s3ObjectReference, bucketOwnerProvider, s3ObjectPluginMetrics);
        try {
            final Instant lastModifiedTime = inputFile.getLastModified();
            final Instant now = Instant.now();
            final Instant originationTime = (lastModifiedTime == null || lastModifiedTime.isAfter(now)) ? now : lastModifiedTime;
            s3ObjectSize = consumeS3Object(inputFile, (record) -> {
                try {
                    Event event = record.getData();

                    String correlationValue = event.getJsonNode().get(s3EnricherProcessorConfig.getCorrelationKey()).asText();

                    event.getMetadata().setExternalOriginationTime(originationTime);
                    event.getEventHandle().setExternalOriginationTime(originationTime);
                    cacheService.put(s3ObjectReference.uri(), correlationValue, event);
                } catch (final Exception e) {
                    LOG.error("Failed writing S3 objects to buffer due to: {}", e.getMessage());
                }
            });

        } catch (final Exception ex) {
            s3ObjectPluginMetrics.getS3ObjectsFailedCounter().increment();
            LOG.error("Error reading from S3 object: s3ObjectReference={}. {}", s3ObjectReference, ex.getMessage());
            throw ex;
        }

        s3ObjectPluginMetrics.getS3ObjectSizeSummary().record(s3ObjectSize);
    }

}
