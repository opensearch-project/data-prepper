/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.plugins.formatdetection.DetectedCompression;
import org.opensearch.dataprepper.plugins.formatdetection.DetectedFormat;
import org.opensearch.dataprepper.plugins.formatdetection.FormatDetectionResult;
import org.opensearch.dataprepper.plugins.formatdetection.FormatDetector;
import org.opensearch.dataprepper.plugins.s3.common.source.S3InputFile;
import org.opensearch.dataprepper.plugins.s3.common.source.S3ObjectReference;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3DataSelection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * S3ObjectHandler that auto-detects format and compression before processing.
 * Used when no codec is explicitly configured in the pipeline.
 */
class AutoDetectS3ObjectWorker implements S3ObjectHandler {
    private static final Logger LOG = LoggerFactory.getLogger(AutoDetectS3ObjectWorker.class);
    private static final int SAMPLE_SIZE = 65536;

    private final S3ObjectRequest s3ObjectRequest;
    private final AutoDetectCodecFactory codecFactory;
    private final FormatDetector formatDetector;
    private final DetectionMetrics metrics;

    AutoDetectS3ObjectWorker(final S3ObjectRequest s3ObjectRequest,
                             final AutoDetectCodecFactory codecFactory,
                             final PluginMetrics pluginMetrics) {
        this.s3ObjectRequest = s3ObjectRequest;
        this.codecFactory = codecFactory;
        this.formatDetector = codecFactory.getFormatDetector();
        this.metrics = new DetectionMetrics(pluginMetrics);
    }

    @Override
    public void processS3Object(final S3ObjectReference s3ObjectReference,
                                final S3DataSelection dataSelection,
                                final AcknowledgementSet acknowledgementSet,
                                final SourceCoordinator<S3SourceProgressState> sourceCoordinator,
                                final String partitionKey) throws IOException {
        final S3InputFile inputFile = new S3InputFile(
                s3ObjectRequest.getS3Client(), s3ObjectReference,
                s3ObjectRequest.getBucketOwnerProvider(),
                s3ObjectRequest.getS3ObjectPluginMetrics());

        // Read sample for detection
        final byte[] sample = readSample(inputFile);
        final long detectStart = System.nanoTime();
        final FormatDetectionResult detection = formatDetector.detect(sample);
        final long detectDurationNanos = System.nanoTime() - detectStart;
        final java.time.Duration detectDuration = java.time.Duration.ofNanos(detectDurationNanos);

        LOG.debug("Auto-detected format for s3://{}/{}: format={}, compression={}, confidence={}, detectionTimeMs={}",
                s3ObjectReference.getBucketName(), s3ObjectReference.getKey(),
                detection.getFormat(), detection.getCompression(), detection.getConfidence(),
                detectDurationNanos / 1_000_000.0);

        metrics.record(detection, detectDuration);

        if (detection.getFormat() == DetectedFormat.UNKNOWN) {
            LOG.error("Unable to detect format for s3://{}/{}. Skipping.",
                    s3ObjectReference.getBucketName(), s3ObjectReference.getKey());
            metrics.logSummary();
            return;
        }

        // Select codec based on detected format
        final InputCodec codec = codecFactory.getCodecForFormat(detection.getFormat());
        if (codec == null) {
            LOG.error("No codec available for format {} (s3://{}/{})",
                    detection.getFormat(), s3ObjectReference.getBucketName(), s3ObjectReference.getKey());
            return;
        }

        // Select decompression based on detected compression
        final CompressionOption compressionOption = mapCompression(detection.getCompression());

        // Process with detected codec and compression
        try {
            codec.parse(inputFile, compressionOption.getDecompressionEngine(), record -> {
                try {
                    s3ObjectRequest.getBuffer().write(record, (int) s3ObjectRequest.getBufferTimeout().toMillis());
                } catch (final Exception e) {
                    LOG.error("Failed writing to buffer: {}", e.getMessage());
                }
            });
            metrics.logSummary();
        } catch (final Exception e) {
            LOG.error("Failed to parse s3://{}/{} with codec {}: {}",
                    s3ObjectReference.getBucketName(), s3ObjectReference.getKey(),
                    detection.getFormat(), e.getMessage());
            throw new IOException(e);
        }
    }

    @Override
    public void deleteS3Object(final S3ObjectReference s3ObjectReference) {
        // Delegate to standard delete logic if needed
    }

    private byte[] readSample(final S3InputFile inputFile) {
        try (final InputStream is = inputFile.newStream()) {
            return is.readNBytes(SAMPLE_SIZE);
        } catch (final IOException e) {
            LOG.warn("Failed to read sample for detection: {}", e.getMessage());
            return new byte[0];
        }
    }

    private CompressionOption mapCompression(final DetectedCompression compression) {
        if (compression == DetectedCompression.GZIP) return CompressionOption.GZIP;
        if (compression == DetectedCompression.SNAPPY) return CompressionOption.SNAPPY;
        if (compression == DetectedCompression.ZSTD) return CompressionOption.ZSTD;
        return CompressionOption.NONE;
    }
}
