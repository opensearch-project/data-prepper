/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.s3;

import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.plugins.formatdetection.DetectedFormat;
import org.opensearch.dataprepper.plugins.formatdetection.FormatDetectionResult;
import org.opensearch.dataprepper.plugins.formatdetection.FormatDetector;
import org.opensearch.dataprepper.plugins.s3.common.ownership.BucketOwnerProvider;
import org.opensearch.dataprepper.plugins.s3.common.source.S3ObjectReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * A {@link Function} that, given an {@link S3ObjectReference}, reads a small byte-range sample
 * from the object, auto-detects its format, and returns the appropriate {@link InputCodec}.
 *
 * <p>Used when no codec is explicitly configured in the S3 source. It reads only the first
 * {@code SAMPLE_SIZE} bytes via a ranged GET so it does not consume the object's full content
 * or advance the stream used for processing.
 */
class AutoDetectCodecFactory implements Function<S3ObjectReference, InputCodec> {
    private static final Logger LOG = LoggerFactory.getLogger(AutoDetectCodecFactory.class);
    static final int SAMPLE_SIZE = 65536; // 64 KB

    private final PluginFactory pluginFactory;
    private final S3Client s3Client;
    private final BucketOwnerProvider bucketOwnerProvider;
    private final FormatDetector formatDetector;
    private final Map<DetectedFormat, InputCodec> codecCache;

    AutoDetectCodecFactory(final PluginFactory pluginFactory,
                           final S3Client s3Client,
                           final BucketOwnerProvider bucketOwnerProvider) {
        this.pluginFactory = pluginFactory;
        this.s3Client = s3Client;
        this.bucketOwnerProvider = bucketOwnerProvider;
        this.formatDetector = new FormatDetector();
        this.codecCache = new ConcurrentHashMap<>();
    }

    @Override
    public InputCodec apply(final S3ObjectReference s3ObjectReference) {
        final byte[] sample = readSample(s3ObjectReference);
        final FormatDetectionResult detection = formatDetector.detect(sample);

        LOG.debug("Auto-detected format for s3://{}/{}: format={}, compression={}, confidence={}",
                s3ObjectReference.getBucketName(), s3ObjectReference.getKey(),
                detection.getFormat(), detection.getCompression(), detection.getConfidence());

        return getCodecForFormat(detection.getFormat());
    }

    private byte[] readSample(final S3ObjectReference s3ObjectReference) {
        final GetObjectRequest.Builder requestBuilder = GetObjectRequest.builder()
                .bucket(s3ObjectReference.getBucketName())
                .key(s3ObjectReference.getKey())
                .range(String.format("bytes=0-%d", SAMPLE_SIZE - 1));

        final Optional<String> bucketOwner = bucketOwnerProvider.getBucketOwner(s3ObjectReference.getBucketName());
        bucketOwner.ifPresent(requestBuilder::expectedBucketOwner);

        try (final ResponseInputStream<GetObjectResponse> stream = s3Client.getObject(requestBuilder.build())) {
            return stream.readAllBytes();
        } catch (final IOException | RuntimeException e) {
            LOG.warn("Failed to read sample for format detection of s3://{}/{}: {}",
                    s3ObjectReference.getBucketName(), s3ObjectReference.getKey(), e.getMessage());
            return new byte[0];
        }
    }

    private InputCodec getCodecForFormat(final DetectedFormat format) {
        return codecCache.computeIfAbsent(format, this::createCodec);
    }

    private InputCodec createCodec(final DetectedFormat format) {
        final String pluginName = mapFormatToCodecPlugin(format);
        if (pluginName == null) {
            LOG.warn("No codec available for detected format: {}", format);
            return null;
        }

        LOG.debug("Auto-detecting codec: {} -> {}", format, pluginName);
        final PluginSetting pluginSetting = new PluginSetting(pluginName, Collections.emptyMap());
        return pluginFactory.loadPlugin(InputCodec.class, pluginSetting);
    }

    private String mapFormatToCodecPlugin(final DetectedFormat format) {
        if (format == DetectedFormat.NDJSON) {
            return "ndjson";
        }
        if (format == DetectedFormat.JSON) {
            return "json";
        }
        if (format == DetectedFormat.CSV || format == DetectedFormat.TSV) {
            return "csv";
        }
        if (format == DetectedFormat.PARQUET) {
            return "parquet";
        }
        if (format == DetectedFormat.AVRO) {
            return "avro";
        }
        if (format == DetectedFormat.TEXT) {
            return "newline";
        }
        return null;
    }
}
