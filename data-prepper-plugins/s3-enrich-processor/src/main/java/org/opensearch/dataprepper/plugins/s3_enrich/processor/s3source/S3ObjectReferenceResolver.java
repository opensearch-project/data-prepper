/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.s3_enrich.processor.s3source;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.s3.common.source.S3ObjectReference;
import org.opensearch.dataprepper.plugins.s3_enrich.processor.S3EnrichProcessorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Resolves an {@link S3ObjectReference} from a given {@link Event}
 * based on the processor configuration.
 */
public class S3ObjectReferenceResolver {
    private static final Logger LOG = LoggerFactory.getLogger(S3ObjectReferenceResolver.class);

    private final S3EnrichProcessorConfig config;
    private final Pattern baseNamePattern;

    public S3ObjectReferenceResolver(final S3EnrichProcessorConfig config) {
        this.config = config;
        this.baseNamePattern = Pattern.compile(config.getEnricherNamePattern());
    }

    /**
     * Resolves an S3ObjectReference from the given event.
     *
     * @param event the event containing the S3 key path
     * @return the resolved S3ObjectReference
     * @throws IllegalArgumentException if bucket or key is missing/blank
     */
    public S3ObjectReference resolve(final Event event) {
        final String bucket = config.getS3EnrichBucketOption().getName();
        final String s3Key = event.get(config.getEnricherKeyPath(), String.class);

        if (bucket == null || bucket.isBlank() || s3Key == null || s3Key.isBlank()) {
            LOG.warn("Missing bucket or key in event, skipping enrichment");
            throw new IllegalArgumentException("Missing bucket or key in event");
        }

        final String enrichFromObjectKeyName = getEnrichFromObjectKey(s3Key);
        return S3ObjectReference.bucketAndKey(bucket, enrichFromObjectKeyName).build();
    }

    /**
     * Constructs the enrichment source object key from the output key.
     */
    private String getEnrichFromObjectKey(final String s3Key) {
        final String enrichFromFileName = extractFileName(s3Key);
        if (enrichFromFileName == null) {
            LOG.warn("Could not extract filename from s3Key: {}", s3Key);
            throw new IllegalArgumentException("Could not extract filename from s3Key: " + s3Key);
        }

        return config.getS3IncludePrefix()
            .map(prefix -> prefix + enrichFromFileName)
            .orElse(enrichFromFileName);
    }

    private String extractFileName(final String outputKey) {
        if (outputKey == null || outputKey.isBlank()) {
            return null;
        }

        final String fileName = extractFileNameFromS3Key(outputKey);
        if (fileName == null || fileName.isBlank()) {
            LOG.debug("Could not extract filename from path: {}", outputKey);
            return null;
        }

        if (baseNamePattern == null) {
            LOG.warn("Base name pattern is not configured, returning original filename: {}", fileName);
            return fileName;
        }

        try {
            final Matcher matcher = baseNamePattern.matcher(fileName);
            if (matcher.matches() && matcher.groupCount() >= 1) {
                final String baseName = matcher.group(1);
                if (baseName != null && !baseName.isBlank()) {
                    return baseName + ".jsonl";
                }
            }
        } catch (Exception e) {
            LOG.error("Error matching pattern against filename: {}", fileName, e);
            return null;
        }

        LOG.debug("Pattern did not match filename: {}, returning as-is", fileName);
        return fileName;
    }

    private String extractFileNameFromS3Key(final String s3Key) {
        if (s3Key == null || s3Key.isBlank()) {
            return null;
        }
        final String normalized = s3Key.endsWith("/") ? s3Key.substring(0, s3Key.length() - 1) : s3Key;
        return normalized.substring(normalized.lastIndexOf('/') + 1);
    }
}
