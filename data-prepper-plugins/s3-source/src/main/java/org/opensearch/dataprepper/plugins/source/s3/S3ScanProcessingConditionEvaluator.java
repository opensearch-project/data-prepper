/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */
package org.opensearch.dataprepper.plugins.source.s3;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3ScanProcessingCondition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Evaluates {@link S3ScanProcessingCondition} entries for a given S3 object before
 * the object is processed. For each applicable condition the evaluator downloads the
 * manifest file co-located with the object and evaluates the {@code when} expression
 * against its JSON content.
 */
public class S3ScanProcessingConditionEvaluator {

    private static final Logger LOG = LoggerFactory.getLogger(S3ScanProcessingConditionEvaluator.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final S3Client s3Client;
    private final ExpressionEvaluator expressionEvaluator;

    public S3ScanProcessingConditionEvaluator(final S3Client s3Client,
                                              final ExpressionEvaluator expressionEvaluator) {
        this.s3Client = s3Client;
        this.expressionEvaluator = expressionEvaluator;
    }

    /**
     * Returns {@code true} if every applicable condition in {@code conditions} is satisfied
     * for the given object. A condition is applicable when {@code include_prefix} is absent
     * or the object key starts with at least one of the listed prefixes.
     */
    public boolean allConditionsMet(final String bucket,
                                    final String objectKey,
                                    final List<S3ScanProcessingCondition> conditions) {
        if (conditions == null || conditions.isEmpty()) {
            return true;
        }
        for (final S3ScanProcessingCondition condition : conditions) {
            if (!isApplicable(objectKey, condition)) {
                continue;
            }
            final String manifestKey = resolveManifestKey(objectKey, condition.getFileName());
            try {
                final String content = readS3ObjectAsString(bucket, manifestKey);
                final Map<String, Object> data = OBJECT_MAPPER.readValue(
                        content, new TypeReference<Map<String, Object>>() {});
                final JacksonEvent event = JacksonEvent.builder()
                        .withEventType("event")
                        .withData(data)
                        .build();
                if (!expressionEvaluator.evaluateConditional(condition.getWhen(), event)) {
                    LOG.debug("Processing condition '{}' not satisfied for {}/{} using manifest {}",
                            condition.getWhen(), bucket, objectKey, manifestKey);
                    return false;
                }
            } catch (final NoSuchKeyException e) {
                LOG.debug("Manifest file {}/{} not found yet, condition not met", bucket, manifestKey);
                return false;
            } catch (final Exception e) {
                LOG.error("Error evaluating processing condition for {}/{}", bucket, objectKey, e);
                return false;
            }
        }
        return true;
    }

    /**
     * Returns the first condition in {@code conditions} that is applicable to the given
     * object key, or {@code null} if none match.
     */
    public S3ScanProcessingCondition findFirstMatching(final String objectKey,
                                                       final List<S3ScanProcessingCondition> conditions) {
        if (conditions == null) {
            return null;
        }
        for (final S3ScanProcessingCondition condition : conditions) {
            if (isApplicable(objectKey, condition)) {
                return condition;
            }
        }
        return null;
    }

    private boolean isApplicable(final String objectKey, final S3ScanProcessingCondition condition) {
        final List<String> includePrefix = condition.getIncludePrefix();
        return includePrefix == null || includePrefix.isEmpty() ||
                includePrefix.stream().anyMatch(objectKey::startsWith);
    }

    private String resolveManifestKey(final String objectKey, final String fileName) {
        final String directory = objectKey.contains("/")
                ? objectKey.substring(0, objectKey.lastIndexOf('/') + 1)
                : "";
        return directory + fileName;
    }

    private String readS3ObjectAsString(final String bucket, final String key) throws IOException {
        final GetObjectRequest request = GetObjectRequest.builder().bucket(bucket).key(key).build();
        try (final ResponseInputStream<GetObjectResponse> response = s3Client.getObject(request)) {
            return new String(response.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
