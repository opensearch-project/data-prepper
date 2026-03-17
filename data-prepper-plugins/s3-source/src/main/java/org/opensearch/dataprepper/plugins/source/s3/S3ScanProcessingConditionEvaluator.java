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
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
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
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Evaluates {@link S3ScanProcessingCondition} entries for a given S3 object before
 * the object is processed. For each applicable condition the evaluator downloads the
 * condition object co-located with the S3 object and evaluates the {@code when} expression
 * against its content. When a {@code codec} is configured on the condition the object is
 * parsed with that codec; otherwise the object is parsed as a JSON document.
 */
public class S3ScanProcessingConditionEvaluator {

    private static final Logger LOG = LoggerFactory.getLogger(S3ScanProcessingConditionEvaluator.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final S3Client s3Client;
    private final ExpressionEvaluator expressionEvaluator;
    private final Map<S3ScanProcessingCondition, InputCodec> codecCache;

    public S3ScanProcessingConditionEvaluator(final S3Client s3Client,
                                              final ExpressionEvaluator expressionEvaluator,
                                              final PluginFactory pluginFactory,
                                              final Collection<S3ScanProcessingCondition> allConditions) {
        this.s3Client = s3Client;
        this.expressionEvaluator = expressionEvaluator;
        final IdentityHashMap<S3ScanProcessingCondition, InputCodec> cache = new IdentityHashMap<>();
        for (final S3ScanProcessingCondition condition : allConditions) {
            if (condition.getCodec() != null) {
                final PluginModel codecModel = condition.getCodec();
                final PluginSetting pluginSetting = new PluginSetting(
                        codecModel.getPluginName(), codecModel.getPluginSettings());
                cache.put(condition, pluginFactory.loadPlugin(InputCodec.class, pluginSetting));
            }
        }
        this.codecCache = Collections.unmodifiableMap(cache);
    }

    /**
     * Returns the first condition in {@code conditions} that is not yet satisfied for the given
     * object, or {@link Optional#empty()} if all applicable conditions are met. A condition is
     * applicable when {@code applicable_prefix} is absent or the object key starts with at least
     * one of the listed applicable prefixes.
     */
    public Optional<S3ScanProcessingCondition> firstUnmetCondition(final String bucket,
                                                                    final String objectKey,
                                                                    final List<S3ScanProcessingCondition> conditions) {
        if (conditions == null || conditions.isEmpty()) {
            return Optional.empty();
        }
        for (final S3ScanProcessingCondition condition : conditions) {
            if (!isApplicable(objectKey, condition)) {
                continue;
            }
            final String conditionObjectKey = resolveConditionObjectKey(objectKey, condition.getObjectName());
            try {
                final Event event;
                final InputCodec codec = codecCache.get(condition);
                if (codec != null) {
                    event = parseFirstEventWithCodec(bucket, conditionObjectKey, codec);
                } else {
                    final String content = readS3ObjectAsString(bucket, conditionObjectKey);
                    final Map<String, Object> data = OBJECT_MAPPER.readValue(
                            content, new TypeReference<Map<String, Object>>() {});
                    event = JacksonEvent.builder()
                            .withEventType("event")
                            .withData(data)
                            .build();
                }
                if (event == null || !expressionEvaluator.evaluateConditional(condition.getWhen(), event)) {
                    LOG.debug("Processing condition '{}' not satisfied for {}/{} using condition object {}",
                            condition.getWhen(), bucket, objectKey, conditionObjectKey);
                    return Optional.of(condition);
                }
            } catch (final NoSuchKeyException e) {
                LOG.debug("Object for condition {}/{} not found yet, condition not met", bucket, conditionObjectKey);
                return Optional.of(condition);
            } catch (final Exception e) {
                LOG.warn("Error reading or evaluating processing condition for {}/{}, processing object as-is",
                        bucket, objectKey, e);
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    private boolean isApplicable(final String objectKey, final S3ScanProcessingCondition condition) {
        final List<String> includePrefix = condition.getApplicablePrefix();
        return includePrefix == null || includePrefix.isEmpty() ||
                includePrefix.stream().anyMatch(objectKey::startsWith);
    }

    private String resolveConditionObjectKey(final String objectKey, final String fileName) {
        final String directory = objectKey.contains("/")
                ? objectKey.substring(0, objectKey.lastIndexOf('/') + 1)
                : "";
        return directory + fileName;
    }

    private Event parseFirstEventWithCodec(final String bucket,
                                            final String key,
                                            final InputCodec codec) throws IOException {
        final AtomicReference<Event> firstEvent = new AtomicReference<>();
        final GetObjectRequest request = GetObjectRequest.builder().bucket(bucket).key(key).build();
        try (final ResponseInputStream<GetObjectResponse> response = s3Client.getObject(request)) {
            codec.parse(response, record -> firstEvent.compareAndSet(null, record.getData()));
        }
        return firstEvent.get();
    }

    private String readS3ObjectAsString(final String bucket, final String key) throws IOException {
        final GetObjectRequest request = GetObjectRequest.builder().bucket(bucket).key(key).build();
        try (final ResponseInputStream<GetObjectResponse> response = s3Client.getObject(request)) {
            return new String(response.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
