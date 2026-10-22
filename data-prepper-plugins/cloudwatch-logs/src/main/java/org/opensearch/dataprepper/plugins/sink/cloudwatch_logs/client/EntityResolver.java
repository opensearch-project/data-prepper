/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatchlogs.model.Entity;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;

/**
 * Resolves a CloudWatch Logs Entity from an event by interpolating the configured
 * {@code key_attributes}/{@code attributes} templates. Events whose key attributes interpolate to the
 * same value share a buffer group, and so one PutLogEvents request.
 *
 * <p>Deliberately stateless. {@link CloudWatchLogsService} already keys its groups by {@link ResolvedKey}
 * and each group holds its own entity, so caching entities here would be a second copy of the same
 * mapping — and one that never evicts, unlike the groups map. Resolution is split in two instead: every
 * event resolves a key, and only the creation of a new group builds an entity.
 */
public class EntityResolver {
    private static final Logger LOG = LoggerFactory.getLogger(EntityResolver.class);
    static final int MAX_ENTITY_VALUE_LENGTH = 512;
    static final String BLANK_VALUE = "blank_value";
    static final String VALUE_TOO_LONG = "value_too_long";

    private final Map<String, String> keyAttributeTemplates;
    private final Map<String, String> attributeTemplates;
    private final Map<String, String> headerTemplates;
    private final ExpressionEvaluator expressionEvaluator;
    private final CloudWatchLogsMetrics cloudWatchLogsMetrics;
    private final Set<String> loggedDrops = new HashSet<>();

    public EntityResolver(final Map<String, String> keyAttributeTemplates,
                          final Map<String, String> attributeTemplates,
                          final ExpressionEvaluator expressionEvaluator) {
        this(keyAttributeTemplates, attributeTemplates, Collections.emptyMap(), expressionEvaluator);
    }

    public EntityResolver(final Map<String, String> keyAttributeTemplates,
                          final Map<String, String> attributeTemplates,
                          final Map<String, String> headerTemplates,
                          final ExpressionEvaluator expressionEvaluator) {
        this(keyAttributeTemplates, attributeTemplates, headerTemplates, expressionEvaluator, null);
    }

    /**
     * @param cloudWatchLogsMetrics counts failed template interpolations and blank header values, or null to skip counting
     */
    public EntityResolver(final Map<String, String> keyAttributeTemplates,
                          final Map<String, String> attributeTemplates,
                          final Map<String, String> headerTemplates,
                          final ExpressionEvaluator expressionEvaluator,
                          final CloudWatchLogsMetrics cloudWatchLogsMetrics) {
        this.keyAttributeTemplates = keyAttributeTemplates;
        this.attributeTemplates = attributeTemplates;
        this.headerTemplates = headerTemplates;
        this.expressionEvaluator = expressionEvaluator;
        this.cloudWatchLogsMetrics = cloudWatchLogsMetrics;
    }

    /**
     * Resolves the grouping key for an event from the {@code key_attributes} and dynamic-header templates.
     * Runs for every event, so it interpolates only what the key needs. Headers are part of the key so that
     * events differing only by a resolved header value land in separate groups, and so separate requests.
     */
    public ResolvedKey resolveKey(final Event event) {
        final Map<String, String> keyAttributes = resolveTemplates(keyAttributeTemplates, event);
        final Map<String, String> headers = resolveTemplates(headerTemplates, event);
        countBlankHeaders(headers);
        return new ResolvedKey(keyAttributes, headers);
    }

    private void countBlankHeaders(final Map<String, String> headers) {
        if (cloudWatchLogsMetrics == null) {
            return;
        }
        int blank = 0;
        for (final String value : headers.values()) {
            if (value == null || value.isBlank()) {
                blank++;
            }
        }
        if (blank > 0) {
            cloudWatchLogsMetrics.increaseHeaderValueBlankCounter(blank);
        }
    }

    /**
     * Whether this resolver builds a per-group entity from templates. False when there are no key-attribute
     * templates, i.e. the group is being partitioned purely by dynamic headers and the entity, if any, is a
     * fixed one supplied by the caller.
     */
    public boolean buildsEntity() {
        return !keyAttributeTemplates.isEmpty();
    }

    /**
     * Builds the entity for an already-resolved key. Only called when a new buffer group is opened, so
     * the {@code attributes} templates are interpolated once per group rather than once per event.
     */
    public Entity buildEntity(final ResolvedKey resolvedKey, final Event event) {
        final Entity.Builder builder = Entity.builder().keyAttributes(resolvedKey.keyAttributes);
        final Map<String, String> resolvedAttributes = resolveTemplates(attributeTemplates, event);
        if (!resolvedAttributes.isEmpty()) {
            builder.attributes(resolvedAttributes);
        }
        return builder.build();
    }

    /**
     * Returns the reason this event's entity must not be sent, or empty if every resolved value is valid.
     * CloudWatch rejects the whole PutLogEvents request when an entity value is empty or over 512 characters.
     */
    public Optional<String> findInvalidValue(final ResolvedKey resolvedKey, final Event event) {
        final Optional<String> invalidKey = findInvalidIn(resolvedKey.keyAttributes, keyAttributeTemplates);
        if (invalidKey.isPresent()) {
            return invalidKey;
        }
        return findInvalidIn(resolveTemplates(attributeTemplates, event), attributeTemplates);
    }

    private Optional<String> findInvalidIn(final Map<String, String> resolved, final Map<String, String> templates) {
        for (final Map.Entry<String, String> entry : resolved.entrySet()) {
            final String reason = invalidReason(entry.getValue());
            if (reason != null) {
                if (loggedDrops.add(entry.getKey() + ':' + reason)) {
                    LOG.warn(NOISY, "Sending events without a CloudWatch entity: '{}' (template '{}') resolved to an "
                            + "invalid value, reason={}", entry.getKey(), templates.get(entry.getKey()), reason);
                }
                return Optional.of(reason);
            }
        }
        return Optional.empty();
    }

    private static String invalidReason(final String value) {
        if (value == null || value.isBlank()) {
            return BLANK_VALUE;
        }
        if (value.codePointCount(0, value.length()) > MAX_ENTITY_VALUE_LENGTH) {
            return VALUE_TOO_LONG;
        }
        return null;
    }

    /**
     * The resolved key attributes of one entity, and the key {@link CloudWatchLogsService} groups
     * buffers by.
     *
     * <p>Equality defers wholly to the resolved attribute map, so two events group together exactly when
     * they describe the same entity. Note that map equality ignores iteration order, which is what we
     * want: the same attributes listed in a different order are the same entity.
     */
    public static final class ResolvedKey {
        private final Map<String, String> keyAttributes;
        private final Map<String, String> headers;

        ResolvedKey(final Map<String, String> keyAttributes, final Map<String, String> headers) {
            this.keyAttributes = keyAttributes;
            this.headers = headers;
        }

        Map<String, String> getHeaders() {
            return headers;
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof ResolvedKey)) {
                return false;
            }
            final ResolvedKey that = (ResolvedKey) other;
            return keyAttributes.equals(that.keyAttributes) && headers.equals(that.headers);
        }

        @Override
        public int hashCode() {
            return 31 * keyAttributes.hashCode() + headers.hashCode();
        }

        @Override
        public String toString() {
            return headers.isEmpty() ? keyAttributes.toString() : keyAttributes + " " + headers;
        }
    }

    /**
     * Interpolates each template value against the event. Both {@code ${/field}} references and
     * {@code ${expression()}} statements are supported; the evaluator is what makes the latter work, so
     * it must be the same one that classified this config as dynamic. A reference to a field the event
     * does not carry resolves to an empty string (three-arg formatString) rather than throwing.
     */
    private Map<String, String> resolveTemplates(final Map<String, String> templates, final Event sampleEvent) {
        final Map<String, String> resolved = new LinkedHashMap<>();
        for (final Map.Entry<String, String> entry : templates.entrySet()) {
            final String value = entry.getValue();
            String interpolated;
            try {
                interpolated = sampleEvent.formatString(value, expressionEvaluator, "");
            } catch (final RuntimeException e) {
                // Malformed template string ("${" with no closing brace, etc.) or a failed expression.
                // Fall back to the literal so a single bad value never aborts entity resolution for the
                // whole group, and log it so this is never a silent substitution.
                LOG.warn("Failed to resolve entity attribute template '{}': {}", value, e.getMessage());
                interpolated = value;
                if (cloudWatchLogsMetrics != null) {
                    cloudWatchLogsMetrics.increaseTemplateExpressionFailedCounter(1);
                }
            }
            resolved.put(entry.getKey(), interpolated);
        }
        return resolved;
    }
}
