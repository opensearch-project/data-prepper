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

import java.util.LinkedHashMap;
import java.util.Map;

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

    private final Map<String, String> keyAttributeTemplates;
    private final Map<String, String> attributeTemplates;
    private final ExpressionEvaluator expressionEvaluator;

    public EntityResolver(final Map<String, String> keyAttributeTemplates,
                          final Map<String, String> attributeTemplates,
                          final ExpressionEvaluator expressionEvaluator) {
        this.keyAttributeTemplates = keyAttributeTemplates;
        this.attributeTemplates = attributeTemplates;
        this.expressionEvaluator = expressionEvaluator;
    }

    /**
     * Resolves the grouping key for an event from the {@code key_attributes} templates. Runs for every
     * event, so it interpolates only what the key needs.
     */
    public ResolvedKey resolveKey(final Event event) {
        return new ResolvedKey(resolveTemplates(keyAttributeTemplates, event));
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
     * The resolved key attributes of one entity, and the key {@link CloudWatchLogsService} groups
     * buffers by.
     *
     * <p>Equality defers wholly to the resolved attribute map, so two events group together exactly when
     * they describe the same entity. Note that map equality ignores iteration order, which is what we
     * want: the same attributes listed in a different order are the same entity.
     */
    public static final class ResolvedKey {
        private final Map<String, String> keyAttributes;

        ResolvedKey(final Map<String, String> keyAttributes) {
            this.keyAttributes = keyAttributes;
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof ResolvedKey)) {
                return false;
            }
            return keyAttributes.equals(((ResolvedKey) other).keyAttributes);
        }

        @Override
        public int hashCode() {
            return keyAttributes.hashCode();
        }

        @Override
        public String toString() {
            return keyAttributes.toString();
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
            }
            resolved.put(entry.getKey(), interpolated);
        }
        return resolved;
    }
}
