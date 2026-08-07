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

import org.opensearch.dataprepper.model.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatchlogs.model.Entity;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolves a CloudWatch Logs Entity from an event by interpolating the configured
 * {@code key_attributes}/{@code attributes} templates. Events resolving to the same key attributes
 * share a group (one PutLogEvents request); the resolved key is returned alongside the entity so the
 * caller keys its buffers with the same value the cache uses.
 */
public class EntityResolver {
    private static final Logger LOG = LoggerFactory.getLogger(EntityResolver.class);

    /**
     * Upper bound on distinct entities cached and buffer groups held concurrently: a memory-safety
     * backstop against runaway key cardinality. Beyond it, events collapse into a shared fallback
     * group carrying no per-resource entity.
     */
    public static final int MAX_ENTITY_CARDINALITY = 1000;

    private final Map<String, String> keyAttributeTemplates;
    private final Map<String, String> attributeTemplates;
    private final int maxCacheSize;
    private final ConcurrentHashMap<String, Entity> entityCache = new ConcurrentHashMap<>();
    private volatile boolean cacheFullLogged = false;

    public EntityResolver(final Map<String, String> keyAttributeTemplates,
                          final Map<String, String> attributeTemplates,
                          final int maxCacheSize) {
        this.keyAttributeTemplates = keyAttributeTemplates;
        this.attributeTemplates = attributeTemplates;
        this.maxCacheSize = maxCacheSize;
    }

    /**
     * Resolves the entity for an event and returns it with its grouping key (the resolved key
     * attributes).
     */
    public ResolvedEntity resolve(final Event event) {
        final Map<String, String> resolvedKeyAttributes = resolveTemplates(keyAttributeTemplates, event);
        final String key = resolvedKeyAttributes.toString();

        final Entity cached = entityCache.get(key);
        if (cached != null) {
            return new ResolvedEntity(key, cached);
        }

        final Entity entity = buildEntity(resolvedKeyAttributes, resolveTemplates(attributeTemplates, event));

        // Bound the cache: only retain when there is room.
        if (entityCache.size() < maxCacheSize) {
            return new ResolvedEntity(key, entityCache.computeIfAbsent(key, k -> entity));
        }

        if (!cacheFullLogged) {
            cacheFullLogged = true;
            LOG.warn("Entity cache reached its limit of {} distinct entities; further keys will be "
                    + "resolved on each flush without caching. Reduce the cardinality of the templated "
                    + "entity key attributes.", maxCacheSize);
        }
        return new ResolvedEntity(key, entity);
    }

    private Entity buildEntity(final Map<String, String> resolvedKeyAttributes,
                               final Map<String, String> resolvedAttributes) {
        final Entity.Builder builder = Entity.builder().keyAttributes(resolvedKeyAttributes);
        if (!resolvedAttributes.isEmpty()) {
            builder.attributes(resolvedAttributes);
        }
        return builder.build();
    }

    /**
     * A resolved entity paired with the grouping key it was resolved under.
     */
    public static final class ResolvedEntity {
        private final String key;
        private final Entity entity;

        ResolvedEntity(final String key, final Entity entity) {
            this.key = key;
            this.entity = entity;
        }

        public String getKey() {
            return key;
        }

        public Entity getEntity() {
            return entity;
        }
    }

    /**
     * Interpolates {@code ${...}} references in each template value; missing keys resolve to an empty
     * string (three-arg formatString) rather than throwing.
     */
    private Map<String, String> resolveTemplates(final Map<String, String> templates, final Event sampleEvent) {
        final Map<String, String> resolved = new LinkedHashMap<>();
        for (final Map.Entry<String, String> entry : templates.entrySet()) {
            final String value = entry.getValue();
            String interpolated;
            try {
                interpolated = sampleEvent.formatString(value, null, "");
            } catch (final RuntimeException e) {
                // Malformed template string ("${" with no closing brace, etc.). Fall back to the
                // literal so a single bad value never aborts entity resolution for the whole group.
                LOG.warn("Failed to resolve entity attribute template '{}': {}", value, e.getMessage());
                interpolated = value;
            }
            resolved.put(entry.getKey(), interpolated);
        }
        return resolved;
    }

    /**
     * Current number of distinct entities retained in the cache. Exposed for the cardinality gauge.
     */
    public int cacheSize() {
        return entityCache.size();
    }

    /**
     * The maximum number of distinct entities retained; the service uses the same bound to cap its
     * buffer groups.
     */
    public int maxCacheSize() {
        return maxCacheSize;
    }
}
