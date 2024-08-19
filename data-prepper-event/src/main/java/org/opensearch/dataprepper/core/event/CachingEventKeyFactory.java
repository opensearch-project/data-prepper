/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.event;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventKeyFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Objects;

class CachingEventKeyFactory implements EventKeyFactory {
    private static final Logger log = LoggerFactory.getLogger(CachingEventKeyFactory.class);
    private final EventKeyFactory delegateEventKeyFactory;
    private final Cache<CacheKey, EventKey> cache;

    private static class CacheKey {
        private final String key;
        private final EventAction[] eventActions;

        private CacheKey(final String key, final EventAction[] eventActions) {
            this.key = key;
            this.eventActions = eventActions;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final CacheKey cacheKey = (CacheKey) o;
            return Objects.equals(key, cacheKey.key) && Arrays.equals(eventActions, cacheKey.eventActions);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(key);
            result = 31 * result + Arrays.hashCode(eventActions);
            return result;
        }
    }

    CachingEventKeyFactory(final EventKeyFactory delegateEventKeyFactory, final EventConfiguration eventConfiguration) {
        Objects.requireNonNull(delegateEventKeyFactory);
        Objects.requireNonNull(eventConfiguration);

        log.debug("Configured to cache a maximum of {} event keys.", eventConfiguration.getMaximumCachedKeys());

        this.delegateEventKeyFactory = delegateEventKeyFactory;
        cache = Caffeine.newBuilder()
                .maximumSize(eventConfiguration.getMaximumCachedKeys())
                .build();
    }

    @Override
    public EventKey createEventKey(final String key, final EventAction... forActions) {
        return getOrCreateEventKey(new CacheKey(key, forActions));
    }

    private EventKey getOrCreateEventKey(final CacheKey cacheKey) {
        return cache.asMap().computeIfAbsent(cacheKey, key -> delegateEventKeyFactory.createEventKey(key.key, key.eventActions));
    }
}
