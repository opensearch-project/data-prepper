/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.translate;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventKeyFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * KeyResolver implementation with ConcurrentHashMap-based caching.
 */
class CachingKeyResolver implements KeyResolver {
    private final ConcurrentHashMap<String, KeyInfo> keyInfoCache;
    private final EventKeyFactory eventKeyFactory;

    CachingKeyResolver(EventKeyFactory eventKeyFactory) {
        this.eventKeyFactory = eventKeyFactory;
        this.keyInfoCache = new ConcurrentHashMap<>();
    }

    @Override
    public EventKey resolveKey(String keyStr, Event event, ExpressionEvaluator evaluator) {
        if (keyStr == null) {
            return null;
        }
        KeyInfo keyInfo = keyInfoCache.computeIfAbsent(keyStr, k -> new KeyInfo(k, eventKeyFactory));
        return keyInfo.resolveKey(event, evaluator);
    }

    /**
     * Returns the current size of the key info cache.
     *
     * @return current cache size
     */
    int getCacheSize() {
        return keyInfoCache.size();
    }

    /**
     * Clears the key info cache.
     */
    void clearCache() {
        keyInfoCache.clear();
    }
}