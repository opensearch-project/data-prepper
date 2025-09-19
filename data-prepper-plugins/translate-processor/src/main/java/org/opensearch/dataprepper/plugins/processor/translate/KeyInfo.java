/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.translate;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventKeyFactory;

/**
 * Caches and optimizes key resolution for both static and dynamic keys.
 */
class KeyInfo {
    private final String keyStr;
    private final EventKey staticKey;
    private final boolean isDynamic;
    private final String[] parsedComponents;
    private final EventKeyFactory factory;

    KeyInfo(String keyStr, EventKeyFactory factory) {
        this.keyStr = keyStr;
        this.factory = factory;
        this.isDynamic = keyStr != null && (keyStr.contains("%{") || keyStr.contains("${"));
        this.staticKey = !this.isDynamic && keyStr != null ? factory.createEventKey(keyStr) : null;
        this.parsedComponents = keyStr != null ? keyStr.split("/") : new String[0];
    }

    /**
     * Resolves the key for a given event, using cached static key when possible.
     *
     * @param event The event to resolve dynamic keys against
     * @param evaluator Expression evaluator for resolving dynamic keys
     * @return Resolved EventKey or null if key cannot be resolved
     */
    EventKey resolveKey(Event event, ExpressionEvaluator evaluator) {
        if (!isDynamic) {
            return staticKey;
        }
        try {
            String resolvedKey = event.formatString(keyStr, evaluator);
            return factory.createEventKey(resolvedKey);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Returns true if this key contains dynamic components that need resolution.
     *
     * @return true if key contains %{} or ${} patterns
     */
    boolean isDynamic() {
        return isDynamic;
    }

    /**
     * Returns the cached static key if this is a static key, null otherwise.
     *
     * @return cached static EventKey or null
     */
    EventKey getStaticKey() {
        return staticKey;
    }

    /**
     * Returns the original key string.
     *
     * @return original key string
     */
    String getKeyStr() {
        return keyStr;
    }

    /**
     * Returns pre-parsed path components for optimized path traversal.
     *
     * @return array of path components
     */
    String[] getParsedComponents() {
        return parsedComponents;
    }
}