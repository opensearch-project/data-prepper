/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.translate;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventKey;

/**
 * Resolves event keys with caching support.
 */
interface KeyResolver {
    /**
     * Resolves a key string to an EventKey, using caching where possible.
     *
     * @param keyStr The key string to resolve
     * @param event The event to resolve against for dynamic keys
     * @param evaluator Expression evaluator for dynamic keys
     * @return Resolved EventKey or null if resolution fails
     */
    EventKey resolveKey(String keyStr, Event event, ExpressionEvaluator evaluator);
}