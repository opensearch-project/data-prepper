/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

interface Parser<T> {

    /**
     * @since 1.3
     * Parse a expression String to an object that can be evaluated by an {@link Evaluator}
     * @param expression String to be parsed
     * @return Object representing a parsed expression
     */
    T parse(final String expression) throws ParseTreeCompositeException;
}
