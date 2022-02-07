/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.script;

import com.amazon.dataprepper.model.event.Event;

/**
 * @since 1.3
 * ScriptEngine interface to abstract the parse and evaluate implementations.
 */
public interface ScriptEngine {
    /**
     * @since 1.3
     * Parse and evaluate the statement string, resolving external references with the provided context. Return type is
     * ambiguous until statement evaluation is complete
     * @param statement string to be parsed and evaluated
     * @param context event used to resolve external references in the statement
     * @return the evaluated result of the statement
     */
    Object evaluate(final String statement, final Event context);

    /**
     * @since 1.3
     * Convenience method for evaluating a statement then casting the result.
     *
     * @see ScriptEngine#evaluate(String, Event)
     *
     * @param statement string to be parsed and evaluated
     * @param context event used to resolve external references in the statement
     * @param toValueType target class for coercion of result object
     * @param <T> target class type for coercion of result object
     * @return coerced result of statement evaluation
     *
     * @throws ClassCastException when unable to course statement result to type T
     */
    <T> T evaluate(final String statement, final Event context, final Class<T> toValueType) throws ClassCastException;
}
