/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import com.amazon.dataprepper.model.event.Event;

/**
 * @since 1.3
 * ExpressionEvaluator interface to abstract the parse and evaluate implementations.
 */
public interface ExpressionEvaluator<T> {

    /**
     * @since 1.3
     * Parse and evaluate the statement string, resolving external references with the provided context. Return type is
     *  ambiguous until statement evaluation is complete
     *
     * @param statement string to be parsed and evaluated
     * @param context event used to resolve external references in the statement
     * @return coerced result of statement evaluation
     */
    T evaluate(final String statement, final Event context);
}
