/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.antlr.v4.runtime.RuleContext;

interface Operator<T> {
    default int getNumberOfOperands(final RuleContext ctx) {
        return 2;
    }

    boolean isBooleanOperator();

    boolean shouldEvaluate(final RuleContext ctx);

    int getSymbol();

    /**
     * @since 1.3
     * Placeholder interface for implementing Data-Prepper supported binary/unary operations on operands that
     * returns custom type T.
     * @param args operands
     * @return T
     */
    T evaluate(final Object ... args);
}
