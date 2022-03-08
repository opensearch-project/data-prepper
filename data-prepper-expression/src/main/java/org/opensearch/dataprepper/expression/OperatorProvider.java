/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Named
class OperatorProvider {
    private final Map<Integer, Operator<?>> symbolToOperators;

    @Inject
    public OperatorProvider(final List<Operator<?>> operators) {
        symbolToOperators = new HashMap<>();
        for (final Operator<?> operator : operators) {
            symbolToOperators.put(operator.getSymbol(), operator);
        }
    }

    public Operator<?> getOperator(final Integer symbol) {
        final Operator<?> operator = symbolToOperators.get(symbol);
        if (operator == null) {
            throw new UnsupportedOperationException("Unsupported operator symbol index: " + symbol);
        }
        return operator;
    }
}
