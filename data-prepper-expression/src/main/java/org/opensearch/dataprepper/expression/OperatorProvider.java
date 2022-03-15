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
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;

@Named
class OperatorProvider {
    private final Map<Integer, Operator<?>> symbolToOperators;

    @Inject
    public OperatorProvider(final List<Operator<?>> operators) {
        checkArgument(Objects.nonNull(operators) && operators.stream().allMatch(Objects::nonNull),
                "Input operators should not contain null.");
        symbolToOperators = new HashMap<>();
        for (final Operator<?> operator : operators) {
            symbolToOperators.put(operator.getSymbol(), operator);
        }
    }

    public Operator<?> getOperator(final int symbol) {
        final Operator<?> operator = symbolToOperators.get(symbol);
        if (operator == null) {
            throw new UnsupportedOperationException("Unsupported operator symbol index: " + symbol);
        }
        return operator;
    }

    public boolean containsOperator(final int symbol) {
        return symbolToOperators.containsKey(symbol);
    }
}
