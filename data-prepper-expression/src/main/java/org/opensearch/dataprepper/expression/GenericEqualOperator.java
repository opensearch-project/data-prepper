/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import java.util.Map;
import java.util.function.BiPredicate;

class GenericEqualOperator extends BinaryOperator<Boolean> {
    private final Map<Class<?>, Map<Class<?>, BiPredicate<Object, Object>>> equalStrategy;

    public GenericEqualOperator(
            final int symbol,
            final int shouldEvaluateRuleIndex,
            final Map<Class<?>, Map<Class<?>, BiPredicate<Object, Object>>> equalStrategy
    ) {
        super(symbol, shouldEvaluateRuleIndex);
        this.equalStrategy = equalStrategy;
    }

    @Override
    public boolean isBooleanOperator() {
        return true;
    }

    @Override
    protected Boolean checkedEvaluate(final Object lhs, final Object rhs) {
        if (lhs == null || rhs == null) {
            return lhs == rhs;
        }
        else if (hasStrategyMatching(lhs, rhs)) {
            return equalStrategy.get(lhs.getClass())
                    .get(rhs.getClass())
                    .test(lhs, rhs);
        }
        else {
            return lhs.equals(rhs);
        }
    }

    private boolean hasStrategyMatching(final Object leftOperand, final Object rightOperand) {
        return equalStrategy.containsKey(leftOperand.getClass()) &&
                equalStrategy.get(leftOperand.getClass())
                        .containsKey(rightOperand.getClass());
    }
}
