/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.antlr.v4.runtime.RuleContext;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

import java.util.Map;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;

public class GenericEqualOperator extends BinaryOperator<Boolean> {
    private final Map<Class<?>, Map<Class<?>, BiFunction<Object, Object, Boolean>>> equalStrategy;

    public GenericEqualOperator(
            final int symbol,
            final int shouldEvaluateRuleIndex,
            final Map<Class<?>, Map<Class<?>, BiFunction<Object, Object, Boolean>>> equalStrategy
    ) {
        super(symbol, shouldEvaluateRuleIndex);
        this.equalStrategy = equalStrategy;
    }

    @Override
    protected Boolean checkedEvaluate(final Object lhs, final Object rhs) {
        if (lhs == null || rhs == null) {
            return lhs == rhs;
        }
        else if (hasStrategyMatching(lhs, rhs)) {
            return equalStrategy.get(lhs.getClass())
                    .get(rhs.getClass())
                    .apply(lhs, rhs);
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
