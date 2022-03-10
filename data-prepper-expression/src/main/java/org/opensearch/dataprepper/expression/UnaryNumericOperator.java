/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import com.google.common.base.Function;
import org.antlr.v4.runtime.RuleContext;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

class UnaryNumericOperator implements Operator<Number> {
    private final int symbol;
    private final String displayName;
    private final Map<Class<? extends Number>, Function<Number, ? extends Number>> strategy;

    protected UnaryNumericOperator(
            final int symbol,
            final String displayName,
            final Map<Class<? extends Number>, Function<Number, ? extends Number>> strategy
    ) {
        this.symbol = symbol;
        this.displayName = displayName;
        this.strategy = strategy;
    }

    @Override
    public boolean shouldEvaluate(final RuleContext ctx) {
        return ctx.getRuleIndex() == DataPrepperExpressionParser.RULE_unaryOperatorExpression;
    }

    @Override
    public int getSymbol() {
        return symbol;
    }

    @Override
    public Number evaluate(final Object ... args) {
        checkArgument(args.length == 1, displayName + " requires operands length to be 1.");
        if (args[0] instanceof Number) {
            return strategy.get(args[0].getClass()).apply((Number) args[0]);
        }
        else {
            final String acceptedTypes = strategy.keySet()
                    .stream()
                    .map(Class::getName)
                    .collect(Collectors.joining(", "));
            throw new RuntimeException(displayName + " requires operand type to be one of the following " + acceptedTypes);
        }
    }
}
