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

class ArithmeticBinaryOperator implements Operator<Number> {
    private final int symbol;
    private final String displayName;
    private final Map<Class<? extends Number>, Map<Class<? extends Number>, BiFunction<Object, Object, Number>>> operandsToOperationMap;

    public ArithmeticBinaryOperator(final int symbol,
            final Map<Class<? extends Number>, Map<Class<? extends Number>, BiFunction<Object, Object, Number>>> operandsToOperationMap) {
        this.symbol = symbol;
        displayName = DataPrepperExpressionParser.VOCABULARY.getDisplayName(symbol);
        this.operandsToOperationMap = operandsToOperationMap;
    }

    @Override
    public boolean shouldEvaluate(final RuleContext ctx) {
        return ctx.getRuleIndex() == DataPrepperExpressionParser.RULE_multiplicativeExpression;
    }

    @Override
    public int getSymbol() {
        return symbol;
    }

    @Override
    public Number evaluate(final Object ... args) {
        checkArgument(args.length == 2, displayName + " requires operands length needs to be 2.");
        final Object leftValue = args[0];
        final Object rightValue = args[1];
        checkArgument(leftValue != null && rightValue != null, displayName + " requires operands length needs to be non-null.");
        final Class<?> leftValueClass = leftValue.getClass();
        final Class<?> rightValueClass = rightValue.getClass();
        if (!operandsToOperationMap.containsKey(leftValueClass)) {
            throw new IllegalArgumentException(displayName + " requires left operand to be either Float or Integer.");
        }
        Map<Class<? extends Number>, BiFunction<Object, Object, Number>> rightOperandToOperation =
                operandsToOperationMap.get(leftValueClass);
        if (!rightOperandToOperation.containsKey(rightValueClass)) {
            throw new IllegalArgumentException(displayName + " requires right operand to be either Float or Integer.");
        }
        final BiFunction<Object, Object, Number> operation = rightOperandToOperation.get(rightValueClass);
        return operation.apply(leftValue, rightValue);
    }
}

