/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.antlr.v4.runtime.RuleContext;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;

class ArithmeticSubtractOperator implements Operator<Number> {
    private final OperatorParameters operatorParameters;
    private final String displayName;

    public ArithmeticSubtractOperator(final OperatorParameters operatorParameters) {
        this.operatorParameters = operatorParameters;
        displayName = DataPrepperExpressionParser.VOCABULARY.getDisplayName(operatorParameters.getSymbol());
        // This line is removed
        // This line is removed
    }

    @Override
    public int getNumberOfOperands(final RuleContext ctx) {
        if (ctx.getRuleIndex() == DataPrepperExpressionParser.RULE_arithmeticExpression)
            return 2;
        return 1;
    }

    @Override
    public boolean shouldEvaluate(final RuleContext ctx) {
        return ctx.getRuleIndex() == DataPrepperExpressionParser.RULE_arithmeticExpression || 
               ctx.getRuleIndex() == DataPrepperExpressionParser.RULE_arithmeticUnaryExpression;
    }

    @Override
    public int getSymbol() {
        return operatorParameters.getSymbol();
    }

    @Override
    public Number evaluate(final Object ... args) {
        if (args.length == 1) {
            if (args[0] instanceof Number) {
                return operatorParameters.getStrategy().get(args[0].getClass()).apply((Number) args[0]);
            } else {
                throw new IllegalArgumentException(displayName + " requires operand to be either Float or Integer.");
            }
        }
        checkArgument(args.length == 2, displayName + " requires operands length needs to be 2.");
        final Object leftValue = args[0];
        final Object rightValue = args[1];
        final Class<?> leftValueClass = leftValue.getClass();
        final Class<?> rightValueClass = rightValue.getClass();
        if (!operatorParameters.getOperandsToOperationMap().containsKey(leftValueClass)) {
            throw new IllegalArgumentException(displayName + " requires left operand to be either Float or Integer.");
        }
        Map<Class<? extends Number>, BiFunction<Object, Object, Number>> rightOperandToOperation =
                operatorParameters.getOperandsToOperationMap().get(leftValueClass);
        if (!rightOperandToOperation.containsKey(rightValueClass)) {
            throw new IllegalArgumentException(displayName + " requires right operand to be either Float or Integer.");
        }
        final BiFunction<Object, Object, Number> operation = rightOperandToOperation.get(rightValueClass);
        return operation.apply(leftValue, rightValue);
    }
}


