/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;

class LessThanOperator implements Operator<Boolean> {
    private static final Map<String, BiFunction<Object, Object, Boolean>> LESS_THAN_ON =
            new HashMap<String, BiFunction<Object, Object, Boolean>>() {{
                put(Integer.class.getName() + "_" + Integer.class.getName(), (a, b) -> (Integer) a < (Integer) b);
                put(Integer.class.getName() + "_" + Float.class.getName(), (a, b) -> (Integer) a < (Float) b);
                put(Float.class.getName() + "_" + Integer.class.getName(), (a, b) -> (Float) a < (Integer) b);
                put(Float.class.getName() + "_" + Float.class.getName(), (a, b) -> (Float) a < (Float) b);}};

    @Override
    public Integer getSymbol() {
        return DataPrepperExpressionParser.LT;
    }

    @Override
    public Boolean evaluate(Object... args) {
        checkArgument(args.length == 2, "Operands length needs to be 2.");
        final Object leftValue = args[0];
        final Object rightValue = args[1];
        final BiFunction<Object, Object, Boolean> operation = LESS_THAN_ON.get(
                BinaryOperandsKeyFactory.typesKey(leftValue, rightValue));
        if (operation == null) {
            throw new IllegalArgumentException(leftValue + " or " + rightValue + " should be either Float or Integer");
        }
        return operation.apply(leftValue, rightValue);
    }
}
