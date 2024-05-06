/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.antlr.v4.runtime.RuleContext;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

import java.util.function.BiPredicate;

import static com.google.common.base.Preconditions.checkArgument;

class GenericTypeOfOperator implements Operator<Boolean> {
    private final int symbol;
    private final String displayName;
    private final BiPredicate<Object, Object> operation;

    public GenericTypeOfOperator(final int symbol, BiPredicate<Object, Object> operation) {
        this.symbol = symbol;
        displayName = DataPrepperExpressionParser.VOCABULARY.getDisplayName(symbol);
        this.operation = operation;
    }

    @Override
    public boolean shouldEvaluate(final RuleContext ctx) {
        return ctx.getRuleIndex() == DataPrepperExpressionParser.RULE_typeOfOperatorExpression;
    }

    @Override
    public int getSymbol() {
        return symbol;
    }

    @Override
    public Boolean evaluate(final Object ... args) {
        checkArgument(args.length == 2, displayName + " requires operands length to be 2.");
        return operation.test(args[0], args[1]);
    }
}
