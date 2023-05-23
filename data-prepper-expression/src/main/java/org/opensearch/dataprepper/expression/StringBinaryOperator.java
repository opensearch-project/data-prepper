/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.antlr.v4.runtime.RuleContext;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

import static com.google.common.base.Preconditions.checkArgument;

class StringBinaryOperator implements Operator<String> {
    private final int symbol;
    private final String displayName;

    public StringBinaryOperator(final int symbol) {
        this.symbol = symbol;
        displayName = DataPrepperExpressionParser.VOCABULARY.getDisplayName(symbol);
    }

    @Override
    public boolean shouldEvaluate(final RuleContext ctx) {
        if (symbol == DataPrepperExpressionParser.DOT) {
            return ctx.getRuleIndex() == DataPrepperExpressionParser.RULE_stringExpression;
        }
        return false;
    }

    @Override
    public int getSymbol() {
        return symbol;
    }

    @Override
    public String evaluate(final Object ... args) {
        checkArgument(args.length == 2, displayName + " requires operands length needs to be 2.");
        final Object leftValue = args[0];
        final Object rightValue = args[1];

        if (leftValue instanceof String && rightValue instanceof String) {
            return (String)leftValue + (String)rightValue;
        }

        throw new IllegalArgumentException(displayName + " requires both operands to be String type");
    }
}


