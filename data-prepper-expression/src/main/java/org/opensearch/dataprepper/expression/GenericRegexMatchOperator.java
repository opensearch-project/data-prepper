/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.antlr.v4.runtime.RuleContext;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;
import org.opensearch.dataprepper.model.pattern.PatternSyntaxException;

import java.util.function.BiPredicate;

import static com.google.common.base.Preconditions.checkArgument;

class GenericRegexMatchOperator implements Operator<Boolean> {
    private final int symbol;
    private final String displayName;
    private final BiPredicate<Object, Object> operation;

    public GenericRegexMatchOperator(final int symbol, BiPredicate<Object, Object> operation) {
        this.symbol = symbol;
        displayName = DataPrepperExpressionParser.VOCABULARY.getDisplayName(symbol);
        this.operation = operation;
    }

    @Override
    public boolean isBooleanOperator() {
        return true;
    }

    @Override
    public boolean shouldEvaluate(final RuleContext ctx) {
        return ctx.getRuleIndex() == DataPrepperExpressionParser.RULE_regexOperatorExpression;
    }

    @Override
    public int getSymbol() {
        return symbol;
    }

    @Override
    public Boolean evaluate(final Object ... args) {
        checkArgument(args.length == 2, displayName + " requires operands length needs to be 2.");
        if(args[0] == null)
            return false;
        checkArgument(args[0] instanceof String, displayName + " requires left operand to be String.");
        checkArgument(args[1] instanceof String, displayName + " requires right operand to be String.");
        try {
            return operation.test(args[0], args[1]);
        } catch (final PatternSyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
