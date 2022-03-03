/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

import java.util.function.BiPredicate;
import java.util.regex.PatternSyntaxException;

import static com.google.common.base.Preconditions.checkArgument;

public class GenericRegexMatchOperator implements Operator<Boolean> {
    private final Integer symbol;
    private final String displayName;
    private final BiPredicate<Object, Object> operation;

    public GenericRegexMatchOperator(final Integer symbol, BiPredicate<Object, Object> operation) {
        this.symbol = symbol;
        displayName = DataPrepperExpressionParser.VOCABULARY.getDisplayName(symbol);
        this.operation = operation;
    }

    @Override
    public Integer getSymbol() {
        return symbol;
    }

    @Override
    public Boolean evaluate(final Object... args) {
        checkArgument(args.length == 2, displayName + " requires operands length needs to be 2.");
        checkArgument(args[0] instanceof String, displayName + " requires left operand to be String.");
        checkArgument(args[1] instanceof String, displayName + " requires right operand to be String.");
        try {
            return operation.test(args[0], args[1]);
        } catch (final PatternSyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
