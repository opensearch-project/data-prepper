/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

import java.util.Set;
import java.util.function.BiPredicate;

import static com.google.common.base.Preconditions.checkArgument;

public class GenericInSetOperator implements Operator<Boolean> {
    private final Integer symbol;
    private final String displayName;
    private final BiPredicate<Object, Object> operation;

    public GenericInSetOperator(final Integer symbol, BiPredicate<Object, Object> operation) {
        this.symbol = symbol;
        displayName = DataPrepperExpressionParser.VOCABULARY.getDisplayName(symbol);
        this.operation = operation;
    }

    @Override
    public Integer getRuleIndex() {
        return DataPrepperExpressionParser.RULE_setOperator;
    }

    @Override
    public Integer getSymbol() {
        return symbol;
    }

    @Override
    public Boolean evaluate(final Object... args) {
        checkArgument(args.length == 2, displayName + " requires operands length to be 2.");
        if (!(args[1] instanceof Set)) {
            throw new IllegalArgumentException(displayName + " requires right operand to be Set.");
        }
        return operation.test(args[0], args[1]);
    }
}
