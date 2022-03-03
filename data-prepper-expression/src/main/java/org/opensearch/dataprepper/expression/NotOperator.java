/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

import javax.inject.Named;

import static com.google.common.base.Preconditions.checkArgument;

@Named
class NotOperator implements Operator<Boolean> {
    private static final Integer SYMBOL = DataPrepperExpressionParser.NOT;
    private static final String DISPLAY_NAME = DataPrepperExpressionParser.VOCABULARY
            .getDisplayName(DataPrepperExpressionParser.NOT);

    @Override
    public Integer getSymbol() {
        return SYMBOL;
    }

    @Override
    public Boolean evaluate(final Object... args) {
        checkArgument(args.length == 1, DISPLAY_NAME + " requires operands length to be 1.");
        checkArgument(args[0] instanceof Boolean, DISPLAY_NAME + " requires operand to be Boolean.");
        return !((Boolean) args[0]);
    }
}
