/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

import javax.inject.Named;

import static com.google.common.base.Preconditions.checkArgument;

@Named
class OrOperator implements Operator<Boolean> {
    private static final Integer SYMBOL = DataPrepperExpressionParser.OR;
    private static final String DISPLAY_NAME = DataPrepperExpressionParser.VOCABULARY
            .getDisplayName(DataPrepperExpressionParser.OR);

    @Override
    public Integer getRuleIndex() {
        return DataPrepperExpressionParser.RULE_conditionalOperator;
    }

    @Override
    public Integer getSymbol() {
        return SYMBOL;
    }

    @Override
    public Boolean evaluate(final Object... args) {
        checkArgument(args.length == 2, DISPLAY_NAME + " requires operands length to be 2.");
        checkArgument(args[0] instanceof Boolean, DISPLAY_NAME + " requires left operand to be Boolean.");
        checkArgument(args[1] instanceof Boolean, DISPLAY_NAME + " requires right Operand to be Boolean.");
        return ((Boolean) args[0]) || ((Boolean) args[1]);
    }
}
