/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

import static com.google.common.base.Preconditions.checkArgument;

class AndOperator implements Operator<Boolean> {
    @Override
    public Integer getSymbol() {
        return DataPrepperExpressionParser.AND;
    }

    @Override
    public Boolean evaluate(Object... args) {
        checkArgument(args.length == 2, "Operands length needs to be 2.");
        checkArgument(args[0] instanceof Boolean, "Left operand needs to be Boolean.");
        checkArgument(args[1] instanceof Boolean, "Right Operand needs to be Boolean.");
        return ((Boolean) args[0]) && ((Boolean) args[1]);
    }
}
