/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

import static com.google.common.base.Preconditions.checkArgument;

public class OrOperator implements Operator<Boolean> {

    @Override
    public Integer getSymbol() {
        return DataPrepperExpressionParser.OR;
    }

    @Override
    public Boolean eval(Object... args) {
        checkArgument(args.length == 2, "Operands length needs to be 2.");
        checkArgument(args[0] instanceof Boolean, "Left operand needs to be Boolean.");
        checkArgument(args[1] instanceof Boolean, "Right Operand needs to be Boolean.");
        return ((Boolean) args[0]) || ((Boolean) args[1]);
    }
}
