/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

import static com.google.common.base.Preconditions.checkArgument;

public class NotEqualOperator implements Operator<Boolean> {

    @Override
    public Integer getSymbol() {
        return DataPrepperExpressionParser.NOT_EQUAL;
    }

    @Override
    public Boolean eval(Object... args) {
        checkArgument(args.length == 2, "Operands length needs to be 2.");
        return !args[0].equals(args[1]);
    }
}
