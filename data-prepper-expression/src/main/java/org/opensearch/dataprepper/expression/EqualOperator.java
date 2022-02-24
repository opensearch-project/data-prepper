/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

import static com.google.common.base.Preconditions.checkArgument;

public class EqualOperator implements Operator<Boolean> {

    @Override
    public Integer getSymbol() {
        return DataPrepperExpressionParser.EQUAL;
    }

    @Override
    public Boolean eval(Object... args) {
        checkArgument(args.length == 2, "Operands length needs to be 2.");
        if ((args[0] == null) || (args[1] == null)) {
            return args[0] == null && args[1] == null;
        }
        return args[0].equals(args[1]);
    }
}
