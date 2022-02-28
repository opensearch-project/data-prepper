/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

class InOperator implements Operator<Boolean> {
    @Override
    public Integer getSymbol() {
        return DataPrepperExpressionParser.IN_SET;
    }

    @Override
    public Boolean evaluate(Object... args) {
        checkArgument(args.length == 2, "Operands length needs to be 2.");
        if (!(args[1] instanceof Set)) {
            throw new IllegalArgumentException(args[1] + " should be Set");
        }
        return ((Set<?>) args[1]).contains(args[0]);
    }
}
