/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

import javax.inject.Named;

import static com.google.common.base.Preconditions.checkArgument;

@Named
class NotEqualOperator implements Operator<Boolean> {

    @Override
    public Integer getSymbol() {
        return DataPrepperExpressionParser.NOT_EQUAL;
    }

    @Override
    public Boolean evaluate(final Object... args) {
        checkArgument(args.length == 2, "Operands length needs to be 2.");
        if ((args[0] == null) || (args[1] == null)) {
            return args[0] != null || args[1] != null;
        }
        return !args[0].equals(args[1]);
    }
}
