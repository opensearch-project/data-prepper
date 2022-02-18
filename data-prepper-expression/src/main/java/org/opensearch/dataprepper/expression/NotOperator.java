/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import static com.google.common.base.Preconditions.checkArgument;

public class NotOperator implements Operator<Boolean> {
    @Override
    public String getSymbol() {
        return "not";
    }

    @Override
    public Boolean eval(Object... args) {
        checkArgument(args.length == 1, "Operands length needs to be 1.");
        checkArgument(args[0] instanceof Boolean, "Operand needs to be Boolean.");
        return !((Boolean) args[0]);
    }
}
