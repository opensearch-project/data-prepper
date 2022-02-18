/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

public class NotInOperator implements Operator<Boolean> {

    @Override
    public String getSymbol() {
        return "not in";
    }

    @Override
    public Boolean eval(Object... args) {
        checkArgument(args.length == 2, "Operands length needs to be 2.");
        checkArgument(args[1] instanceof Set, "Right Operand needs to be Set.");
        return !((Set<?>) args[1]).contains(args[0]);
    }
}
