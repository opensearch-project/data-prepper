/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import static com.google.common.base.Preconditions.checkArgument;

public class LessThanOperator implements Operator<Boolean> {
    @Override
    public String getSymbol() {
        return "<";
    }

    @Override
    public Boolean eval(Object... args) {
        checkArgument(args.length == 2, "Operands length needs to be 2.");
        if ((args[0] instanceof Integer) && (args[1] instanceof Float)) {
            return ((Integer) args[0]) < ((Float) args[1]);
        } else if ((args[0] instanceof Float) && (args[1] instanceof Integer)) {
            return ((Float) args[0]) < ((Integer) args[1]);
        } else if ((args[0] instanceof Integer) && (args[1] instanceof Integer)) {
            return ((Integer) args[0]) < ((Integer) args[1]);
        } else if ((args[0] instanceof Float) && (args[1] instanceof Float)) {
            return ((Float) args[0]) < ((Float) args[1]);
        }
        throw new IllegalArgumentException(args[0] + " or " + args[1] + " should be either Float or Integer");
    }
}
