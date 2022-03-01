/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

public class NegateOperator implements Operator<Boolean> {
    private final Integer symbol;
    private final Operator<Boolean> operationToNegate;

    public NegateOperator(final Integer symbol, Operator<Boolean> operationToNegate) {
        this.symbol = symbol;
        this.operationToNegate = operationToNegate;
    }

    @Override
    public Integer getSymbol() {
        return symbol;
    }

    @Override
    public Boolean evaluate(final Object... args) {
        return !operationToNegate.evaluate(args);
    }
}
