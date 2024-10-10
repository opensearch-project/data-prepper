/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
*/

package org.opensearch.dataprepper.expression;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

public class OperatorParameters {
    private final int symbol;
    private final Map<Class<? extends Number>, Map<Class<? extends Number>, BiFunction<Object, Object, Number>>> operandsToOperationMap;
    private final Map<Class<? extends Number>, Function<Number, ? extends Number>> strategy;

    public OperatorParameters(
            final int symbol,
            final Map<Class<? extends Number>, Map<Class<? extends Number>, BiFunction<Object, Object, Number>>> operandsToOperationMap,
            final Map<Class<? extends Number>, Function<Number, ? extends Number>> strategy
    ) {
        this.symbol = symbol;
        this.operandsToOperationMap = operandsToOperationMap;
        this.strategy = strategy;
    }

    public int getSymbol() {
        return symbol;
    }

    public Map<Class<? extends Number>, Map<Class<? extends Number>, BiFunction<Object, Object, Number>>> getOperandsToOperationMap() {
        return operandsToOperationMap;
    }

    public Map<Class<? extends Number>, Function<Number, ? extends Number>> getStrategy() {
        return strategy;
    }
}
