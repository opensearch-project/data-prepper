/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;
import org.springframework.context.annotation.Bean;

import javax.inject.Named;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

@Named
class UnaryNumericOperatorConfiguration {
    @Bean
    public UnaryNumericOperator subtractUnaryNumericOperator() {
        final Map<Class<? extends Number>, Function<Number, ? extends Number>> strategy = new HashMap<>();
        strategy.put(Integer.class, arg -> -arg.intValue());
        strategy.put(Float.class, arg -> -arg.floatValue());

        return new UnaryNumericOperator(
                DataPrepperExpressionParser.SUBTRACT,
                strategy
        );
    }
}
