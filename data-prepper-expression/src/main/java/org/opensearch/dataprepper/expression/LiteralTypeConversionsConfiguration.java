/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.springframework.context.annotation.Bean;

import javax.inject.Named;
import java.io.Serializable;
import java.util.Map;
import java.util.function.Function;

@Named
class LiteralTypeConversionsConfiguration {
    @Bean
    public Map<Class<? extends Serializable>, Function<Object, Object>> literalTypeConversions() {
        return Map.of(
            String.class, Function.identity(),
            Boolean.class, Function.identity(),
            Integer.class, Function.identity(),
            Float.class, Function.identity(),
            Long.class, Function.identity(),
            Double.class, o -> ((Double) o).floatValue()
        );
    }
}
