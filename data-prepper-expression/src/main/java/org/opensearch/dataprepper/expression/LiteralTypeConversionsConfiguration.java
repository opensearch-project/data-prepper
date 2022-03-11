/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.springframework.context.annotation.Bean;

import javax.inject.Named;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

@Named
class LiteralTypeConversionsConfiguration {
    @Bean
    public Map<Class<? extends Serializable>, Function<Object, Object>> literalTypeConversions() {
        return new HashMap<Class<? extends Serializable>, Function<Object, Object>>() {{
            put(String.class, Function.identity());
            put(Boolean.class, Function.identity());
            put(Integer.class, Function.identity());
            put(Float.class, Function.identity());
            put(Double.class, o -> ((Double) o).floatValue());
        }};
    }
}
